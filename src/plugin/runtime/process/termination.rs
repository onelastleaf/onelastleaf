use std::{io, time::Duration};

use tokio::{process::Child, time::Instant};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{PluginError, PluginId, PluginInstanceId},
};

const SIGNAL_GRACE: Duration = Duration::from_secs(2);
const KILL_REAP_RESERVE: Duration = Duration::from_millis(250);
const CONTROLLER_NOTICE_RESERVE: Duration = Duration::from_millis(100);

pub(super) struct OwnedPluginProcess {
    pub(super) child: Child,
    pub(super) process_group: i32,
    pub(super) reaped: bool,
}

impl Drop for OwnedPluginProcess {
    fn drop(&mut self) {
        let _ = signal_group(self.process_group, libc::SIGKILL);
        if !self.reaped {
            let _ = self.child.start_kill();
        }
    }
}

pub(super) async fn terminate_process(
    process: &mut OwnedPluginProcess,
    graceful_deadline: Instant,
    absolute_deadline: Instant,
    logger: &NodeLogger,
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    correlation_id: &str,
) -> Result<(), PluginError> {
    let notice_deadline = absolute_deadline
        .checked_sub(CONTROLLER_NOTICE_RESERVE)
        .unwrap_or_else(Instant::now);
    let kill_deadline = notice_deadline
        .checked_sub(KILL_REAP_RESERVE)
        .unwrap_or_else(Instant::now);
    match tokio::time::timeout_at(graceful_deadline.min(kill_deadline), process.child.wait()).await
    {
        Ok(Ok(_)) => {
            process.reaped = true;
            return terminate_reaped_process_group(
                process.process_group,
                absolute_deadline,
                logger,
                plugin_id,
                instance_id,
                correlation_id,
            )
            .await;
        }
        Ok(Err(error)) => return Err(PluginError::io("wait for plugin process", error)),
        Err(_) => {}
    }
    log_process_signal(logger, plugin_id, instance_id, correlation_id, "SIGTERM");
    signal_group(process.process_group, libc::SIGTERM)?;
    let signal_deadline = (Instant::now() + SIGNAL_GRACE).min(kill_deadline);
    match tokio::time::timeout_at(signal_deadline, process.child.wait()).await {
        Ok(Ok(_)) => {
            process.reaped = true;
            return terminate_reaped_process_group(
                process.process_group,
                absolute_deadline,
                logger,
                plugin_id,
                instance_id,
                correlation_id,
            )
            .await;
        }
        Ok(Err(error)) => return Err(PluginError::io("wait for plugin process", error)),
        Err(_) => {}
    }
    log_process_signal(logger, plugin_id, instance_id, correlation_id, "SIGKILL");
    signal_group(process.process_group, libc::SIGKILL)?;
    match tokio::time::timeout_at(notice_deadline, process.child.wait()).await {
        Ok(Ok(_)) => {
            process.reaped = true;
            wait_for_process_group_exit(process.process_group, notice_deadline).await
        }
        Ok(Err(error)) => Err(PluginError::io("reap plugin process", error)),
        Err(_) => {
            let _ = process.child.start_kill();
            Err(PluginError::Aborted(
                "plugin process could not be reaped before the shutdown deadline".to_owned(),
            ))
        }
    }
}

pub(super) async fn terminate_reaped_process_group(
    process_group: i32,
    absolute_deadline: Instant,
    logger: &NodeLogger,
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    correlation_id: &str,
) -> Result<(), PluginError> {
    if !process_group_exists(process_group)? {
        return Ok(());
    }
    let notice_deadline = absolute_deadline
        .checked_sub(CONTROLLER_NOTICE_RESERVE)
        .unwrap_or_else(Instant::now);
    let kill_deadline = notice_deadline
        .checked_sub(KILL_REAP_RESERVE)
        .unwrap_or_else(Instant::now);
    log_process_signal(logger, plugin_id, instance_id, correlation_id, "SIGTERM");
    signal_group(process_group, libc::SIGTERM)?;
    let term_deadline = (Instant::now() + SIGNAL_GRACE).min(kill_deadline);
    if wait_for_process_group_exit(process_group, term_deadline)
        .await
        .is_ok()
    {
        return Ok(());
    }
    log_process_signal(logger, plugin_id, instance_id, correlation_id, "SIGKILL");
    signal_group(process_group, libc::SIGKILL)?;
    wait_for_process_group_exit(process_group, notice_deadline).await
}

async fn wait_for_process_group_exit(
    process_group: i32,
    deadline: Instant,
) -> Result<(), PluginError> {
    loop {
        if !process_group_exists(process_group)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(PluginError::Aborted(
                "plugin process group remained alive after signal escalation".to_owned(),
            ));
        }
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(10)).min(deadline)).await;
    }
}

pub(super) fn process_group_exists(process_group: i32) -> Result<bool, PluginError> {
    if unsafe { libc::kill(-process_group, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(PluginError::io("inspect plugin process group", error)),
    }
}

fn log_process_signal(
    logger: &NodeLogger,
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    correlation_id: &str,
    signal: &str,
) {
    logger.emit(
        LogLevel::Warn,
        "oll::plugin",
        "plugin_process_signal_sent",
        correlation_id,
        serde_json::json!({
            "plugin_id": plugin_id.as_str(),
            "plugin_instance_id": instance_id.to_string(),
            "signal": signal,
        }),
    );
}

fn signal_group(process_group: i32, signal: i32) -> Result<(), PluginError> {
    if unsafe { libc::kill(-process_group, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(PluginError::io("signal plugin process group", error))
    }
}
