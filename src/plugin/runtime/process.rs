mod output;
mod termination;

use std::{
    io,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use time::OffsetDateTime;
use tokio::{
    net::TcpListener,
    process::Command,
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::{
    plugin::{
        InstalledPlugin, PluginError, PluginInstanceId,
        package::{EffectiveManifest, ExpansionPaths},
    },
    protocol::oll::plugin_runtime_server::PluginRuntimeServer,
};

use super::{
    InstanceCommand, InstanceNotice, InstanceShutdown, MAX_PLUGIN_GRPC_MESSAGE_BYTES,
    RuntimeDependencies,
    session::{InstanceService, SessionOutcome, run_session},
};
use output::{finish_output_tasks, pipe_plugin_output, stop_server};
use termination::{OwnedPluginProcess, terminate_process, terminate_reaped_process_group};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_FAILURE_GRACE: Duration = Duration::from_secs(2);

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_plugin_instance(
    dependencies: RuntimeDependencies,
    plugin: InstalledPlugin,
    instance_id: PluginInstanceId,
    commands: mpsc::Receiver<InstanceCommand>,
    shutdown: watch::Receiver<Option<InstanceShutdown>>,
    notices: mpsc::UnboundedSender<InstanceNotice>,
    lifecycle_correlation_id: String,
    package_gate: tokio::sync::OwnedMutexGuard<()>,
) {
    let plugin_id = plugin.plugin_id.clone();
    let outcome = run_plugin_instance_inner(
        dependencies,
        plugin,
        instance_id,
        commands,
        shutdown,
        &notices,
        &lifecycle_correlation_id,
        package_gate,
    )
    .await;
    let (failure, correlation_id) = match outcome {
        Ok(outcome) => (outcome.failure, outcome.correlation_id),
        Err(error) => (Some(error.code().to_owned()), lifecycle_correlation_id),
    };
    let _ = notices.send(InstanceNotice::Ended {
        plugin_id,
        instance_id,
        failure,
        correlation_id,
        ended_at: OffsetDateTime::now_utc(),
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_plugin_instance_inner(
    dependencies: RuntimeDependencies,
    plugin: InstalledPlugin,
    instance_id: PluginInstanceId,
    mut commands: mpsc::Receiver<InstanceCommand>,
    mut shutdown: watch::Receiver<Option<InstanceShutdown>>,
    notices: &mpsc::UnboundedSender<InstanceNotice>,
    lifecycle_correlation_id: &str,
    package_gate: tokio::sync::OwnedMutexGuard<()>,
) -> Result<SessionOutcome, PluginError> {
    let install = dependencies
        .packages
        .generation(&plugin.plugin_id, plugin.current_generation);
    let effective: EffectiveManifest =
        serde_json::from_slice(&plugin.effective_manifest).map_err(|_| {
            PluginError::CorruptStore("effective plugin manifest cannot be decoded".to_owned())
        })?;
    if effective
        .plugin_id()
        .map_err(|error| PluginError::CorruptStore(error.to_string()))?
        != plugin.plugin_id
        || effective
            .plugin_name()
            .map_err(|error| PluginError::CorruptStore(error.to_string()))?
            != plugin.plugin_name
    {
        return Err(PluginError::CorruptStore(
            "effective plugin manifest identity differs from SQL state".to_owned(),
        ));
    }
    let mask_dir = dependencies.config_root.join("plugin-masks");
    let argv = effective
        .expanded_runtime_argv(&ExpansionPaths {
            source: None,
            staging: None,
            install: &install,
            mask_dir: &mask_dir,
        })
        .map_err(|error| PluginError::FailedPrecondition(error.to_string()))?;
    let executable = argv
        .first()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PluginError::CorruptStore("effective plugin runtime argv is empty".to_owned())
        })?;

    let startup_deadline = Instant::now() + STARTUP_TIMEOUT;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| PluginError::io("bind plugin loopback listener", error))?;
    let endpoint = format!(
        "http://{}",
        listener
            .local_addr()
            .map_err(|error| PluginError::io("inspect plugin loopback listener", error))?
    );
    let (service, connection) = InstanceService::new();
    let (server_shutdown, mut server_shutdown_rx) = watch::channel(false);
    let reader = dependencies
        .parent_liveness
        .reader_for_child()
        .map_err(|error| PluginError::FailedPrecondition(error.to_string()))?;
    let mut command = Command::new(executable);
    command
        .args(&argv[1..])
        .current_dir(&install)
        .env("OLL_PLUGIN_ENDPOINT", endpoint)
        .stdin(Stdio::from(reader))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|error| PluginError::io("spawn plugin process", error))?;
    drop(package_gate);
    let process_id = child.id().ok_or_else(|| {
        PluginError::FailedPrecondition("spawned plugin process has no process ID".to_owned())
    })?;
    let process_group = i32::try_from(process_id).map_err(|_| {
        PluginError::FailedPrecondition("plugin process ID is out of range".to_owned())
    })?;
    let started_at = OffsetDateTime::now_utc();
    let _ = notices.send(InstanceNotice::Spawned {
        plugin_id: plugin.plugin_id.clone(),
        instance_id,
        process_id,
        started_at,
    });

    // The listener was bound before spawn, so an eager plugin connection is
    // queued by the kernel while the server task is installed here. Starting
    // the server only after a successful spawn avoids leaking a detached
    // listener when process creation fails.
    let incoming = TcpListenerStream::new(listener);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(plugin_runtime_service(service))
            .serve_with_incoming_shutdown(incoming, async move {
                if !*server_shutdown_rx.borrow() {
                    let _ = server_shutdown_rx.changed().await;
                }
            })
            .await
    });

    let stdout = child.stdout.take().map(|stream| {
        tokio::spawn(pipe_plugin_output(
            stream,
            dependencies.logger.clone(),
            plugin.plugin_id.clone(),
            plugin.plugin_name.clone(),
            instance_id,
            "stdout",
            lifecycle_correlation_id.to_owned(),
        ))
    });
    let stderr = child.stderr.take().map(|stream| {
        tokio::spawn(pipe_plugin_output(
            stream,
            dependencies.logger.clone(),
            plugin.plugin_id.clone(),
            plugin.plugin_name.clone(),
            instance_id,
            "stderr",
            lifecycle_correlation_id.to_owned(),
        ))
    });
    let mut process = OwnedPluginProcess {
        child,
        process_group,
        reaped: false,
    };
    let mut connection = Box::pin(connection);
    let connected = loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_ok() {
                    let request = shutdown
                        .borrow()
                        .clone()
                        .expect("changed shutdown lane contains a request");
                    let termination = terminate_process(
                        &mut process,
                        Instant::now(),
                        request.deadline,
                        &dependencies.logger,
                        &plugin.plugin_id,
                        instance_id,
                        &request.correlation_id,
                    ).await;
                    stop_server(server_shutdown, server, request.deadline).await;
                    finish_output_tasks(stdout, stderr, request.deadline).await;
                    termination?;
                    return Ok(SessionOutcome::stopped(
                        request.correlation_id,
                        Instant::now(),
                        request.deadline,
                        None,
                    ));
                }
            }
            connected = &mut connection => {
                break connected.map_err(|_| PluginError::FailedPrecondition(
                    "plugin runtime listener stopped before Connect".to_owned()
                ))?;
            }
            status = process.child.wait() => {
                let status = status.map_err(|error| PluginError::io("wait for plugin process", error))?;
                process.reaped = true;
                let outcome = SessionOutcome::failed(
                    format!("plugin_exited_before_connect:{}", exit_label(status)),
                    lifecycle_correlation_id.to_owned(),
                );
                terminate_reaped_process_group(
                    process.process_group,
                    outcome.absolute_deadline,
                    &dependencies.logger,
                    &plugin.plugin_id,
                    instance_id,
                    &outcome.correlation_id,
                ).await?;
                stop_server(server_shutdown, server, outcome.absolute_deadline).await;
                finish_output_tasks(stdout, stderr, outcome.absolute_deadline).await;
                return Ok(outcome);
            }
            command = commands.recv() => {
                match command {
                    Some(InstanceCommand::StartJob { response, .. }) => {
                        let _ = response.send(Err(PluginError::FailedPrecondition(
                            "plugin runtime session is not ready".to_owned()
                        )));
                    }
                    Some(InstanceCommand::CancelJob { dispatched, .. }) => {
                        let _ = dispatched.send(Err(PluginError::FailedPrecondition(
                            "plugin runtime session is not ready".to_owned()
                        )));
                    }
                    None => {
                        let outcome = SessionOutcome::failed(
                            "plugin_supervisor_stopped".to_owned(),
                            lifecycle_correlation_id.to_owned(),
                        );
                        let termination = terminate_process(
                            &mut process,
                            outcome.graceful_deadline,
                            outcome.absolute_deadline,
                            &dependencies.logger,
                            &plugin.plugin_id,
                            instance_id,
                            &outcome.correlation_id,
                        ).await;
                        stop_server(server_shutdown, server, outcome.absolute_deadline).await;
                        finish_output_tasks(stdout, stderr, outcome.absolute_deadline).await;
                        termination?;
                        return Ok(outcome);
                    }
                }
            }
            () = tokio::time::sleep_until(startup_deadline) => {
                let outcome = SessionOutcome::failed(
                    "plugin_connect_timeout".to_owned(),
                    lifecycle_correlation_id.to_owned(),
                );
                let termination = terminate_process(
                    &mut process,
                    outcome.graceful_deadline,
                    outcome.absolute_deadline,
                    &dependencies.logger,
                    &plugin.plugin_id,
                    instance_id,
                    &outcome.correlation_id,
                ).await;
                stop_server(server_shutdown, server, outcome.absolute_deadline).await;
                finish_output_tasks(stdout, stderr, outcome.absolute_deadline).await;
                termination?;
                return Ok(outcome);
            }
        }
    };

    let (process_ended, process_ended_rx) = watch::channel(false);
    let (shutdown_deadline, shutdown_deadline_rx) = watch::channel(None);
    let mut session = tokio::spawn(run_session(
        dependencies.clone(),
        plugin.clone(),
        instance_id,
        connected,
        commands,
        shutdown,
        notices.clone(),
        lifecycle_correlation_id.to_owned(),
        startup_deadline,
        process_ended_rx,
        shutdown_deadline,
    ));
    let outcome = tokio::select! {
        outcome = &mut session => {
            outcome.map_err(|_| PluginError::FailedPrecondition(
                "plugin session task failed".to_owned()
            ))?
        }
        status = process.child.wait() => {
            let status = status.map_err(|error| PluginError::io("wait for plugin process", error))?;
            process.reaped = true;
            process_ended.send_replace(true);
            let fallback = SessionOutcome::failed(
                format!("plugin_process_exited:{}", exit_label(status)),
                lifecycle_correlation_id.to_owned(),
            );
            let session_deadline = shutdown_deadline_rx
                .borrow()
                .unwrap_or_else(|| Instant::now() + SESSION_FAILURE_GRACE)
                .min(Instant::now() + SESSION_FAILURE_GRACE);
            let session_wait = tokio::time::timeout_at(session_deadline, &mut session);
            let group_cleanup = terminate_reaped_process_group(
                process.process_group,
                fallback.absolute_deadline,
                &dependencies.logger,
                &plugin.plugin_id,
                instance_id,
                &fallback.correlation_id,
            );
            let (session_result, group_result) = tokio::join!(session_wait, group_cleanup);
            if let Err(error) = group_result {
                if session_result.is_err() {
                    session.abort();
                    let _ = (&mut session).await;
                }
                return Err(error);
            }
            match session_result {
                Ok(Ok(mut outcome)) => {
                    if outcome.failure.is_none() {
                        outcome.failure = Some(format!("plugin_process_exited:{}", exit_label(status)));
                    }
                    outcome
                }
                Ok(Err(_)) => fallback,
                Err(_) => {
                    session.abort();
                    let _ = (&mut session).await;
                    fallback
                }
            }
        }
    };
    let mut termination = Ok(());
    if !process.reaped {
        termination = terminate_process(
            &mut process,
            outcome.graceful_deadline,
            outcome.absolute_deadline,
            &dependencies.logger,
            &plugin.plugin_id,
            instance_id,
            &outcome.correlation_id,
        )
        .await;
    }
    stop_server(server_shutdown, server, outcome.absolute_deadline).await;
    finish_output_tasks(stdout, stderr, outcome.absolute_deadline).await;
    termination?;
    Ok(outcome)
}

fn plugin_runtime_service(service: InstanceService) -> PluginRuntimeServer<InstanceService> {
    plugin_runtime_service_with_limit(service, MAX_PLUGIN_GRPC_MESSAGE_BYTES)
}

fn plugin_runtime_service_with_limit(
    service: InstanceService,
    limit: usize,
) -> PluginRuntimeServer<InstanceService> {
    PluginRuntimeServer::new(service)
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit)
}

fn exit_label(status: ExitStatus) -> String {
    status
        .code()
        .map_or_else(|| "signal".to_owned(), |code| code.to_string())
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
