use std::{
    collections::{BTreeMap, HashMap},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use futures_util::FutureExt as _;
use time::OffsetDateTime;
use tokio::{
    sync::{RwLock, mpsc},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

use crate::{
    node::logging::LogLevel,
    plugin::{DesiredPluginState, ObservedPluginState, PluginError, PluginId, PluginInstanceId},
};

use super::super::{
    INSTANCE_COMMAND_CAPACITY, InstanceNotice, InstanceSender, InstanceShutdown,
    RuntimeDependencies, process::run_plugin_instance,
};
use super::{ActiveInstance, ControllerCommand};

const PROCESS_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const MAXIMUM_RESTART_BACKOFF: Duration = Duration::from_secs(60);
const MAXIMUM_RECONCILE_RETRY_BACKOFF: Duration = Duration::from_secs(5);

pub(super) struct ReconcileContext<'a> {
    pub(super) dependencies: &'a RuntimeDependencies,
    pub(super) active: &'a Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    pub(super) controller: &'a mpsc::Sender<ControllerCommand>,
    pub(super) package_gate_waiters: &'a mut HashMap<PluginId, JoinHandle<()>>,
    pub(super) reconcile_timers: &'a mut JoinSet<ControllerCommand>,
}

pub(super) struct SpawnContext<'a> {
    pub(super) dependencies: &'a RuntimeDependencies,
    pub(super) active: &'a Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    pub(super) instance_tasks: &'a mut JoinSet<()>,
    pub(super) notices: &'a mpsc::UnboundedSender<InstanceNotice>,
    pub(super) reconcile_timers: &'a mut JoinSet<ControllerCommand>,
}

pub(super) async fn reconcile_one(
    context: ReconcileContext<'_>,
    plugin_id: PluginId,
    correlation_id: String,
    retry_attempt: u32,
) {
    let ReconcileContext {
        dependencies,
        active,
        controller,
        package_gate_waiters,
        reconcile_timers,
    } = context;
    let plugin = match dependencies
        .store
        .get_plugin(&crate::plugin::PluginSelector::Id(plugin_id.clone()))
        .await
    {
        Ok(plugin) => plugin,
        Err(PluginError::NotFound(_)) => return,
        Err(error) => {
            lifecycle_error(
                dependencies,
                "plugin_reconcile_failed",
                &correlation_id,
                Some(&plugin_id),
                &error,
            );
            if retryable_reconcile_error(&error) {
                schedule_reconcile_retry(
                    reconcile_timers,
                    plugin_id,
                    correlation_id,
                    retry_attempt,
                );
            }
            return;
        }
    };
    let running_instance = active.read().await.get(&plugin_id).cloned();
    if let Some(instance) = running_instance {
        if instance.state == ObservedPluginState::Ready
            && (plugin.restart_attempt != 0
                || plugin.restart_not_before.is_some()
                || plugin.last_lifecycle_failure.is_some())
            && let Err(error) = dependencies
                .store
                .record_instance_ready(&plugin_id, instance.instance_id)
                .await
        {
            lifecycle_error(
                dependencies,
                "plugin_ready_persistence_failed",
                &correlation_id,
                Some(&plugin_id),
                &error,
            );
            if retryable_reconcile_error(&error) {
                schedule_reconcile_retry(
                    reconcile_timers,
                    plugin_id,
                    correlation_id,
                    retry_attempt,
                );
            }
            return;
        }
        if plugin.desired_state == DesiredPluginState::Stopped
            || plugin.restart_sequence != plugin.consumed_restart_sequence
        {
            if let Some(instance) = active.write().await.get_mut(&plugin_id) {
                instance.state = ObservedPluginState::Stopping;
            }
            let deadline = Instant::now() + PROCESS_SHUTDOWN_GRACE;
            instance.sender.shutdown(InstanceShutdown {
                reason: if plugin.desired_state == DesiredPluginState::Stopped {
                    "desired state stopped".to_owned()
                } else {
                    "plugin restart requested".to_owned()
                },
                correlation_id,
                deadline,
            });
        }
        return;
    }
    if plugin.desired_state == DesiredPluginState::Stopped {
        return;
    }
    if plugin.last_lifecycle_failure.is_some() && plugin.restart_not_before.is_none() {
        let attempt = plugin.restart_attempt.saturating_add(1);
        let delay = restart_backoff(attempt);
        let not_before = OffsetDateTime::now_utc() + delay;
        if let Err(error) = dependencies
            .store
            .record_restart_backoff(&plugin_id, attempt, Some(not_before))
            .await
        {
            lifecycle_error(
                dependencies,
                "plugin_restart_schedule_failed",
                &correlation_id,
                Some(&plugin_id),
                &error,
            );
            if retryable_reconcile_error(&error) {
                schedule_reconcile_retry(
                    reconcile_timers,
                    plugin_id,
                    correlation_id,
                    retry_attempt,
                );
            }
            return;
        }
        dependencies.logger.emit(
            LogLevel::Warn,
            "oll::plugin",
            "plugin_restart_scheduled",
            &correlation_id,
            serde_json::json!({
                "plugin_id": plugin_id.as_str(),
                "restart_attempt": attempt,
                "backoff_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            }),
        );
        schedule_reconcile(reconcile_timers, plugin_id, correlation_id, delay, 0);
        return;
    }
    if let Some(not_before) = plugin.restart_not_before
        && not_before > OffsetDateTime::now_utc()
    {
        schedule_reconcile(
            reconcile_timers,
            plugin_id,
            correlation_id,
            (not_before - OffsetDateTime::now_utc()).unsigned_abs(),
            0,
        );
        return;
    }

    if package_gate_waiters.contains_key(&plugin_id) {
        return;
    }
    let package_gates = Arc::clone(&dependencies.package_gates);
    let gate_plugin_id = plugin_id.clone();
    let gate_controller = controller.clone();
    let waiter = tokio::spawn(async move {
        let gate = package_gates.lock(&gate_plugin_id).await;
        let _ = gate_controller
            .send(ControllerCommand::PackageGateReady {
                plugin_id: gate_plugin_id,
                correlation_id,
                retry_attempt,
                gate,
            })
            .await;
    });
    package_gate_waiters.insert(plugin_id, waiter);
}

pub(super) async fn reconcile_with_package_gate(
    context: SpawnContext<'_>,
    plugin_id: PluginId,
    correlation_id: String,
    retry_attempt: u32,
    gate: tokio::sync::OwnedMutexGuard<()>,
) {
    let SpawnContext {
        dependencies,
        active,
        instance_tasks,
        notices,
        reconcile_timers,
    } = context;
    let plugin = match dependencies
        .store
        .get_plugin(&crate::plugin::PluginSelector::Id(plugin_id.clone()))
        .await
    {
        Ok(plugin) if plugin.desired_state == DesiredPluginState::Running => plugin,
        Ok(_) | Err(PluginError::NotFound(_)) => return,
        Err(error) => {
            lifecycle_error(
                dependencies,
                "plugin_spawn_failed",
                &correlation_id,
                Some(&plugin_id),
                &error,
            );
            if retryable_reconcile_error(&error) {
                schedule_reconcile_retry(
                    reconcile_timers,
                    plugin_id,
                    correlation_id,
                    retry_attempt,
                );
            }
            return;
        }
    };
    if active.read().await.contains_key(&plugin_id) {
        return;
    }
    match dependencies.packages.current_generation(&plugin_id) {
        Ok(Some(generation)) if generation == plugin.current_generation => {}
        Ok(_) => {
            lifecycle_error(
                dependencies,
                "plugin_spawn_failed",
                &correlation_id,
                Some(&plugin_id),
                &PluginError::FailedPrecondition(
                    "plugin SQL and current package generation differ".to_owned(),
                ),
            );
            schedule_reconcile_retry(reconcile_timers, plugin_id, correlation_id, retry_attempt);
            return;
        }
        Err(error) => {
            lifecycle_error(
                dependencies,
                "plugin_spawn_failed",
                &correlation_id,
                Some(&plugin_id),
                &PluginError::FailedPrecondition(error.to_string()),
            );
            schedule_reconcile_retry(reconcile_timers, plugin_id, correlation_id, retry_attempt);
            return;
        }
    }
    let instance_id = PluginInstanceId::new();
    if let Err(error) = dependencies
        .store
        .record_running_instance(&plugin_id, plugin.current_generation, instance_id)
        .await
    {
        lifecycle_error(
            dependencies,
            "plugin_spawn_failed",
            &correlation_id,
            Some(&plugin_id),
            &error,
        );
        if retryable_reconcile_error(&error) {
            schedule_reconcile_retry(reconcile_timers, plugin_id, correlation_id, retry_attempt);
        }
        return;
    }
    let (command_sender, command_receiver) = mpsc::channel(INSTANCE_COMMAND_CAPACITY);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(None);
    active.write().await.insert(
        plugin_id.clone(),
        ActiveInstance {
            instance_id,
            generation: plugin.current_generation,
            sender: InstanceSender {
                work: command_sender,
                shutdown: shutdown_sender,
            },
            state: ObservedPluginState::Starting,
            process_id: None,
            started_at: None,
            ready_at: None,
            actions: Vec::new(),
        },
    );
    dependencies.logger.emit(
        LogLevel::Info,
        "oll::plugin",
        "plugin_process_starting",
        &correlation_id,
        serde_json::json!({
            "plugin_id": plugin_id.as_str(),
            "plugin_name": plugin.plugin_name.as_str(),
            "plugin_instance_id": instance_id.to_string(),
            "running_generation": plugin.current_generation.to_string(),
        }),
    );
    let process_notices = notices.clone();
    let panic_notices = notices.clone();
    let process_dependencies = dependencies.clone();
    let panic_plugin_id = plugin_id;
    let panic_correlation_id = correlation_id.clone();
    instance_tasks.spawn(async move {
        let panicked = AssertUnwindSafe(run_plugin_instance(
            process_dependencies,
            plugin,
            instance_id,
            command_receiver,
            shutdown_receiver,
            process_notices,
            correlation_id,
            gate,
        ))
        .catch_unwind()
        .await
        .is_err();
        if panicked {
            let _ = panic_notices.send(InstanceNotice::Ended {
                plugin_id: panic_plugin_id,
                instance_id,
                failure: Some("plugin_runtime_task_panicked".to_owned()),
                correlation_id: panic_correlation_id,
                ended_at: OffsetDateTime::now_utc(),
            });
        }
    });
}

pub(super) fn schedule_reconcile(
    timers: &mut JoinSet<ControllerCommand>,
    plugin_id: PluginId,
    correlation_id: String,
    delay: Duration,
    retry_attempt: u32,
) {
    timers.spawn(async move {
        tokio::time::sleep(delay).await;
        ControllerCommand::Reconcile {
            plugin_id,
            correlation_id,
            explicit: false,
            retry_attempt,
        }
    });
}

pub(super) fn schedule_reconcile_retry(
    timers: &mut JoinSet<ControllerCommand>,
    plugin_id: PluginId,
    correlation_id: String,
    retry_attempt: u32,
) {
    let next_attempt = retry_attempt.saturating_add(1);
    schedule_reconcile(
        timers,
        plugin_id,
        correlation_id,
        reconcile_retry_backoff(next_attempt),
        next_attempt,
    );
}

pub(super) fn reconcile_retry_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6);
    Duration::from_millis(100_u64 << exponent).min(MAXIMUM_RECONCILE_RETRY_BACKOFF)
}

pub(super) fn retryable_reconcile_error(error: &PluginError) -> bool {
    matches!(
        error,
        PluginError::Store(_) | PluginError::FailedPrecondition(_)
    )
}

fn restart_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6);
    Duration::from_secs(1_u64 << exponent).min(MAXIMUM_RESTART_BACKOFF)
}

pub(super) fn lifecycle_error(
    dependencies: &RuntimeDependencies,
    event: &str,
    correlation_id: &str,
    plugin_id: Option<&PluginId>,
    error: &PluginError,
) {
    dependencies.logger.emit(
        LogLevel::Error,
        "oll::plugin",
        event,
        correlation_id,
        serde_json::json!({
            "plugin_id": plugin_id.map(PluginId::as_str),
            "error_code": error.code(),
        }),
    );
}
