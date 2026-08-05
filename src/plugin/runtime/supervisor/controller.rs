use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use tokio::{
    sync::{RwLock, mpsc},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

use crate::{
    node::logging::LogLevel,
    plugin::{ObservedPluginState, PluginId},
};

use super::super::{InstanceNotice, InstanceShutdown, RuntimeDependencies};
use super::{
    ActiveInstance, ControllerCommand,
    lifecycle::handle_notice,
    reconciliation::{
        ReconcileContext, SpawnContext, lifecycle_error, reconcile_one, reconcile_retry_backoff,
        reconcile_with_package_gate,
    },
    settlement::settle_ended_instance,
};

pub(super) async fn run_controller(
    dependencies: RuntimeDependencies,
    active: Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    sender: mpsc::Sender<ControllerCommand>,
    mut receiver: mpsc::Receiver<ControllerCommand>,
    notices: mpsc::UnboundedSender<InstanceNotice>,
    mut notice_receiver: mpsc::UnboundedReceiver<InstanceNotice>,
) {
    let mut shutting_down = false;
    let mut shutdown_deadline = None;
    let mut shutdown_response = None;
    let mut removal_blocks = HashSet::new();
    let mut removal_waiters = HashMap::new();
    let mut package_gate_waiters = HashMap::<PluginId, JoinHandle<()>>::new();
    let mut reconcile_timers = JoinSet::<ControllerCommand>::new();
    let mut instance_tasks = JoinSet::<()>::new();
    loop {
        let command = tokio::select! {
            biased;
            command = receiver.recv() => {
                let Some(command) = command else { break };
                command
            }
            notice = notice_receiver.recv() => {
                let Some(notice) = notice else { break };
                handle_notice(
                    &dependencies,
                    &active,
                    notice,
                    shutting_down,
                    &removal_blocks,
                    &mut removal_waiters,
                    &mut reconcile_timers,
                )
                .await;
                if shutting_down && active.read().await.is_empty() {
                    break;
                }
                continue;
            }
            Some(timer) = reconcile_timers.join_next(), if !reconcile_timers.is_empty() => {
                match timer {
                    Ok(command) => command,
                    Err(_) => {
                        dependencies.logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_reconcile_timer_failed",
                            &crate::node::logging::new_correlation_id(),
                            serde_json::json!({ "error_code": "runtime_task_failed" }),
                        );
                        continue;
                    }
                }
            }
            Some(task) = instance_tasks.join_next(), if !instance_tasks.is_empty() => {
                if task.is_err() {
                    dependencies.logger.emit(
                        LogLevel::Error,
                        "oll::plugin",
                        "plugin_instance_owner_task_failed",
                        &crate::node::logging::new_correlation_id(),
                        serde_json::json!({ "error_code": "runtime_task_failed" }),
                    );
                }
                continue;
            }
            () = tokio::time::sleep_until(
                shutdown_deadline.unwrap_or_else(Instant::now)
            ), if shutdown_deadline.is_some() => {
                break;
            }
        };
        match command {
            ControllerCommand::Reconcile {
                plugin_id,
                correlation_id,
                explicit,
                retry_attempt,
            } if !shutting_down => {
                if explicit {
                    removal_blocks.remove(&plugin_id);
                }
                if !removal_blocks.contains(&plugin_id) {
                    reconcile_one(
                        ReconcileContext {
                            dependencies: &dependencies,
                            active: &active,
                            controller: &sender,
                            package_gate_waiters: &mut package_gate_waiters,
                            reconcile_timers: &mut reconcile_timers,
                        },
                        plugin_id,
                        correlation_id,
                        retry_attempt,
                    )
                    .await;
                }
            }
            ControllerCommand::ReconcileAll {
                correlation_id,
                retry_attempt,
            } if !shutting_down => match dependencies.store.list_plugins().await {
                Ok(plugins) => {
                    for plugin in plugins {
                        if !removal_blocks.contains(&plugin.plugin_id) {
                            reconcile_one(
                                ReconcileContext {
                                    dependencies: &dependencies,
                                    active: &active,
                                    controller: &sender,
                                    package_gate_waiters: &mut package_gate_waiters,
                                    reconcile_timers: &mut reconcile_timers,
                                },
                                plugin.plugin_id,
                                correlation_id.clone(),
                                0,
                            )
                            .await;
                        }
                    }
                }
                Err(error) => {
                    lifecycle_error(
                        &dependencies,
                        "plugin_reconcile_failed",
                        &correlation_id,
                        None,
                        &error,
                    );
                    let next_attempt = retry_attempt.saturating_add(1);
                    let delay = reconcile_retry_backoff(next_attempt);
                    reconcile_timers.spawn(async move {
                        tokio::time::sleep(delay).await;
                        ControllerCommand::ReconcileAll {
                            correlation_id,
                            retry_attempt: next_attempt,
                        }
                    });
                }
            },
            ControllerCommand::PackageGateReady {
                plugin_id,
                correlation_id,
                retry_attempt,
                gate,
            } => {
                if let Some(mut waiter) = package_gate_waiters.remove(&plugin_id) {
                    let _ = (&mut waiter).await;
                }
                if !shutting_down && !removal_blocks.contains(&plugin_id) {
                    reconcile_with_package_gate(
                        SpawnContext {
                            dependencies: &dependencies,
                            active: &active,
                            instance_tasks: &mut instance_tasks,
                            notices: &notices,
                            reconcile_timers: &mut reconcile_timers,
                        },
                        plugin_id,
                        correlation_id,
                        retry_attempt,
                        gate,
                    )
                    .await;
                }
            }
            ControllerCommand::SettleEnded {
                plugin_id,
                instance_id,
                expected,
                failure,
                correlation_id,
                ended_at,
                retry_attempt,
            } => {
                settle_ended_instance(
                    &dependencies,
                    &active,
                    plugin_id,
                    instance_id,
                    expected,
                    failure,
                    correlation_id,
                    ended_at,
                    retry_attempt,
                    shutting_down,
                    &removal_blocks,
                    &mut removal_waiters,
                    &mut reconcile_timers,
                )
                .await;
            }
            ControllerCommand::StopForRemoval {
                plugin_id,
                deadline,
                correlation_id,
                response,
            } => {
                removal_blocks.insert(plugin_id.clone());
                if let Some(mut waiter) = package_gate_waiters.remove(&plugin_id) {
                    waiter.abort();
                    let _ = (&mut waiter).await;
                }
                let instance = {
                    let mut active = active.write().await;
                    active.get_mut(&plugin_id).map(|instance| {
                        instance.state = ObservedPluginState::Stopping;
                        instance.sender.clone()
                    })
                };
                if let Some(instance) = instance {
                    removal_waiters.insert(plugin_id, response);
                    instance.shutdown(InstanceShutdown {
                        reason: "plugin removal".to_owned(),
                        correlation_id,
                        deadline,
                    });
                } else {
                    let _ = response.send(Ok(()));
                }
            }
            ControllerCommand::Shutdown {
                deadline,
                correlation_id,
                response,
            } => {
                shutting_down = true;
                shutdown_deadline = Some(
                    shutdown_deadline.map_or(deadline, |current: Instant| current.min(deadline)),
                );
                shutdown_response = Some(response);
                let mut gate_waiters = package_gate_waiters
                    .drain()
                    .map(|(_, waiter)| waiter)
                    .collect::<Vec<_>>();
                for waiter in &gate_waiters {
                    waiter.abort();
                }
                for waiter in &mut gate_waiters {
                    let _ = waiter.await;
                }
                let instances = {
                    let mut active = active.write().await;
                    active
                        .values_mut()
                        .map(|instance| {
                            instance.state = ObservedPluginState::Stopping;
                            instance.sender.clone()
                        })
                        .collect::<Vec<_>>()
                };
                for instance in instances {
                    instance.shutdown(InstanceShutdown {
                        reason: "daemon shutdown".to_owned(),
                        correlation_id: correlation_id.clone(),
                        deadline,
                    });
                }
            }
            #[cfg(test)]
            ControllerCommand::Barrier { response } => {
                let _ = response.send(());
            }
            ControllerCommand::Reconcile { .. } | ControllerCommand::ReconcileAll { .. } => {}
        }

        if shutting_down && active.read().await.is_empty() {
            break;
        }
    }

    for (_, mut waiter) in package_gate_waiters.drain() {
        waiter.abort();
        let _ = (&mut waiter).await;
    }
    reconcile_timers.abort_all();
    while reconcile_timers.join_next().await.is_some() {}
    instance_tasks.abort_all();
    while instance_tasks.join_next().await.is_some() {}
    if let Some(response) = shutdown_response.take() {
        let _ = response.send(());
    }
}
