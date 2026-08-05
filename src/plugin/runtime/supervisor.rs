mod controller;
mod jobs;
mod lifecycle;
mod reconciliation;
mod settlement;

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use time::OffsetDateTime;
use tokio::{
    sync::{Mutex, RwLock, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

use crate::{
    configuration::ConfigRuntime,
    node::{
        ParentLivenessPipe,
        identity::IdentityCoordinator,
        logging::{LogLevel, NodeLogger},
    },
    plugin::{
        ArtifactPublisher, ObservedPluginState, PluginError, PluginId, PluginInstanceId,
        PluginStore,
        package::{PackageLayout, PluginPackageGates},
    },
    replica::ReplicaRuntime,
};

#[cfg(test)]
use super::InstanceCommand;
use super::{InstanceSender, RuntimeDependencies};
use controller::run_controller;
#[cfg(test)]
pub(super) use jobs::{dispatch_job_cancellation, retained_operation};

const CONTROLLER_CAPACITY: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginAction {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginSessionSnapshot {
    pub state: ObservedPluginState,
    pub instance_id: PluginInstanceId,
    pub install_generation: Uuid,
    pub process_id: Option<u32>,
    pub started_at: Option<OffsetDateTime>,
    pub ready_at: Option<OffsetDateTime>,
    pub actions: Vec<PluginAction>,
}

#[cfg(test)]
pub(crate) struct SaturatedInstanceWorkQueue {
    _permits: Vec<mpsc::OwnedPermit<InstanceCommand>>,
}

#[derive(Clone)]
pub(super) struct ActiveInstance {
    pub(super) instance_id: PluginInstanceId,
    pub(super) generation: Uuid,
    pub(super) sender: InstanceSender,
    pub(super) state: ObservedPluginState,
    pub(super) process_id: Option<u32>,
    pub(super) started_at: Option<OffsetDateTime>,
    pub(super) ready_at: Option<OffsetDateTime>,
    pub(super) actions: Vec<PluginAction>,
}

pub(super) enum ControllerCommand {
    Reconcile {
        plugin_id: PluginId,
        correlation_id: String,
        explicit: bool,
        retry_attempt: u32,
    },
    ReconcileAll {
        correlation_id: String,
        retry_attempt: u32,
    },
    PackageGateReady {
        plugin_id: PluginId,
        correlation_id: String,
        retry_attempt: u32,
        gate: tokio::sync::OwnedMutexGuard<()>,
    },
    SettleEnded {
        plugin_id: PluginId,
        instance_id: PluginInstanceId,
        expected: bool,
        failure: Option<String>,
        correlation_id: String,
        ended_at: OffsetDateTime,
        retry_attempt: u32,
    },
    StopForRemoval {
        plugin_id: PluginId,
        deadline: Instant,
        correlation_id: String,
        response: oneshot::Sender<Result<(), PluginError>>,
    },
    Shutdown {
        deadline: Instant,
        correlation_id: String,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    Barrier { response: oneshot::Sender<()> },
}

/// Event-driven owner for every direct plugin child and active runtime session.
pub struct PluginSupervisor {
    dependencies: RuntimeDependencies,
    active: Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    controller: mpsc::Sender<ControllerCommand>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl PluginSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start(
        store: PluginStore,
        packages: PackageLayout,
        package_gates: Arc<PluginPackageGates>,
        config_root: PathBuf,
        config: ConfigRuntime,
        replica: Arc<ReplicaRuntime>,
        identities: Arc<IdentityCoordinator>,
        logger: Arc<NodeLogger>,
        artifacts: ArtifactPublisher,
        parent_liveness: Arc<ParentLivenessPipe>,
        recovered_nonterminal_jobs: u64,
        startup_correlation_id: &str,
    ) -> Result<Arc<Self>, PluginError> {
        let dependencies = RuntimeDependencies {
            store,
            packages,
            package_gates,
            config_root,
            config,
            replica,
            identities,
            logger,
            artifacts,
            parent_liveness,
        };
        let active = Arc::new(RwLock::new(BTreeMap::new()));
        let (controller, receiver) = mpsc::channel(CONTROLLER_CAPACITY);
        let (notices, notice_receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_controller(
            dependencies.clone(),
            Arc::clone(&active),
            controller.clone(),
            receiver,
            notices,
            notice_receiver,
        ));
        let supervisor = Arc::new(Self {
            dependencies,
            active,
            controller,
            task: Mutex::new(Some(task)),
        });
        supervisor.dependencies.logger.emit(
            LogLevel::Info,
            "oll::plugin",
            "plugin_runtime_started",
            startup_correlation_id,
            serde_json::json!({ "recovered_nonterminal_jobs": recovered_nonterminal_jobs }),
        );
        supervisor
            .send_controller(ControllerCommand::ReconcileAll {
                correlation_id: startup_correlation_id.to_owned(),
                retry_attempt: 0,
            })
            .await?;
        Ok(supervisor)
    }

    pub async fn session_snapshot(&self, plugin_id: &PluginId) -> Option<PluginSessionSnapshot> {
        self.active
            .read()
            .await
            .get(plugin_id)
            .map(|instance| PluginSessionSnapshot {
                state: instance.state,
                instance_id: instance.instance_id,
                install_generation: instance.generation,
                process_id: instance.process_id,
                started_at: instance.started_at,
                ready_at: instance.ready_at,
                actions: instance.actions.clone(),
            })
    }

    pub async fn session_snapshots(&self) -> BTreeMap<PluginId, PluginSessionSnapshot> {
        self.active
            .read()
            .await
            .iter()
            .map(|(plugin_id, instance)| {
                (
                    plugin_id.clone(),
                    PluginSessionSnapshot {
                        state: instance.state,
                        instance_id: instance.instance_id,
                        install_generation: instance.generation,
                        process_id: instance.process_id,
                        started_at: instance.started_at,
                        ready_at: instance.ready_at,
                        actions: instance.actions.clone(),
                    },
                )
            })
            .collect()
    }

    pub async fn reconcile_plugin(
        &self,
        plugin_id: &PluginId,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        require_correlation(correlation_id)?;
        self.send_controller(ControllerCommand::Reconcile {
            plugin_id: plugin_id.clone(),
            correlation_id: correlation_id.to_owned(),
            explicit: true,
            retry_attempt: 0,
        })
        .await
    }

    pub async fn stop_for_removal(
        &self,
        plugin_id: &PluginId,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        require_correlation(correlation_id)?;
        let (response, result) = oneshot::channel();
        self.send_controller(ControllerCommand::StopForRemoval {
            plugin_id: plugin_id.clone(),
            deadline,
            correlation_id: correlation_id.to_owned(),
            response,
        })
        .await?;
        tokio::time::timeout_at(deadline, result)
            .await
            .map_err(|_| {
                PluginError::FailedPrecondition(
                    "plugin removal stop exceeded the shutdown deadline".to_owned(),
                )
            })?
            .map_err(|_| {
                PluginError::FailedPrecondition(
                    "plugin supervisor stopped before removal cleanup".to_owned(),
                )
            })?
    }

    pub(crate) async fn settle_artifact_publications(
        &self,
        plugin_id: &PluginId,
    ) -> Result<(), PluginError> {
        self.dependencies
            .artifacts
            .settle_plugin_publications(plugin_id)
            .await
    }

    pub(crate) async fn shutdown_artifact_publications(
        &self,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        self.dependencies
            .artifacts
            .shutdown_publications(deadline, correlation_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn controller_barrier(&self) -> Result<(), PluginError> {
        let (response, result) = oneshot::channel();
        self.send_controller(ControllerCommand::Barrier { response })
            .await?;
        result.await.map_err(|_| {
            PluginError::FailedPrecondition(
                "plugin supervisor stopped before its test barrier".to_owned(),
            )
        })
    }

    #[cfg(test)]
    pub(crate) async fn saturate_instance_work_queue(
        &self,
        plugin_id: &PluginId,
    ) -> Result<SaturatedInstanceWorkQueue, PluginError> {
        let sender = self
            .active
            .read()
            .await
            .get(plugin_id)
            .map(|instance| instance.sender.work.clone())
            .ok_or_else(|| {
                PluginError::FailedPrecondition(
                    "plugin has no active instance work queue".to_owned(),
                )
            })?;
        let mut permits = Vec::with_capacity(super::INSTANCE_COMMAND_CAPACITY);
        for _ in 0..super::INSTANCE_COMMAND_CAPACITY {
            permits.push(sender.clone().reserve_owned().await.map_err(|_| {
                PluginError::FailedPrecondition("plugin instance work queue closed".to_owned())
            })?);
        }
        Ok(SaturatedInstanceWorkQueue { _permits: permits })
    }

    pub async fn shutdown(
        &self,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        require_correlation(correlation_id)?;
        let (response, result) = oneshot::channel();
        self.send_controller(ControllerCommand::Shutdown {
            deadline,
            correlation_id: correlation_id.to_owned(),
            response,
        })
        .await?;
        let response = tokio::time::timeout_at(deadline, result).await;
        let task = self.task.lock().await.take();
        match response {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                if let Some(mut task) = task {
                    task.abort();
                    let _ = (&mut task).await;
                }
                return Err(PluginError::FailedPrecondition(
                    "plugin supervisor stopped before shutdown completed".to_owned(),
                ));
            }
            Err(_) => {
                if let Some(mut task) = task {
                    task.abort();
                    let _ = (&mut task).await;
                }
                return Err(PluginError::FailedPrecondition(
                    "plugin shutdown exceeded the node deadline".to_owned(),
                ));
            }
        }
        if let Some(mut task) = task {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(PluginError::FailedPrecondition(
                        "plugin supervisor task failed during shutdown".to_owned(),
                    ));
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    return Err(PluginError::FailedPrecondition(
                        "plugin supervisor task exceeded the node shutdown deadline".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn send_controller(&self, command: ControllerCommand) -> Result<(), PluginError> {
        self.controller.send(command).await.map_err(|_| {
            PluginError::FailedPrecondition("plugin supervisor is not running".to_owned())
        })
    }
}

pub(super) fn require_correlation(correlation_id: &str) -> Result<(), PluginError> {
    if correlation_id.is_empty() {
        Err(PluginError::InvalidArgument(
            "plugin operation correlation ID must not be empty".to_owned(),
        ))
    } else {
        Ok(())
    }
}
