use std::{path::PathBuf, sync::Arc};

use serde_json::json;
use time::OffsetDateTime;
use tokio::{sync::oneshot, time::Instant};

use crate::{
    configuration::ConfigRuntime,
    node::{
        ParentLivenessPipe,
        identity::IdentityCoordinator,
        logging::{LogLevel, NodeLogger},
    },
    replica::ReplicaRuntime,
};

use super::{
    PluginRuntime,
    operations::{OperationContext, operation_result_lost},
};
use crate::plugin::{
    DesiredPluginState, PluginError, PluginSelector, PluginStore,
    artifact::{ArtifactPublisher, MAX_ARTIFACT_CHUNK_BYTES},
    package::{PackageLayout, PackageManager},
    runtime::PluginSupervisor,
};

impl PluginRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start(
        config_root: PathBuf,
        plugin_data_root: PathBuf,
        artifact_download_dir: PathBuf,
        config: ConfigRuntime,
        replica: Arc<ReplicaRuntime>,
        identities: Arc<IdentityCoordinator>,
        logger: Arc<NodeLogger>,
        parent_liveness: Arc<ParentLivenessPipe>,
        startup_correlation_id: &str,
    ) -> Result<Arc<Self>, PluginError> {
        let store = PluginStore::initialize(replica.database_pool()).await?;
        let layout = PackageLayout::initialize(plugin_data_root)
            .map_err(|error| PluginError::FailedPrecondition(error.to_string()))?;
        let (package_shutdown, package_shutdown_rx) = tokio::sync::watch::channel(None);
        let packages = PackageManager::new(
            config_root.clone(),
            layout.clone(),
            store.clone(),
            package_shutdown_rx,
            Arc::clone(&logger),
        );

        // Artifact intents must be recovered before the preceding daemon's
        // nonterminal jobs are failed. Clearing stale instance ownership must
        // in turn precede package/removal recovery.
        let (artifacts, artifact_recovery) = ArtifactPublisher::initialize(
            store.clone(),
            &artifact_download_dir,
            MAX_ARTIFACT_CHUNK_BYTES,
            Arc::clone(&logger),
            OffsetDateTime::now_utc(),
            startup_correlation_id,
        )
        .await?;
        let recovered_nonterminal_jobs = store
            .fail_nonterminal_jobs_on_startup(OffsetDateTime::now_utc())
            .await?;
        packages.recover(startup_correlation_id).await?;
        let supervisor = PluginSupervisor::start(
            store.clone(),
            layout,
            packages.gates(),
            config_root,
            config,
            replica,
            identities,
            Arc::clone(&logger),
            artifacts,
            parent_liveness,
            recovered_nonterminal_jobs,
            startup_correlation_id,
        )
        .await?;
        let runtime = Arc::new(Self {
            store,
            packages,
            supervisor,
            package_shutdown,
            operations: super::operations::OperationTracker::new(Arc::clone(&logger)),
            logger,
        });
        runtime.logger.emit(
            LogLevel::Info,
            "oll::plugin",
            "plugin_system_ready",
            startup_correlation_id,
            json!({
                "artifact_intents_recovered": artifact_recovery.recovered,
                "artifact_intents_failed": artifact_recovery.failed,
            }),
        );
        Ok(runtime)
    }

    pub async fn set_desired_state(
        self: &Arc<Self>,
        selector: &PluginSelector,
        desired_state: DesiredPluginState,
        correlation_id: &str,
    ) -> Result<crate::plugin::InstalledPlugin, PluginError> {
        let installed = self.store.get_plugin(selector).await?;
        let plugin_id = installed.plugin_id;
        let store = self.store.clone();
        let supervisor = Arc::clone(&self.supervisor);
        let logger = Arc::clone(&self.logger);
        let correlation_id = correlation_id.to_owned();
        let (response, result) = oneshot::channel();
        self.operations
            .spawn(
                OperationContext::new("set_desired_state", correlation_id.clone()),
                async move {
                    let updated = match store.set_desired_state(&plugin_id, desired_state).await {
                        Ok(updated) => updated,
                        Err(error) => {
                            let _ = response.send(Err(error));
                            return;
                        }
                    };
                    let _ = response.send(Ok(updated.clone()));
                    logger.emit(
                        LogLevel::Info,
                        "oll::plugin",
                        "plugin_desired_state_changed",
                        &correlation_id,
                        json!({
                            "plugin_id": updated.plugin_id.as_str(),
                            "plugin_name": updated.plugin_name.as_str(),
                            "plugin_desired_state": desired_state.as_str(),
                        }),
                    );
                    if let Err(error) = supervisor
                        .reconcile_plugin(&updated.plugin_id, &correlation_id)
                        .await
                    {
                        logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_reconcile_enqueue_failed",
                            &correlation_id,
                            json!({
                                "plugin_id": updated.plugin_id.as_str(),
                                "operation": "set_desired_state",
                                "error_code": error.code(),
                            }),
                        );
                    }
                },
            )
            .await?;
        result.await.map_err(operation_result_lost)?
    }

    pub async fn restart(
        self: &Arc<Self>,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<crate::plugin::InstalledPlugin, PluginError> {
        let installed = self.store.get_plugin(selector).await?;
        let plugin_id = installed.plugin_id;
        let store = self.store.clone();
        let supervisor = Arc::clone(&self.supervisor);
        let logger = Arc::clone(&self.logger);
        let correlation_id = correlation_id.to_owned();
        let (response, result) = oneshot::channel();
        self.operations
            .spawn(
                OperationContext::new("restart_plugin", correlation_id.clone()),
                async move {
                    let updated = match store.request_restart(&plugin_id).await {
                        Ok(updated) => updated,
                        Err(error) => {
                            let _ = response.send(Err(error));
                            return;
                        }
                    };
                    let _ = response.send(Ok(updated.clone()));
                    logger.emit(
                        LogLevel::Info,
                        "oll::plugin",
                        "plugin_restart_requested",
                        &correlation_id,
                        json!({
                            "plugin_id": updated.plugin_id.as_str(),
                            "plugin_name": updated.plugin_name.as_str(),
                            "restart_sequence": updated.restart_sequence,
                        }),
                    );
                    if let Err(error) = supervisor
                        .reconcile_plugin(&updated.plugin_id, &correlation_id)
                        .await
                    {
                        logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_reconcile_enqueue_failed",
                            &correlation_id,
                            json!({
                                "plugin_id": updated.plugin_id.as_str(),
                                "operation": "restart_plugin",
                                "error_code": error.code(),
                            }),
                        );
                    }
                },
            )
            .await?;
        result.await.map_err(operation_result_lost)?
    }

    pub async fn shutdown(
        &self,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        self.package_shutdown.send_replace(Some(deadline));
        let packages = self.packages.shutdown(deadline);
        let operations = self.operations.shutdown(deadline);
        let artifacts = self
            .supervisor
            .shutdown_artifact_publications(deadline, correlation_id);
        let supervisor = self.supervisor.shutdown(deadline, correlation_id);
        let (packages, operations, artifacts, supervisor) =
            tokio::join!(packages, operations, artifacts, supervisor);
        packages?;
        operations?;
        artifacts?;
        supervisor?;
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin",
            "plugin_system_stopped",
            correlation_id,
            json!({}),
        );
        Ok(())
    }
}
