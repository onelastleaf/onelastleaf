use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::{sync::oneshot, time::Instant};

use crate::{
    node::logging::LogLevel,
    plugin::{
        PluginError, PluginId, PluginName, PluginSelector,
        package::{
            InstallRemoteRequest, PackageManager, PackageOperationOutcome, PackageOperationResult,
            ReleaseListing, RemovalPreparation,
        },
        runtime::PluginSupervisor,
    },
};

use super::{PluginRuntime, operations::OperationContext};

const REMOVAL_PROCESS_DEADLINE: Duration = Duration::from_secs(15);

impl PluginRuntime {
    pub async fn install_declared(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let started = Instant::now();
        self.package_operation_started("install_declared", correlation_id, None);
        let result = self.packages.install_declared(correlation_id).await;
        self.package_operation_finished("install_declared", correlation_id, started, &result);
        result
    }

    pub async fn install_remote(
        &self,
        request: InstallRemoteRequest,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let started = Instant::now();
        self.package_operation_started("install_remote", correlation_id, None);
        let result = self.packages.install_remote(request, correlation_id).await;
        self.package_operation_finished("install_remote", correlation_id, started, &result);
        result
    }

    pub async fn update(
        &self,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let started = Instant::now();
        self.package_operation_started("update", correlation_id, Some(selector.as_str()));
        let result = self.packages.update(selector, correlation_id).await;
        self.package_operation_finished("update", correlation_id, started, &result);
        result
    }

    pub async fn reconcile_exact(
        self: &Arc<Self>,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let started = Instant::now();
        self.package_operation_started("reconcile_exact", correlation_id, None);
        let runtime = Arc::clone(self);
        let inherited_correlation = correlation_id.to_owned();
        let result = self
            .packages
            .reconcile_exact_with(correlation_id, move |plugin_id| {
                let runtime = Arc::clone(&runtime);
                let correlation_id = inherited_correlation.clone();
                async move {
                    let result = runtime.remove_id(&plugin_id, &correlation_id).await;
                    if let Err(error) = &result {
                        runtime.logger.emit(
                            LogLevel::Error,
                            "oll::plugin::package",
                            "plugin_package_removal_failed",
                            &correlation_id,
                            json!({
                                "operation": "reconcile_exact",
                                "plugin_id": plugin_id.as_str(),
                                "error_code": error.code(),
                            }),
                        );
                    }
                    result
                }
            })
            .await;
        self.package_operation_finished("reconcile_exact", correlation_id, started, &result);
        result
    }

    pub async fn remove(
        self: &Arc<Self>,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<PackageOperationResult, PluginError> {
        let started = Instant::now();
        let installed = self.store.get_plugin(selector).await?;
        self.package_operation_started(
            "remove",
            correlation_id,
            Some(installed.plugin_id.as_str()),
        );
        let result = self.remove_id(&installed.plugin_id, correlation_id).await;
        match &result {
            Ok(result) => self.log_package_results(
                "remove",
                correlation_id,
                started,
                std::slice::from_ref(result),
            ),
            Err(error) => self.log_package_error("remove", correlation_id, started, error),
        }
        result
    }

    pub async fn list_releases(
        &self,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<(PluginId, Vec<ReleaseListing>), PluginError> {
        let started = Instant::now();
        self.package_operation_started("list_releases", correlation_id, Some(selector.as_str()));
        let result = self.packages.list_releases(selector, correlation_id).await;
        match &result {
            Ok((plugin_id, releases)) => self.logger.emit(
                LogLevel::Info,
                "oll::plugin::package",
                "plugin_package_operation_result",
                correlation_id,
                json!({
                    "operation": "list_releases",
                    "plugin_id": plugin_id.as_str(),
                    "outcome": "succeeded",
                    "release_count": releases.len(),
                    "duration_ms": elapsed_millis(started),
                }),
            ),
            Err(error) => self.log_package_error("list_releases", correlation_id, started, error),
        }
        result
    }

    async fn remove_id(
        self: &Arc<Self>,
        plugin_id: &PluginId,
        correlation_id: &str,
    ) -> Result<PackageOperationResult, PluginError> {
        let (response, result) = oneshot::channel();
        let packages = self.packages.clone();
        let supervisor = Arc::clone(&self.supervisor);
        let plugin_id = plugin_id.clone();
        let correlation_id = correlation_id.to_owned();
        self.operations
            .spawn(
                OperationContext::new("remove_plugin", correlation_id.clone()),
                async move {
                    let removal = async {
                        let preparation =
                            packages.begin_removal(&plugin_id, &correlation_id).await?;
                        continue_removal(packages, supervisor, preparation, &correlation_id).await
                    }
                    .await;
                    let _ = response.send(removal);
                },
            )
            .await?;
        result.await.map_err(|_| {
            PluginError::FailedPrecondition(
                "plugin removal continuation ended without a result".to_owned(),
            )
        })?
    }

    fn package_operation_started(
        &self,
        operation: &str,
        correlation_id: &str,
        selector: Option<&str>,
    ) {
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_operation_started",
            correlation_id,
            json!({ "operation": operation, "selector": selector }),
        );
    }

    fn package_operation_finished(
        &self,
        operation: &str,
        correlation_id: &str,
        started: Instant,
        result: &Result<Vec<PackageOperationResult>, PluginError>,
    ) {
        match result {
            Ok(results) => self.log_package_results(operation, correlation_id, started, results),
            Err(error) => self.log_package_error(operation, correlation_id, started, error),
        }
    }

    fn log_package_results(
        &self,
        operation: &str,
        correlation_id: &str,
        started: Instant,
        results: &[PackageOperationResult],
    ) {
        let duration_ms = elapsed_millis(started);
        for result in results {
            self.logger.emit(
                if result.outcome == PackageOperationOutcome::Failed {
                    LogLevel::Error
                } else {
                    LogLevel::Info
                },
                "oll::plugin::package",
                "plugin_package_operation_result",
                correlation_id,
                json!({
                    "operation": operation,
                    "plugin_id": result.plugin_id.as_ref().map(PluginId::as_str),
                    "plugin_name": result.plugin_name.as_ref().map(PluginName::as_str),
                    "outcome": result.outcome.as_str(),
                    "error_codes": result.diagnostics.iter().map(|value| value.code.as_str()).collect::<Vec<_>>(),
                    "package_phases": result.diagnostics.iter().map(|value| value.phase.as_str()).collect::<Vec<_>>(),
                    "duration_ms": duration_ms,
                }),
            );
        }
        let (overall_outcome, failed_count) = package_completion_outcome(results);
        self.logger.emit(
            if failed_count == 0 {
                LogLevel::Info
            } else {
                LogLevel::Error
            },
            "oll::plugin::package",
            "plugin_package_operation_completed",
            correlation_id,
            json!({
                "operation": operation,
                "outcome": overall_outcome,
                "result_count": results.len(),
                "failed_count": failed_count,
                "duration_ms": duration_ms,
            }),
        );
    }

    fn log_package_error(
        &self,
        operation: &str,
        correlation_id: &str,
        started: Instant,
        error: &PluginError,
    ) {
        self.logger.emit(
            LogLevel::Error,
            "oll::plugin::package",
            "plugin_package_operation_failed",
            correlation_id,
            json!({
                "operation": operation,
                "error_code": error.code(),
                "duration_ms": elapsed_millis(started),
            }),
        );
    }
}

fn package_completion_outcome(results: &[PackageOperationResult]) -> (&'static str, usize) {
    let failed = results
        .iter()
        .filter(|result| result.outcome == PackageOperationOutcome::Failed)
        .count();
    let outcome = if failed == 0 {
        "succeeded"
    } else if failed == results.len() {
        "failed"
    } else {
        "partial_failure"
    };
    (outcome, failed)
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn continue_removal(
    packages: PackageManager,
    supervisor: Arc<PluginSupervisor>,
    preparation: RemovalPreparation,
    correlation_id: &str,
) -> Result<PackageOperationResult, PluginError> {
    let plugin_id = preparation.plugin_id().clone();
    supervisor
        .stop_for_removal(
            &plugin_id,
            Instant::now() + REMOVAL_PROCESS_DEADLINE,
            correlation_id,
        )
        .await?;
    supervisor.settle_artifact_publications(&plugin_id).await?;
    match packages.finish_removal(preparation).await {
        Err(error @ PluginError::Aborted(_)) => {
            // The prepared intent was cleared because plugins.lua changed.
            // Restore supervision from unchanged SQL desired state.
            let _ = supervisor
                .reconcile_plugin(&plugin_id, correlation_id)
                .await;
            Err(error)
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package_result(outcome: PackageOperationOutcome) -> PackageOperationResult {
        PackageOperationResult {
            plugin_id: None,
            plugin_name: None,
            outcome,
            diagnostics: Vec::new(),
            confirmation_summary: None,
            confirmation_digest: None,
        }
    }

    #[test]
    fn package_completion_distinguishes_empty_success_and_partial_failure() {
        assert_eq!(package_completion_outcome(&[]), ("succeeded", 0));
        assert_eq!(
            package_completion_outcome(&[
                package_result(PackageOperationOutcome::Installed),
                package_result(PackageOperationOutcome::Failed),
            ]),
            ("partial_failure", 1),
        );
        assert_eq!(
            package_completion_outcome(&[package_result(PackageOperationOutcome::Failed)]),
            ("failed", 1),
        );
    }
}
