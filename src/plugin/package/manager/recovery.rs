use std::collections::BTreeSet;

use serde_json::json;

use crate::node::logging::LogLevel;

use super::*;

impl PackageManager {
    pub async fn recover(&self, startup_correlation_id: &str) -> Result<(), PluginError> {
        let recovery_started = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_recovery_started",
            startup_correlation_id,
            json!({ "package_phase": "recovery" }),
        );
        let recovery = async {
            for intent in self.store.package_publish_intents().await? {
                let intent_started = std::time::Instant::now();
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_publication_recovery_started",
                    &intent.correlation_id,
                    json!({
                        "package_phase": "recovery",
                        "recovery_kind": "publication",
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                        "install_generation": intent.candidate_generation.to_string(),
                    }),
                );
                let result: Result<(), PluginError> = async {
                    let disk = self
                        .layout
                        .current_generation(&intent.plugin_id)
                        .map_err(package_configuration_error)?;
                    if !matches!(self.publish_inputs_match(&intent), Ok(true)) {
                        self.discard_stale_publish(&intent, disk).await?;
                        return Ok(());
                    }
                    if disk == Some(intent.candidate_generation) {
                        self.store
                            .finalize_package_publish(
                                &intent.plugin_id,
                                intent.candidate_generation,
                            )
                            .await?;
                    } else if disk == intent.expected_current_generation
                        && (self
                            .layout
                            .plugin_root(&intent.plugin_id)
                            .join("candidates")
                            .join(intent.candidate_generation.to_string())
                            .exists()
                            || self
                                .layout
                                .generation(&intent.plugin_id, intent.candidate_generation)
                                .exists())
                    {
                        self.layout
                            .publish_candidate(
                                &intent.plugin_id,
                                intent.candidate_generation,
                                intent.expected_current_generation,
                            )
                            .map_err(package_configuration_error)?;
                        self.store
                            .finalize_package_publish(
                                &intent.plugin_id,
                                intent.candidate_generation,
                            )
                            .await?;
                    } else {
                        self.discard_stale_publish(&intent, disk).await?;
                    }
                    Ok(())
                }
                .await;
                self.logger.emit(
                    if result.is_ok() {
                        LogLevel::Info
                    } else {
                        LogLevel::Error
                    },
                    "oll::plugin::package",
                    if result.is_ok() {
                        "plugin_package_publication_recovery_succeeded"
                    } else {
                        "plugin_package_publication_recovery_failed"
                    },
                    &intent.correlation_id,
                    json!({
                        "package_phase": "recovery",
                        "recovery_kind": "publication",
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                        "install_generation": intent.candidate_generation.to_string(),
                        "error_code": result.as_ref().err().map(PluginError::code),
                        "duration_ms": intent_started.elapsed().as_millis(),
                    }),
                );
                result?;
            }
            for intent in self.store.removal_intents().await? {
                let intent_started = std::time::Instant::now();
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_removal_recovery_started",
                    &intent.correlation_id,
                    json!({
                        "package_phase": "recovery",
                        "recovery_kind": "removal",
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                        "removal_phase": intent.phase.as_str(),
                    }),
                );
                let correlation_id = intent.correlation_id.clone();
                let operation_id = intent.operation_id.clone();
                let plugin_id = intent.plugin_id.clone();
                let _gate = self.gates.lock(&plugin_id).await;
                let result = self.complete_removal(intent).await;
                let resolved = matches!(result, Ok(()) | Err(PluginError::Aborted(_)));
                self.logger.emit(
                    if resolved {
                        LogLevel::Info
                    } else {
                        LogLevel::Error
                    },
                    "oll::plugin::package",
                    if resolved {
                        "plugin_package_removal_recovery_succeeded"
                    } else {
                        "plugin_package_removal_recovery_failed"
                    },
                    &correlation_id,
                    json!({
                        "package_phase": "recovery",
                        "recovery_kind": "removal",
                        "plugin_id": plugin_id.as_str(),
                        "package_operation_id": operation_id,
                        "error_code": result.as_ref().err().map(PluginError::code),
                        "duration_ms": intent_started.elapsed().as_millis(),
                    }),
                );
                match result {
                    Ok(()) | Err(PluginError::Aborted(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            let installed = self.store.list_plugins().await?;
            let authoritative_plugin_ids = installed
                .iter()
                .map(|plugin| plugin.plugin_id.clone())
                .collect::<BTreeSet<_>>();
            for plugin in installed {
                let mut retained = BTreeSet::from([plugin.current_generation]);
                if let Some(running) = plugin.running_generation {
                    retained.insert(running);
                }
                let _ = self.layout.prune_generations(&plugin.plugin_id, &retained);
            }
            let retained = self
                .store
                .package_publish_intents()
                .await?
                .into_iter()
                .map(|intent| (intent.plugin_id, intent.candidate_generation))
                .collect();
            self.layout
                .cleanup_incomplete_staging(&authoritative_plugin_ids, &retained)
                .map_err(package_configuration_error)?;
            Ok(())
        }
        .await;
        self.logger.emit(
            if recovery.is_ok() {
                LogLevel::Info
            } else {
                LogLevel::Error
            },
            "oll::plugin::package",
            if recovery.is_ok() {
                "plugin_package_recovery_succeeded"
            } else {
                "plugin_package_recovery_failed"
            },
            startup_correlation_id,
            json!({
                "package_phase": "recovery",
                "error_code": recovery.as_ref().err().map(PluginError::code),
                "duration_ms": recovery_started.elapsed().as_millis(),
            }),
        );
        recovery
    }
}
