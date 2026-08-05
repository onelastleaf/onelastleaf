use std::{collections::BTreeSet, fs};

use serde_json::json;
use tokio::sync::oneshot;

use crate::node::logging::LogLevel;

use super::*;

impl PackageManager {
    pub(super) async fn publish_candidate(
        &self,
        candidate: PreparedCandidate,
    ) -> PackageOperationResult {
        if self.require_package_admission().await.is_err() {
            return PackageOperationResult::failed(
                Some(candidate.built.plugin_id.clone()),
                Some(candidate.built.plugin_name.clone()),
                PackageDiagnostic::shutting_down(),
            );
        }
        let outcome = if candidate.expected_current.is_some() {
            PackageOperationOutcome::Updated
        } else {
            PackageOperationOutcome::Installed
        };
        let intent = PackagePublishIntent {
            plugin_id: candidate.built.plugin_id.clone(),
            plugin_name: candidate.built.plugin_name.clone(),
            operation_id: candidate.operation_id.clone(),
            expected_current_generation: candidate.expected_current,
            candidate_generation: candidate.built.generation,
            normalized_declaration: candidate.built.declaration_bytes.clone(),
            declaration_sha256: candidate.built.declaration_sha256,
            effective_manifest: candidate.built.effective_manifest_bytes.clone(),
            selected_commit: Some(candidate.built.selected_commit.clone()),
            install_mode: candidate.built.install_mode,
            release_id: candidate.built.release_id.clone(),
            correlation_id: candidate.correlation_id.clone(),
        };
        let plugin_id = intent.plugin_id.clone();
        let plugin_name = intent.plugin_name.clone();
        let publication_started = std::time::Instant::now();
        let install_generation = intent.candidate_generation;
        let operation_id = intent.operation_id.clone();
        let correlation_id = intent.correlation_id.clone();
        let build_log_path = candidate.built.build_log_path.clone();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_publication_started",
            &correlation_id,
            json!({
                "package_phase": "publication",
                "plugin_id": plugin_id.as_str(),
                "plugin_name": plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": install_generation.to_string(),
                "build_log_path": build_log_path.display().to_string(),
            }),
        );
        if let Err(error) = self
            .layout
            .sync_candidate_tree(&intent.plugin_id, intent.candidate_generation)
        {
            self.logger.emit(
                LogLevel::Error,
                "oll::plugin::package",
                "plugin_package_publication_failed",
                &correlation_id,
                json!({
                    "package_phase": "publication",
                    "plugin_id": plugin_id.as_str(),
                    "plugin_name": plugin_name.as_str(),
                    "package_operation_id": operation_id,
                    "install_generation": install_generation.to_string(),
                    "build_log_path": build_log_path.display().to_string(),
                    "error_code": error.code(),
                    "duration_ms": publication_started.elapsed().as_millis(),
                }),
            );
            return PackageOperationResult::failed(
                Some(plugin_id),
                Some(plugin_name),
                PackageDiagnostic::from_package(error).with_declaration(&candidate.declaration),
            );
        }
        let context = DurablePublishContext {
            plugin_id: intent.plugin_id.clone(),
            operation_id: intent.operation_id.clone(),
            correlation_id: intent.correlation_id.clone(),
        };
        let manager = self.clone();
        let task_plugin_id = plugin_id.clone();
        let task_plugin_name = plugin_name.clone();
        let task_operation_id = operation_id.clone();
        let task_correlation_id = correlation_id.clone();
        let task_build_log_path = build_log_path.clone();
        let (response, result) = oneshot::channel();
        if let Err(error) = self
            .package_tasks
            .spawn_publish(context, async move {
                let outcome = manager
                    .complete_durable_publish(candidate, intent, outcome)
                    .await;
                let failed = outcome.outcome == PackageOperationOutcome::Failed;
                manager.logger.emit(
                    if failed {
                        LogLevel::Error
                    } else {
                        LogLevel::Info
                    },
                    "oll::plugin::package",
                    if failed {
                        "plugin_package_publication_failed"
                    } else {
                        "plugin_package_publication_succeeded"
                    },
                    &task_correlation_id,
                    json!({
                        "package_phase": "publication",
                        "plugin_id": task_plugin_id.as_str(),
                        "plugin_name": task_plugin_name.as_str(),
                        "package_operation_id": task_operation_id,
                        "install_generation": install_generation.to_string(),
                        "build_log_path": task_build_log_path.display().to_string(),
                        "outcome": outcome.outcome.as_str(),
                        "error_codes": outcome.diagnostics.iter().map(|diagnostic| diagnostic.code.as_str()).collect::<Vec<_>>(),
                        "duration_ms": publication_started.elapsed().as_millis(),
                    }),
                );
                let _ = response.send(outcome);
            })
            .await
        {
            self.logger.emit(
                LogLevel::Error,
                "oll::plugin::package",
                "plugin_package_publication_failed",
                &correlation_id,
                json!({
                    "package_phase": "publication",
                    "plugin_id": plugin_id.as_str(),
                    "plugin_name": plugin_name.as_str(),
                    "package_operation_id": operation_id,
                    "install_generation": install_generation.to_string(),
                    "build_log_path": build_log_path.display().to_string(),
                    "error_code": error.code(),
                    "duration_ms": publication_started.elapsed().as_millis(),
                }),
            );
            return PackageOperationResult::failed(
                Some(plugin_id),
                Some(plugin_name),
                PackageDiagnostic::store(error),
            );
        }
        match result.await {
            Ok(result) => result,
            Err(_) => {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::plugin::package",
                    "plugin_package_publication_failed",
                    &correlation_id,
                    json!({
                        "package_phase": "publication",
                        "plugin_id": plugin_id.as_str(),
                        "plugin_name": plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "install_generation": install_generation.to_string(),
                        "build_log_path": build_log_path.display().to_string(),
                        "error_code": "publication_task_failed",
                        "duration_ms": publication_started.elapsed().as_millis(),
                    }),
                );
                PackageOperationResult::failed(
                    Some(plugin_id),
                    Some(plugin_name),
                    PackageDiagnostic::store(PluginError::FailedPrecondition(
                        "durable plugin package publication ended without a result".to_owned(),
                    )),
                )
            }
        }
    }

    async fn complete_durable_publish(
        &self,
        mut candidate: PreparedCandidate,
        intent: PackagePublishIntent,
        outcome: PackageOperationOutcome,
    ) -> PackageOperationResult {
        if let Err(error) = self.store.prepare_package_publish(&intent).await {
            return PackageOperationResult::failed(
                Some(intent.plugin_id),
                Some(intent.plugin_name),
                PackageDiagnostic::store(error),
            );
        }
        candidate.recovery_owned = true;
        #[cfg(test)]
        self.pause_publish_test_hook(PublishPause::AfterIntent)
            .await;
        let stale = match self.publish_inputs_match(&intent) {
            Ok(true) => None,
            Ok(false) => Some(PackageError::new(
                "install_publish_failed",
                "publication",
                "plugin declaration or mask changed before publication",
            )),
            Err(error) => Some(error),
        };
        if let Some(error) = stale {
            if let Err(cleanup_error) = self
                .discard_stale_publish(&intent, intent.expected_current_generation)
                .await
            {
                return PackageOperationResult::failed(
                    Some(intent.plugin_id),
                    Some(intent.plugin_name),
                    PackageDiagnostic::store(cleanup_error),
                );
            }
            return PackageOperationResult::failed(
                Some(intent.plugin_id),
                Some(intent.plugin_name),
                PackageDiagnostic::from_package(error).with_declaration(&candidate.declaration),
            );
        }
        if let Err(error) = self.layout.publish_candidate(
            &intent.plugin_id,
            intent.candidate_generation,
            intent.expected_current_generation,
        ) {
            return PackageOperationResult::failed(
                Some(intent.plugin_id),
                Some(intent.plugin_name),
                PackageDiagnostic::from_package(error).with_declaration(&candidate.declaration),
            );
        }
        #[cfg(test)]
        self.pause_publish_test_hook(PublishPause::AfterCurrentSwitch)
            .await;
        match self
            .store
            .finalize_package_publish(&intent.plugin_id, intent.candidate_generation)
            .await
        {
            Ok(plugin) => {
                let mut retained = BTreeSet::from([plugin.current_generation]);
                if let Some(running) = plugin.running_generation {
                    retained.insert(running);
                }
                // Publication is already committed. Obsolete-generation cleanup
                // is recoverable housekeeping and must not turn that success
                // into a client-visible failed install.
                let diagnostics = self
                    .layout
                    .prune_generations(&plugin.plugin_id, &retained)
                    .err()
                    .map(|error| {
                        PackageDiagnostic::from_package(error)
                            .with_declaration(&candidate.declaration)
                    })
                    .into_iter()
                    .collect();
                PackageOperationResult {
                    plugin_id: Some(plugin.plugin_id),
                    plugin_name: Some(plugin.plugin_name),
                    outcome,
                    diagnostics,
                    confirmation_summary: None,
                    confirmation_digest: None,
                }
            }
            Err(error) => PackageOperationResult::failed(
                Some(intent.plugin_id),
                Some(intent.plugin_name),
                PackageDiagnostic::store(error),
            ),
        }
    }

    pub(super) fn publish_inputs_match(
        &self,
        intent: &PackagePublishIntent,
    ) -> Result<bool, PackageError> {
        let declarations = read_plugin_declarations(&self.config_root)?;
        let Some(declaration) = declarations.get(&intent.plugin_id) else {
            return Ok(false);
        };
        let declaration_bytes = serde_json::to_vec(declaration).map_err(|_| {
            PackageError::new(
                "plugin_config_schema",
                "declaration",
                "cannot encode normalized plugin declaration",
            )
        })?;
        if declaration.normalized_sha256() != intent.declaration_sha256
            || declaration_bytes != intent.normalized_declaration
        {
            return Ok(false);
        }

        let generation = self
            .layout
            .pending_generation(&intent.plugin_id, intent.candidate_generation)
            .ok_or_else(|| {
                PackageError::new(
                    "install_publish_failed",
                    "publication",
                    "verified plugin candidate is missing",
                )
            })?;
        let publisher_source =
            fs::read_to_string(generation.join("oll.toml")).map_err(|error| {
                PackageError::io(
                    "manifest_missing",
                    "manifest",
                    "cannot reread candidate publisher manifest",
                    error,
                )
            })?;
        let publisher = PublisherManifest::parse(&publisher_source)?;
        let mask = crate::plugin::package::build::read_mask(&self.config_root, &intent.plugin_id)?;
        let effective = EffectiveManifest::merge(publisher, mask)?;
        let effective_bytes = serde_json::to_vec(&effective)
            .map_err(|_| PackageError::manifest("cannot encode effective plugin manifest"))?;
        Ok(effective_bytes == intent.effective_manifest)
    }

    pub(super) async fn discard_stale_publish(
        &self,
        intent: &PackagePublishIntent,
        disk: Option<Uuid>,
    ) -> Result<(), PluginError> {
        if disk == Some(intent.candidate_generation) {
            self.layout
                .replace_current(
                    &intent.plugin_id,
                    Some(intent.candidate_generation),
                    intent.expected_current_generation,
                )
                .map_err(package_configuration_error)?;
        }
        self.layout
            .discard_unpublished_generation(&intent.plugin_id, intent.candidate_generation)
            .map_err(package_configuration_error)?;
        self.store
            .discard_package_publish_intent(&intent.plugin_id, intent.candidate_generation)
            .await?;
        Ok(())
    }
}
