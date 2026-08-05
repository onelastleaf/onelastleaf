use std::fs;

use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::node::logging::LogLevel;
use crate::plugin::{PluginError, PluginId, PluginSelector, RemovalIntent, RemovalPhase};

use super::{
    PackageManager, PackageOperationOutcome, PackageOperationResult, RemovalPreparation,
    package_configuration_error, require_correlation,
};
use crate::plugin::package::{
    PluginDeclarations, plugins_file_sha256, read_plugin_declarations, write_plugin_declarations,
};

impl PackageManager {
    pub async fn begin_removal(
        &self,
        plugin_id: &PluginId,
        correlation_id: &str,
    ) -> Result<RemovalPreparation, PluginError> {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        let gate = self.gates.lock(plugin_id).await;
        self.require_package_admission().await?;
        let installed = self
            .store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await?;
        if let Some(intent) = self.store.removal_intent(plugin_id).await? {
            return Ok(RemovalPreparation {
                intent,
                plugin_name: installed.plugin_name,
                _gate: gate,
            });
        }
        let _declaration_guard = self.declarations.lock().await;
        let mut declarations =
            read_plugin_declarations(&self.config_root).map_err(package_configuration_error)?;
        let digest = plugins_file_sha256(&self.config_root).map_err(package_configuration_error)?;
        declarations.remove(plugin_id);
        let prepared = declarations.to_lua().into_bytes();
        let trash =
            self.layout
                .root()
                .join(format!(".trash-{}-{}", plugin_id.as_str(), Uuid::new_v4()));
        let intent = RemovalIntent {
            plugin_id: plugin_id.clone(),
            operation_id: Uuid::new_v4().to_string(),
            plugins_lua_sha256: digest,
            prepared_plugins_lua: prepared,
            trash_path: trash,
            phase: RemovalPhase::Prepared,
            correlation_id: correlation_id.to_owned(),
        };
        self.store.prepare_removal(&intent).await?;
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_removal_prepared",
            correlation_id,
            json!({
                "package_phase": "removal",
                "removal_phase": intent.phase.as_str(),
                "plugin_id": intent.plugin_id.as_str(),
                "package_operation_id": intent.operation_id,
            }),
        );
        Ok(RemovalPreparation {
            intent,
            plugin_name: installed.plugin_name,
            _gate: gate,
        })
    }

    pub async fn finish_removal(
        &self,
        preparation: RemovalPreparation,
    ) -> Result<PackageOperationResult, PluginError> {
        let installed = self
            .store
            .get_plugin(&PluginSelector::Id(preparation.intent.plugin_id.clone()))
            .await?;
        if installed.running_instance_id.is_some() {
            return Err(PluginError::FailedPrecondition(
                "plugin process must be stopped before removal".to_owned(),
            ));
        }
        self.complete_removal(preparation.intent.clone()).await?;
        Ok(PackageOperationResult {
            plugin_id: Some(preparation.intent.plugin_id),
            plugin_name: Some(preparation.plugin_name),
            outcome: PackageOperationOutcome::Removed,
            diagnostics: Vec::new(),
            confirmation_summary: None,
            confirmation_digest: None,
        })
    }

    pub(super) async fn complete_removal(
        &self,
        mut intent: RemovalIntent,
    ) -> Result<(), PluginError> {
        let started = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_removal_started",
            &intent.correlation_id,
            json!({
                "package_phase": "removal",
                "removal_phase": intent.phase.as_str(),
                "plugin_id": intent.plugin_id.as_str(),
                "package_operation_id": intent.operation_id,
            }),
        );
        let result = async {
            if intent.phase == RemovalPhase::Prepared {
                let current_bytes = fs::read(
                    self.config_root
                        .join(crate::plugin::package::declarations::PLUGINS_FILENAME),
                )
                .map_err(|error| PluginError::io("read plugins.lua for removal recovery", error))?;
                let current: [u8; 32] = Sha256::digest(&current_bytes).into();
                if current == intent.plugins_lua_sha256 {
                    let prepared =
                        String::from_utf8(intent.prepared_plugins_lua.clone()).map_err(|_| {
                            PluginError::CorruptStore(
                                "prepared plugins.lua is not UTF-8".to_owned(),
                            )
                        })?;
                    let declarations = PluginDeclarations::parse(&prepared)
                        .map_err(package_configuration_error)?;
                    write_plugin_declarations(&self.config_root, &declarations)
                        .map_err(package_configuration_error)?;
                } else if current_bytes != intent.prepared_plugins_lua {
                    self.store
                        .discard_prepared_removal(&intent.plugin_id)
                        .await?;
                    return Err(PluginError::Aborted(
                        "plugins.lua changed before plugin removal".to_owned(),
                    ));
                }
                self.store
                    .advance_removal(
                        &intent.plugin_id,
                        RemovalPhase::Prepared,
                        RemovalPhase::DeclarationPublished,
                    )
                    .await?;
                intent.phase = RemovalPhase::DeclarationPublished;
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_removal_phase_completed",
                    &intent.correlation_id,
                    json!({
                        "package_phase": "removal",
                        "removal_phase": intent.phase.as_str(),
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                    }),
                );
            }
            if intent.phase == RemovalPhase::DeclarationPublished {
                self.layout
                    .move_plugin_to(&intent.plugin_id, &intent.trash_path)
                    .map_err(package_configuration_error)?;
                self.store
                    .advance_removal(
                        &intent.plugin_id,
                        RemovalPhase::DeclarationPublished,
                        RemovalPhase::PackageTrashed,
                    )
                    .await?;
                intent.phase = RemovalPhase::PackageTrashed;
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_removal_phase_completed",
                    &intent.correlation_id,
                    json!({
                        "package_phase": "removal",
                        "removal_phase": intent.phase.as_str(),
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                    }),
                );
            }
            if intent.phase == RemovalPhase::PackageTrashed {
                self.store.finalize_removal(&intent.plugin_id).await?;
                match fs::remove_dir_all(&intent.trash_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(PluginError::io("delete plugin removal trash", error));
                    }
                }
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_removal_phase_completed",
                    &intent.correlation_id,
                    json!({
                        "package_phase": "removal",
                        "removal_phase": "finalized",
                        "plugin_id": intent.plugin_id.as_str(),
                        "package_operation_id": intent.operation_id,
                    }),
                );
            }
            Ok(())
        }
        .await;
        match &result {
            Ok(()) => self.logger.emit(
                LogLevel::Info,
                "oll::plugin::package",
                "plugin_package_removal_succeeded",
                &intent.correlation_id,
                json!({
                    "package_phase": "removal",
                    "plugin_id": intent.plugin_id.as_str(),
                    "package_operation_id": intent.operation_id,
                    "duration_ms": started.elapsed().as_millis(),
                }),
            ),
            Err(error) => self.logger.emit(
                LogLevel::Error,
                "oll::plugin::package",
                "plugin_package_removal_failed",
                &intent.correlation_id,
                json!({
                    "package_phase": "removal",
                    "plugin_id": intent.plugin_id.as_str(),
                    "package_operation_id": intent.operation_id,
                    "error_code": error.code(),
                    "duration_ms": started.elapsed().as_millis(),
                }),
            ),
        }
        result
    }
}
