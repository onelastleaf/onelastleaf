use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::Arc,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::json;
use uuid::Uuid;

use crate::node::logging::LogLevel;

use super::*;

impl PackageManager {
    pub(super) async fn install_declaration_set(
        &self,
        declarations: &PluginDeclarations,
        force_update: bool,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let mut immediate = Vec::new();
        let mut resolving = FuturesUnordered::new();
        for (plugin_id, declaration) in declarations.iter() {
            let manager = self.clone();
            let plugin_id = plugin_id.clone();
            let declaration = declaration.clone();
            resolving.push(async move {
                manager
                    .resolve_candidate(
                        plugin_id,
                        declaration,
                        force_update,
                        None,
                        InstalledResolution::Lookup,
                        correlation_id,
                    )
                    .await
            });
        }
        let mut candidates = Vec::new();
        while let Some(resolved) = resolving.next().await {
            match resolved {
                Resolved::Candidate(candidate) => candidates.push(*candidate),
                Resolved::Result(result) => immediate.push(result),
            }
        }
        immediate.extend(self.publish_resolved_set(candidates).await?);
        immediate.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        Ok(immediate)
    }

    pub(super) async fn publish_resolved_set(
        &self,
        candidates: Vec<PreparedResolution>,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        let mut results = Vec::new();
        let conflicts = self.conflicting_resolutions(&candidates).await?;
        let mut publishing = FuturesUnordered::new();
        for candidate in candidates {
            if conflicts.contains(&candidate.resolved.plugin_id) {
                results.push(PackageOperationResult::failed(
                    Some(candidate.resolved.plugin_id.clone()),
                    Some(candidate.resolved.plugin_name.clone()),
                    PackageDiagnostic::name_conflict(
                        &candidate.resolved.plugin_name,
                        &candidate.declaration,
                    ),
                ));
            } else {
                let manager = self.clone();
                publishing.push(async move {
                    let prepared = manager.build_candidate(candidate).await;
                    manager.finish_single(prepared).await
                });
            }
        }
        while let Some(result) = publishing.next().await {
            results.push(result);
        }
        results.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        Ok(results)
    }

    pub(super) async fn prepare_candidate(
        &self,
        plugin_id: PluginId,
        declaration: PluginDeclaration,
        force_update: bool,
        existing_checkout: Option<(String, GitCheckout)>,
        correlation_id: &str,
    ) -> Prepared {
        match self
            .resolve_candidate(
                plugin_id,
                declaration,
                force_update,
                existing_checkout,
                InstalledResolution::Lookup,
                correlation_id,
            )
            .await
        {
            Resolved::Candidate(candidate) => self.build_candidate(*candidate).await,
            Resolved::Result(result) => Prepared::Result(result),
        }
    }

    pub(super) async fn resolve_candidate(
        &self,
        plugin_id: PluginId,
        declaration: PluginDeclaration,
        force_update: bool,
        existing_checkout: Option<(String, GitCheckout)>,
        installed_resolution: InstalledResolution,
        correlation_id: &str,
    ) -> Resolved {
        let gate = self.gates.lock(&plugin_id).await;
        if self.require_package_admission().await.is_err() {
            return Resolved::Result(PackageOperationResult::failed(
                Some(plugin_id),
                None,
                PackageDiagnostic::shutting_down(),
            ));
        }
        let installed = match installed_resolution {
            InstalledResolution::Snapshot(installed) => *installed,
            InstalledResolution::Lookup => match self
                .store
                .get_plugin(&PluginSelector::Id(plugin_id.clone()))
                .await
            {
                Ok(plugin) => Some(plugin),
                Err(PluginError::NotFound(_)) => None,
                Err(error) => {
                    return Resolved::Result(PackageOperationResult::failed(
                        Some(plugin_id),
                        None,
                        PackageDiagnostic::store(error),
                    ));
                }
            },
        };
        let inputs_changed = match installed.as_ref() {
            Some(installed) => match self.local_inputs_changed(installed, &declaration) {
                Ok(changed) => changed,
                Err(error) => {
                    return Resolved::Result(PackageOperationResult::failed(
                        Some(plugin_id),
                        Some(installed.plugin_name.clone()),
                        PackageDiagnostic::from_package(error).with_declaration(&declaration),
                    ));
                }
            },
            None => true,
        };

        let (mut operation_id, mut checkout) = match existing_checkout {
            Some((operation_id, checkout)) => (Some(operation_id), Some(checkout)),
            None => (None, None),
        };
        let mut checkout_guard = None;
        if checkout.is_none()
            && force_update
            && !matches!(declaration.selection, GitSelection::Revision(_))
        {
            let checkout_operation_id = Uuid::new_v4().to_string();
            let staging = match self.layout.discovery_staging(&checkout_operation_id) {
                Ok(path) => path,
                Err(error) => {
                    return Resolved::Result(PackageOperationResult::failed(
                        Some(plugin_id),
                        installed.as_ref().map(|value| value.plugin_name.clone()),
                        PackageDiagnostic::from_package(error).with_declaration(&declaration),
                    ));
                }
            };
            let staging_guard = StagingGuard(staging.clone());
            let build_log = match self.layout.build_log(&plugin_id, &checkout_operation_id) {
                Ok(path) => path,
                Err(error) => {
                    return Resolved::Result(PackageOperationResult::failed(
                        Some(plugin_id),
                        installed.as_ref().map(|value| value.plugin_name.clone()),
                        PackageDiagnostic::from_package(error).with_declaration(&declaration),
                    ));
                }
            };
            match self
                .builder
                .checkout(
                    &declaration,
                    &staging,
                    &build_log,
                    Some(&plugin_id),
                    &checkout_operation_id,
                    correlation_id,
                )
                .await
            {
                Ok(selected) => {
                    if installed.as_ref().is_some_and(|current| {
                        current.selected_commit.as_deref() == Some(selected.commit.as_str())
                    }) && !inputs_changed
                    {
                        return Resolved::Result(PackageOperationResult::satisfied(
                            plugin_id,
                            installed.expect("checked installed plugin").plugin_name,
                        ));
                    }
                    checkout = Some(selected);
                    checkout_guard = Some(staging_guard);
                    operation_id = Some(checkout_operation_id);
                }
                Err(error) => {
                    return Resolved::Result(PackageOperationResult::failed(
                        Some(plugin_id),
                        installed.as_ref().map(|value| value.plugin_name.clone()),
                        PackageDiagnostic::from_package(error).with_declaration(&declaration),
                    ));
                }
            }
        } else if checkout.is_none() && installed.is_some() && !inputs_changed {
            return Resolved::Result(PackageOperationResult::satisfied(
                plugin_id,
                installed.expect("checked installed plugin").plugin_name,
            ));
        }

        let operation_id = operation_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let verification_started = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_candidate_verification_started",
            correlation_id,
            json!({
                "package_phase": "candidate_verification",
                "verification_scope": "manifest_and_inputs",
                "plugin_id": plugin_id.as_str(),
                "package_operation_id": operation_id,
            }),
        );
        let mut verification_cancellation = PhaseCancellationGuard::new(
            Arc::clone(&self.logger),
            "plugin_package_candidate_verification_failed",
            correlation_id,
            json!({
                "package_phase": "candidate_verification",
                "verification_scope": "manifest_and_inputs",
                "plugin_id": plugin_id.as_str(),
                "package_operation_id": operation_id,
            }),
        );
        let resolved = self
            .builder
            .resolve(
                &plugin_id,
                &declaration,
                &operation_id,
                checkout,
                correlation_id,
            )
            .await;
        verification_cancellation.complete();
        match resolved {
            Ok(resolved) => {
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_candidate_verification_succeeded",
                    correlation_id,
                    json!({
                        "package_phase": "candidate_verification",
                        "verification_scope": "manifest_and_inputs",
                        "plugin_id": plugin_id.as_str(),
                        "plugin_name": resolved.plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "build_log_path": resolved.build_log.display().to_string(),
                        "duration_ms": verification_started.elapsed().as_millis(),
                    }),
                );
                Resolved::Candidate(Box::new(PreparedResolution {
                    resolved,
                    expected_current: installed.map(|value| value.current_generation),
                    operation_id,
                    correlation_id: correlation_id.to_owned(),
                    _gate: gate,
                    _checkout_guard: checkout_guard,
                    layout: self.layout.clone(),
                    declaration,
                }))
            }
            Err(error) => {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::plugin::package",
                    "plugin_package_candidate_verification_failed",
                    correlation_id,
                    json!({
                        "package_phase": "candidate_verification",
                        "verification_scope": "manifest_and_inputs",
                        "plugin_id": plugin_id.as_str(),
                        "package_operation_id": operation_id,
                        "build_log_path": error.build_log_path().map(|path| path.display().to_string()),
                        "error_code": error.code(),
                        "duration_ms": verification_started.elapsed().as_millis(),
                    }),
                );
                Resolved::Result(PackageOperationResult::failed(
                    Some(plugin_id),
                    installed.map(|value| value.plugin_name),
                    PackageDiagnostic::from_package(error).with_declaration(&declaration),
                ))
            }
        }
    }

    async fn build_candidate(&self, candidate: PreparedResolution) -> Prepared {
        let PreparedResolution {
            resolved,
            expected_current,
            operation_id,
            correlation_id,
            _gate,
            _checkout_guard,
            layout,
            declaration,
        } = candidate;
        let plugin_id = resolved.plugin_id.clone();
        let plugin_name = resolved.plugin_name.clone();
        let build_log_path = resolved.build_log.clone();
        if self.require_package_admission().await.is_err() {
            return Prepared::Result(PackageOperationResult::failed(
                Some(plugin_id),
                Some(plugin_name),
                PackageDiagnostic::shutting_down(),
            ));
        }
        let build_started = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_candidate_build_started",
            &correlation_id,
            json!({
                "package_phase": "candidate_build",
                "plugin_id": plugin_id.as_str(),
                "plugin_name": plugin_name.as_str(),
                "package_operation_id": operation_id,
                "build_log_path": build_log_path.display().to_string(),
            }),
        );
        let mut build_cancellation = PhaseCancellationGuard::new(
            Arc::clone(&self.logger),
            "plugin_package_candidate_build_failed",
            &correlation_id,
            json!({
                "package_phase": "candidate_build",
                "plugin_id": plugin_id.as_str(),
                "plugin_name": plugin_name.as_str(),
                "package_operation_id": operation_id,
                "build_log_path": build_log_path.display().to_string(),
            }),
        );
        let built = self
            .builder
            .build_resolved(&declaration, resolved, &operation_id, &correlation_id)
            .await;
        build_cancellation.complete();
        match built {
            Ok(built) => {
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::package",
                    "plugin_package_candidate_build_succeeded",
                    &correlation_id,
                    json!({
                        "package_phase": "candidate_build",
                        "plugin_id": plugin_id.as_str(),
                        "plugin_name": plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "install_generation": built.generation.to_string(),
                        "build_log_path": build_log_path.display().to_string(),
                        "duration_ms": build_started.elapsed().as_millis(),
                    }),
                );
                Prepared::Candidate(Box::new(PreparedCandidate {
                    built,
                    expected_current,
                    operation_id,
                    correlation_id,
                    _gate,
                    _checkout_guard,
                    layout,
                    recovery_owned: false,
                    declaration,
                }))
            }
            Err(error) => {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::plugin::package",
                    "plugin_package_candidate_build_failed",
                    &correlation_id,
                    json!({
                        "package_phase": "candidate_build",
                        "plugin_id": plugin_id.as_str(),
                        "plugin_name": plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "build_log_path": build_log_path.display().to_string(),
                        "error_code": error.code(),
                        "duration_ms": build_started.elapsed().as_millis(),
                    }),
                );
                Prepared::Result(PackageOperationResult::failed(
                    Some(plugin_id),
                    Some(plugin_name),
                    PackageDiagnostic::from_package(error).with_declaration(&declaration),
                ))
            }
        }
    }

    pub(super) async fn finish_single(&self, prepared: Prepared) -> PackageOperationResult {
        match prepared {
            Prepared::Result(result) => result,
            Prepared::Candidate(candidate) => self.publish_candidate(*candidate).await,
        }
    }

    async fn conflicting_resolutions(
        &self,
        candidates: &[PreparedResolution],
    ) -> Result<BTreeSet<PluginId>, PluginError> {
        let mut desired_names: BTreeMap<PluginName, Vec<PluginId>> = BTreeMap::new();
        for candidate in candidates {
            desired_names
                .entry(candidate.resolved.plugin_name.clone())
                .or_default()
                .push(candidate.resolved.plugin_id.clone());
        }
        let mut conflicts = desired_names
            .into_values()
            .filter(|plugin_ids| plugin_ids.len() > 1)
            .flatten()
            .collect::<BTreeSet<_>>();
        let mut installed_by_name = BTreeMap::new();
        for installed in self.store.list_plugins().await? {
            installed_by_name.insert(installed.plugin_name, installed.plugin_id);
        }
        for candidate in candidates {
            if installed_by_name
                .get(&candidate.resolved.plugin_name)
                .is_some_and(|owner| owner != &candidate.resolved.plugin_id)
            {
                conflicts.insert(candidate.resolved.plugin_id.clone());
            }
        }
        Ok(conflicts)
    }

    fn local_inputs_changed(
        &self,
        installed: &crate::plugin::InstalledPlugin,
        declaration: &PluginDeclaration,
    ) -> Result<bool, PackageError> {
        if declaration.normalized_sha256() != installed.declaration_sha256 {
            return Ok(true);
        }
        let source = fs::read_to_string(
            self.layout
                .generation(&installed.plugin_id, installed.current_generation)
                .join("oll.toml"),
        )
        .map_err(|error| {
            PackageError::io(
                "manifest_missing",
                "manifest",
                "cannot read the published plugin manifest",
                error,
            )
        })?;
        let publisher = PublisherManifest::parse(&source)?;
        let mask =
            crate::plugin::package::build::read_mask(&self.config_root, &installed.plugin_id)?;
        let effective = super::EffectiveManifest::merge(publisher, mask)?;
        let encoded = serde_json::to_vec(&effective)
            .map_err(|_| PackageError::manifest("cannot encode the effective plugin manifest"))?;
        Ok(encoded != installed.effective_manifest)
    }
}
