use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{InstallMode, PluginId, PluginName},
};
use serde_json::json;

use super::manager::owner::PackageTaskOwner;
use super::{
    EffectiveManifest, ExpansionPaths, GitCheckout, PackageError, PackageLayout,
    PhaseCancellationGuard, PluginDeclaration, ProcessCancellation, ProcessOutcome,
    PublisherManifest, checkout_git_remote, run_process_group,
};

mod release_install;
mod validation;

pub(super) use validation::read_mask;
use validation::{check_dependencies, validate_runtime};

#[derive(Clone, Debug)]
pub struct BuiltCandidate {
    pub plugin_id: PluginId,
    pub plugin_name: PluginName,
    pub generation: Uuid,
    pub declaration_bytes: Vec<u8>,
    pub declaration_sha256: [u8; 32],
    pub effective_manifest_bytes: Vec<u8>,
    pub selected_commit: String,
    pub install_mode: InstallMode,
    pub release_id: Option<String>,
    pub build_log_path: PathBuf,
}

pub(super) struct ResolvedCandidate {
    pub plugin_id: PluginId,
    pub plugin_name: PluginName,
    pub selected_commit: String,
    publisher_source: String,
    publisher: PublisherManifest,
    effective: EffectiveManifest,
    checkout: GitCheckout,
    staging: PathBuf,
    staging_guard: DirectoryGuard,
    pub(super) build_log: PathBuf,
}

struct ReleaseMaterialization<'a> {
    declaration: &'a PluginDeclaration,
    resolved: &'a ResolvedCandidate,
    operation_id: &'a str,
    correlation_id: &'a str,
    generation: Uuid,
    install: &'a Path,
    candidate_guard: DirectoryGuard,
}

#[derive(Clone)]
pub struct CandidateBuilder {
    config_root: PathBuf,
    layout: PackageLayout,
    shutdown: watch::Receiver<Option<tokio::time::Instant>>,
    package_tasks: Arc<PackageTaskOwner>,
    logger: Arc<NodeLogger>,
}

impl CandidateBuilder {
    pub(super) fn new(
        config_root: PathBuf,
        layout: PackageLayout,
        shutdown: watch::Receiver<Option<tokio::time::Instant>>,
        package_tasks: Arc<PackageTaskOwner>,
        logger: Arc<NodeLogger>,
    ) -> Self {
        Self {
            config_root,
            layout,
            shutdown,
            package_tasks,
            logger,
        }
    }

    pub async fn checkout(
        &self,
        declaration: &PluginDeclaration,
        staging: &Path,
        build_log: &Path,
        plugin_id: Option<&PluginId>,
        operation_id: &str,
        correlation_id: &str,
    ) -> Result<GitCheckout, PackageError> {
        let started = Instant::now();
        let (branch, revision) = match &declaration.selection {
            super::GitSelection::Default => (None, None),
            super::GitSelection::Branch(branch) => (Some(branch.as_str()), None),
            super::GitSelection::Revision(revision) => (None, Some(revision.as_str())),
        };
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_git_selection_started",
            correlation_id,
            json!({
                "package_phase": "git_selection",
                "plugin_id": plugin_id.map(PluginId::as_str),
                "package_operation_id": operation_id,
                "build_log_path": build_log.display().to_string(),
                "sanitized_remote": declaration.sanitized_remote(),
                "branch": branch,
                "revision": revision,
            }),
        );
        let mut cancellation = PhaseCancellationGuard::new(
            Arc::clone(&self.logger),
            "plugin_package_git_selection_failed",
            correlation_id,
            json!({
                "package_phase": "git_selection",
                "plugin_id": plugin_id.map(PluginId::as_str),
                "package_operation_id": operation_id,
                "build_log_path": build_log.display().to_string(),
            }),
        );
        let result = checkout_git_remote(
            &declaration.remote,
            &declaration.selection,
            &staging.join("source"),
            build_log,
            ProcessCancellation::from_receiver(self.shutdown.clone())
                .with_owner(Arc::clone(&self.package_tasks))
                .with_cleanup_after_drop([staging.to_owned()]),
        )
        .await;
        cancellation.complete();
        match &result {
            Ok(checkout) => self.logger.emit(
                LogLevel::Info,
                "oll::plugin::package",
                "plugin_package_git_selection_succeeded",
                correlation_id,
                json!({
                    "package_phase": "git_selection",
                    "plugin_id": plugin_id.map(PluginId::as_str),
                    "package_operation_id": operation_id,
                    "build_log_path": build_log.display().to_string(),
                    "selected_commit": checkout.commit,
                    "duration_ms": started.elapsed().as_millis(),
                }),
            ),
            Err(error) => self.logger.emit(
                LogLevel::Error,
                "oll::plugin::package",
                "plugin_package_git_selection_failed",
                correlation_id,
                json!({
                    "package_phase": "git_selection",
                    "plugin_id": plugin_id.map(PluginId::as_str),
                    "package_operation_id": operation_id,
                    "build_log_path": build_log.display().to_string(),
                    "error_code": error.code(),
                    "duration_ms": started.elapsed().as_millis(),
                }),
            ),
        }
        result
    }

    pub(super) async fn resolve(
        &self,
        expected_plugin_id: &PluginId,
        declaration: &PluginDeclaration,
        operation_id: &str,
        existing_checkout: Option<GitCheckout>,
        correlation_id: &str,
    ) -> Result<ResolvedCandidate, PackageError> {
        let staging = self
            .layout
            .operation_staging(expected_plugin_id, operation_id)?;
        let staging_guard = DirectoryGuard::new(staging.clone());
        let build_log = self.layout.build_log(expected_plugin_id, operation_id)?;
        let checkout = match existing_checkout {
            Some(checkout) => checkout,
            None => {
                self.checkout(
                    declaration,
                    &staging,
                    &build_log,
                    Some(expected_plugin_id),
                    operation_id,
                    correlation_id,
                )
                .await?
            }
        };
        let publisher_source =
            fs::read_to_string(checkout.source_root.join("oll.toml")).map_err(|error| {
                PackageError::io(
                    "manifest_missing",
                    "manifest",
                    "selected repository has no readable oll.toml",
                    error,
                )
                .with_build_log(build_log.clone())
            })?;
        let publisher = PublisherManifest::parse(&publisher_source)?;
        let actual_id: PluginId = publisher.plugin.id.parse().map_err(|message| {
            PackageError::manifest(format!("publisher PluginId is invalid: {message}"))
        })?;
        if &actual_id != expected_plugin_id {
            return Err(PackageError::manifest(
                "publisher PluginId changed from the installation declaration",
            ));
        }
        let mask = read_mask(&self.config_root, expected_plugin_id)?;
        let effective = EffectiveManifest::merge(publisher.clone(), mask)?;
        let plugin_name = effective.plugin_name()?;
        if declaration.mode == super::DeclarationMode::Source {
            check_dependencies(&effective, &checkout.source_root)?;
        }

        Ok(ResolvedCandidate {
            plugin_id: actual_id,
            plugin_name,
            selected_commit: checkout.commit.clone(),
            publisher_source,
            publisher,
            effective,
            checkout,
            staging,
            staging_guard,
            build_log,
        })
    }

    pub(super) async fn build_resolved(
        &self,
        declaration: &PluginDeclaration,
        mut resolved: ResolvedCandidate,
        operation_id: &str,
        correlation_id: &str,
    ) -> Result<BuiltCandidate, PackageError> {
        let generation = Uuid::new_v4();

        let install = self.layout.candidate(&resolved.plugin_id, generation)?;
        let mut candidate_guard = DirectoryGuard::new(install.clone());
        let mask_dir = self.config_root.join("plugin-masks");
        let paths = ExpansionPaths {
            source: Some(&resolved.checkout.source_root),
            staging: Some(&resolved.staging),
            install: &install,
            mask_dir: &mask_dir,
        };
        match declaration.mode {
            super::DeclarationMode::Source => {
                for argv in resolved.effective.expanded_source_steps(&paths)? {
                    match run_process_group(
                        &argv,
                        &resolved.checkout.source_root,
                        &resolved.build_log,
                        ProcessCancellation::from_receiver(self.shutdown.clone())
                            .with_owner(Arc::clone(&self.package_tasks))
                            .with_cleanup_after_drop([resolved.staging.clone(), install.clone()]),
                    )
                    .await?
                    {
                        ProcessOutcome::Exited(status) if status.success() => {}
                        ProcessOutcome::Exited(_) => {
                            return Err(PackageError::new(
                                "recipe_step_failed",
                                "recipe",
                                "source recipe step exited unsuccessfully",
                            )
                            .with_build_log(resolved.build_log));
                        }
                        ProcessOutcome::Cancelled => {
                            return Err(PackageError::new(
                                "recipe_step_failed",
                                "recipe",
                                "source recipe was cancelled",
                            )
                            .with_build_log(resolved.build_log));
                        }
                    }
                }
                fs::write(
                    install.join("oll.toml"),
                    resolved.publisher_source.as_bytes(),
                )
                .map_err(|error| {
                    PackageError::io(
                        "recipe_output_missing",
                        "recipe",
                        "cannot retain publisher manifest in candidate",
                        error,
                    )
                })?;
            }
            super::DeclarationMode::Release => {
                candidate_guard = self
                    .materialize_release(ReleaseMaterialization {
                        declaration,
                        resolved: &resolved,
                        operation_id,
                        correlation_id,
                        generation,
                        install: &install,
                        candidate_guard,
                    })
                    .await?;
            }
        }
        let verification_started = Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_candidate_verification_started",
            correlation_id,
            json!({
                "package_phase": "candidate_verification",
                "verification_scope": "built_candidate",
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "build_log_path": resolved.build_log.display().to_string(),
            }),
        );
        if let Err(error) = validate_runtime(&resolved.effective, &install, &mask_dir) {
            self.logger.emit(
                LogLevel::Error,
                "oll::plugin::package",
                "plugin_package_candidate_verification_failed",
                correlation_id,
                json!({
                    "package_phase": "candidate_verification",
                    "verification_scope": "built_candidate",
                    "plugin_id": resolved.plugin_id.as_str(),
                    "plugin_name": resolved.plugin_name.as_str(),
                    "package_operation_id": operation_id,
                    "install_generation": generation.to_string(),
                    "build_log_path": resolved.build_log.display().to_string(),
                    "error_code": error.code(),
                    "duration_ms": verification_started.elapsed().as_millis(),
                }),
            );
            return Err(error);
        }
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_candidate_verification_succeeded",
            correlation_id,
            json!({
                "package_phase": "candidate_verification",
                "verification_scope": "built_candidate",
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "build_log_path": resolved.build_log.display().to_string(),
                "duration_ms": verification_started.elapsed().as_millis(),
            }),
        );
        let effective_manifest_bytes = serde_json::to_vec(&resolved.effective).map_err(|_| {
            PackageError::new(
                "manifest_invalid",
                "manifest",
                "cannot encode effective plugin manifest",
            )
        })?;
        let declaration_bytes = serde_json::to_vec(declaration).map_err(|_| {
            PackageError::new(
                "plugin_config_schema",
                "declaration",
                "cannot encode normalized plugin declaration",
            )
        })?;
        let declaration_sha256 = declaration.normalized_sha256();
        candidate_guard.disarm();
        resolved.staging_guard.remove_now();
        Ok(BuiltCandidate {
            plugin_id: resolved.plugin_id,
            plugin_name: resolved.plugin_name,
            generation,
            declaration_bytes,
            declaration_sha256,
            effective_manifest_bytes,
            selected_commit: resolved.selected_commit,
            install_mode: match declaration.mode {
                super::DeclarationMode::Source => InstallMode::Source,
                super::DeclarationMode::Release => InstallMode::Release,
            },
            release_id: declaration.release.clone(),
            build_log_path: resolved.build_log,
        })
    }
}

struct DirectoryGuard {
    path: PathBuf,
    armed: bool,
}

impl DirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn remove_now(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
            self.armed = false;
        }
    }
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
