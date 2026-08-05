use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    sync::{Mutex, OwnedMutexGuard, watch},
    time::Instant,
};
use uuid::Uuid;

use crate::node::logging::NodeLogger;
use crate::plugin::{
    InstalledPlugin, PackagePublishIntent, PluginError, PluginId, PluginName, PluginSelector,
    PluginStore, RemovalIntent,
};

use super::build::ResolvedCandidate;
use super::{
    BuiltCandidate, CandidateBuilder, DeclarationMode, EffectiveManifest, GitCheckout,
    GitSelection, PackageError, PackageLayout, PhaseCancellationGuard, PluginDeclaration,
    PluginDeclarations, PublisherManifest, ReleaseIndex, ReleaseListing, read_plugin_declarations,
    write_plugin_declarations,
};

mod exact_reconcile;
mod operations;
pub(super) mod owner;
mod publication;
mod reconcile;
mod recovery;
mod removal;

use owner::{DurablePublishContext, PackageTaskOwner};
#[cfg(test)]
use owner::{PublishPause, PublishTestHook};

#[derive(Clone, Debug)]
pub struct InstallRemoteRequest {
    pub declaration: PluginDeclaration,
    pub overwrite: Option<OverwriteAuthorization>,
}

#[derive(Clone, Debug)]
pub struct OverwriteAuthorization {
    pub plugin_id: PluginId,
    pub expected_declaration_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackageOperationOutcome {
    Installed,
    Updated,
    Removed,
    AlreadySatisfied,
    ConfirmationRequired,
    Failed,
}

impl PackageOperationOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::Updated => "updated",
            Self::Removed => "removed",
            Self::AlreadySatisfied => "already_satisfied",
            Self::ConfirmationRequired => "confirmation_required",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug)]
pub struct PackageDiagnostic {
    pub code: String,
    pub phase: String,
    pub message: String,
    pub hint: Option<String>,
    pub build_log_path: Option<PathBuf>,
    pub sanitized_remote: Option<String>,
    pub branch: Option<String>,
    pub revision: Option<String>,
    pub release_id: Option<String>,
    pub target: Option<String>,
}

impl PackageDiagnostic {
    fn from_package(error: PackageError) -> Self {
        Self {
            code: error.code().to_owned(),
            phase: error.phase().to_owned(),
            message: error.message().to_owned(),
            hint: error.hint().map(str::to_owned),
            build_log_path: error.build_log_path().map(Path::to_owned),
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }
    }

    fn with_declaration(mut self, declaration: &PluginDeclaration) -> Self {
        self.sanitized_remote = Some(declaration.sanitized_remote());
        match &declaration.selection {
            GitSelection::Default => {}
            GitSelection::Branch(value) => self.branch = Some(value.clone()),
            GitSelection::Revision(value) => self.revision = Some(value.clone()),
        }
        self.release_id = declaration.release.clone();
        if declaration.mode == DeclarationMode::Release {
            self.target = super::local_target().ok().map(str::to_owned);
        }
        self
    }

    fn store(_error: PluginError) -> Self {
        Self {
            code: "install_publish_failed".to_owned(),
            phase: "store".to_owned(),
            message: "plugin package state could not be committed".to_owned(),
            hint: None,
            build_log_path: None,
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }
    }

    fn shutting_down() -> Self {
        Self {
            code: "operation_cancelled".to_owned(),
            phase: "admission".to_owned(),
            message: "plugin package manager is shutting down".to_owned(),
            hint: None,
            build_log_path: None,
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }
    }

    fn name_conflict(name: &PluginName, declaration: &PluginDeclaration) -> Self {
        Self {
            code: "plugin_name_conflict".to_owned(),
            phase: "validation".to_owned(),
            message: format!("effective plugin name {name} is not unique"),
            hint: None,
            build_log_path: None,
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }
        .with_declaration(declaration)
    }

    fn blocked_by_removal(
        name: &PluginName,
        owner: &PluginId,
        declaration: &PluginDeclaration,
    ) -> Self {
        Self {
            code: "plugin_name_conflict".to_owned(),
            phase: "removal".to_owned(),
            message: format!(
                "effective plugin name {name} remains bound because removal of {owner} failed"
            ),
            hint: None,
            build_log_path: None,
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }
        .with_declaration(declaration)
    }
}

#[derive(Debug)]
pub struct PackageOperationResult {
    pub plugin_id: Option<PluginId>,
    pub plugin_name: Option<PluginName>,
    pub outcome: PackageOperationOutcome,
    pub diagnostics: Vec<PackageDiagnostic>,
    pub confirmation_summary: Option<String>,
    pub confirmation_digest: Option<[u8; 32]>,
}

pub struct RemovalPreparation {
    intent: RemovalIntent,
    plugin_name: PluginName,
    _gate: OwnedMutexGuard<()>,
}

impl RemovalPreparation {
    pub fn plugin_id(&self) -> &PluginId {
        &self.intent.plugin_id
    }

    pub fn plugin_name(&self) -> &PluginName {
        &self.plugin_name
    }
}

impl PackageOperationResult {
    fn satisfied(plugin_id: PluginId, plugin_name: PluginName) -> Self {
        Self {
            plugin_id: Some(plugin_id),
            plugin_name: Some(plugin_name),
            outcome: PackageOperationOutcome::AlreadySatisfied,
            diagnostics: Vec::new(),
            confirmation_summary: None,
            confirmation_digest: None,
        }
    }

    fn failed(
        plugin_id: Option<PluginId>,
        plugin_name: Option<PluginName>,
        diagnostic: PackageDiagnostic,
    ) -> Self {
        Self {
            plugin_id,
            plugin_name,
            outcome: PackageOperationOutcome::Failed,
            diagnostics: vec![diagnostic],
            confirmation_summary: None,
            confirmation_digest: None,
        }
    }
}

#[derive(Default)]
pub struct PluginPackageGates {
    entries: Mutex<HashMap<PluginId, Arc<Mutex<()>>>>,
}

impl PluginPackageGates {
    pub async fn lock(&self, plugin_id: &PluginId) -> OwnedMutexGuard<()> {
        let gate = self
            .entries
            .lock()
            .await
            .entry(plugin_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        gate.lock_owned().await
    }
}

#[derive(Clone)]
pub struct PackageManager {
    config_root: PathBuf,
    layout: PackageLayout,
    store: PluginStore,
    builder: CandidateBuilder,
    declarations: Arc<Mutex<()>>,
    gates: Arc<PluginPackageGates>,
    package_tasks: Arc<PackageTaskOwner>,
    logger: Arc<NodeLogger>,
    shutdown: watch::Receiver<Option<tokio::time::Instant>>,
    #[cfg(test)]
    publish_test_hook: Arc<Mutex<Option<Arc<PublishTestHook>>>>,
}

impl PackageManager {
    pub fn new(
        config_root: PathBuf,
        layout: PackageLayout,
        store: PluginStore,
        shutdown: watch::Receiver<Option<tokio::time::Instant>>,
        logger: Arc<NodeLogger>,
    ) -> Self {
        let package_tasks = PackageTaskOwner::new(Arc::clone(&logger));
        let builder = CandidateBuilder::new(
            config_root.clone(),
            layout.clone(),
            shutdown.clone(),
            Arc::clone(&package_tasks),
            Arc::clone(&logger),
        );
        Self {
            config_root,
            layout,
            store,
            builder,
            declarations: Arc::new(Mutex::new(())),
            gates: Arc::new(PluginPackageGates::default()),
            package_tasks,
            logger,
            shutdown,
            #[cfg(test)]
            publish_test_hook: Arc::new(Mutex::new(None)),
        }
    }

    pub fn gates(&self) -> Arc<PluginPackageGates> {
        Arc::clone(&self.gates)
    }

    pub async fn shutdown(&self, deadline: Instant) -> Result<(), PluginError> {
        self.package_tasks.shutdown(deadline).await
    }

    #[cfg(test)]
    async fn set_publish_test_hook(&self, hook: Arc<PublishTestHook>) {
        *self.publish_test_hook.lock().await = Some(hook);
    }

    #[cfg(test)]
    async fn pause_publish_test_hook(&self, pause: PublishPause) {
        let hook = self.publish_test_hook.lock().await.clone();
        if let Some(hook) = hook.filter(|hook| {
            hook.pause == pause
                || (hook.pause == PublishPause::PanicAfterIntent
                    && pause == PublishPause::AfterIntent)
        }) {
            hook.reached();
            if hook.pause == PublishPause::PanicAfterIntent {
                panic!("injected durable package publication panic");
            }
            hook.wait_for_release().await;
        }
    }

    async fn require_package_admission(&self) -> Result<(), PluginError> {
        if self.shutdown.borrow().is_some() || !self.package_tasks.is_accepting().await {
            Err(PluginError::FailedPrecondition(
                "plugin package manager is shutting down".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
}

enum Prepared {
    Candidate(Box<PreparedCandidate>),
    Result(PackageOperationResult),
}

enum Resolved {
    Candidate(Box<PreparedResolution>),
    Result(PackageOperationResult),
}

enum InstalledResolution {
    Lookup,
    Snapshot(Box<Option<InstalledPlugin>>),
}

struct PreparedResolution {
    resolved: ResolvedCandidate,
    expected_current: Option<Uuid>,
    operation_id: String,
    correlation_id: String,
    _gate: OwnedMutexGuard<()>,
    _checkout_guard: Option<StagingGuard>,
    layout: PackageLayout,
    declaration: PluginDeclaration,
}

struct PreparedCandidate {
    built: BuiltCandidate,
    expected_current: Option<Uuid>,
    operation_id: String,
    correlation_id: String,
    _gate: OwnedMutexGuard<()>,
    _checkout_guard: Option<StagingGuard>,
    layout: PackageLayout,
    recovery_owned: bool,
    declaration: PluginDeclaration,
}

impl Drop for PreparedCandidate {
    fn drop(&mut self) {
        if !self.recovery_owned {
            self.layout
                .remove_candidate(&self.built.plugin_id, self.built.generation);
        }
    }
}

struct StagingGuard(PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn require_correlation(correlation_id: &str) -> Result<(), PluginError> {
    if correlation_id.is_empty() {
        Err(PluginError::InvalidArgument(
            "plugin package correlation ID must not be empty".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn package_configuration_error(error: PackageError) -> PluginError {
    PluginError::InvalidArgument(error.to_string())
}

#[cfg(test)]
mod tests;
