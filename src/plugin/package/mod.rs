mod archive;
mod build;
mod declarations;
mod git;
mod manager;
mod manifest;
mod process;
mod release;
mod storage;

pub use archive::{extract_release_archive, stage_release_download};
pub use build::{BuiltCandidate, CandidateBuilder};
pub use declarations::{
    DeclarationMode, GitSelection, PluginDeclaration, PluginDeclarations, mask_path,
    plugins_file_sha256, read_plugin_declarations, write_plugin_declarations,
};
pub use git::{GitCheckout, checkout_git_remote};
pub use manager::{
    InstallRemoteRequest, OverwriteAuthorization, PackageDiagnostic, PackageManager,
    PackageOperationOutcome, PackageOperationResult, PluginPackageGates, RemovalPreparation,
};
pub use manifest::{
    EffectiveManifest, ExpansionPaths, ManifestMask, PublisherManifest, SourceCheckout,
    executable_exists, validate_local_package_config,
};
pub(super) use process::{ProcessCancellation, ProcessOutcome, run_process_group};
pub use release::{ArchiveKind, ReleaseArtifact, ReleaseIndex, ReleaseListing, local_target};
pub use storage::PackageLayout;

use std::{fmt, io, path::PathBuf, sync::Arc, time::Instant};

use serde_json::Value;

use crate::node::logging::{LogLevel, NodeLogger};

struct PhaseCancellationGuard {
    logger: Arc<NodeLogger>,
    failure_event: &'static str,
    correlation_id: String,
    fields: Value,
    started: Instant,
    armed: bool,
}

impl PhaseCancellationGuard {
    fn new(
        logger: Arc<NodeLogger>,
        failure_event: &'static str,
        correlation_id: &str,
        fields: Value,
    ) -> Self {
        Self {
            logger,
            failure_event,
            correlation_id: correlation_id.to_owned(),
            fields,
            started: Instant::now(),
            armed: true,
        }
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for PhaseCancellationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Value::Object(fields) = &mut self.fields else {
            return;
        };
        fields.insert("outcome".to_owned(), Value::String("cancelled".to_owned()));
        fields.insert(
            "error_code".to_owned(),
            Value::String("operation_cancelled".to_owned()),
        );
        fields.insert(
            "duration_ms".to_owned(),
            Value::from(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)),
        );
        self.logger.emit(
            LogLevel::Error,
            "oll::plugin::package",
            self.failure_event,
            &self.correlation_id,
            self.fields.clone(),
        );
    }
}

#[derive(Debug)]
pub struct PackageError {
    code: &'static str,
    phase: &'static str,
    message: String,
    hint: Option<String>,
    build_log_path: Option<PathBuf>,
    source: Option<io::Error>,
}

impl PackageError {
    pub fn new(code: &'static str, phase: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            phase,
            message: message.into(),
            hint: None,
            build_log_path: None,
            source: None,
        }
    }

    pub fn manifest(message: impl Into<String>) -> Self {
        Self::new("manifest_invalid", "manifest", message)
    }

    pub fn mask(message: impl Into<String>) -> Self {
        Self::new("mask_invalid", "mask", message)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new("protocol_incompatible", "manifest", message)
    }

    pub fn entrypoint(message: impl Into<String>) -> Self {
        Self::new("entrypoint_invalid", "validation", message)
    }

    pub fn io(
        code: &'static str,
        phase: &'static str,
        message: impl Into<String>,
        source: io::Error,
    ) -> Self {
        let mut error = Self::new(code, phase, message);
        error.source = Some(source);
        error
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_build_log(mut self, path: PathBuf) -> Self {
        self.build_log_path = Some(path);
        self
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn phase(&self) -> &'static str {
        self.phase
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn build_log_path(&self) -> Option<&std::path::Path> {
        self.build_log_path.as_deref()
    }

    fn as_mask(error: Self) -> Self {
        Self::mask(error.message)
    }
}

impl fmt::Display for PackageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PackageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
