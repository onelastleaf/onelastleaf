use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use serde_json::json;

use crate::node::logging::LogLevel;
use crate::plugin::package::{
    ArchiveKind, DeclarationLocation, ReleaseIndex, extract_release_archive, local_target,
    stage_local_release_package, stage_release_download,
};

use super::validation::validate_platform_archive;
use super::*;

impl CandidateBuilder {
    pub(super) async fn materialize_release(
        &self,
        request: ReleaseMaterialization<'_>,
    ) -> Result<(DirectoryGuard, String), PackageError> {
        let ReleaseMaterialization {
            declaration,
            resolved,
            operation_id,
            correlation_id,
            generation,
            install,
            candidate_guard,
        } = request;
        let release_source = fs::read_to_string(resolved.source_root().join("oll-release.json"))
            .map_err(|error| {
                PackageError::io(
                    "release_index_missing",
                    "release_index",
                    "selected package location has no readable oll-release.json",
                    error,
                )
            })?;
        let index = ReleaseIndex::parse(&release_source, &resolved.publisher)?;
        let release_id = index
            .resolve_release_id(declaration.release.as_deref())?
            .to_owned();
        let package = index.select(&release_id, local_target()?)?;
        let archive_kind = package.archive;
        validate_platform_archive(archive_kind)?;
        let archive = resolved.staging.join(match archive_kind {
            ArchiveKind::TarGz => "release.tar.gz",
            ArchiveKind::Zip => "release.zip",
        });
        let local_package = matches!(&declaration.location, DeclarationLocation::Local { .. });
        let (phase, started_event, succeeded_event, failed_event) = if local_package {
            (
                "release_copy",
                "plugin_package_release_copy_started",
                "plugin_package_release_copy_succeeded",
                "plugin_package_release_copy_failed",
            )
        } else {
            (
                "release_download",
                "plugin_package_release_download_started",
                "plugin_package_release_download_succeeded",
                "plugin_package_release_download_failed",
            )
        };
        let staging_started = Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            started_event,
            correlation_id,
            json!({
                "package_phase": phase,
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "build_log_path": resolved.build_log.display().to_string(),
                "release_id": release_id,
                "target": package.target,
                "expected_bytes": package.size_bytes,
            }),
        );
        let mut staging_cancellation = PhaseCancellationGuard::new(
            Arc::clone(&self.logger),
            failed_event,
            correlation_id,
            json!({
                "package_phase": phase,
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "release_id": release_id,
                "target": package.target,
            }),
        );
        let mut shutdown = self.shutdown.clone();
        let shutdown_requested = async move {
            if shutdown.borrow().is_some() {
                return;
            }
            while shutdown.changed().await.is_ok() {
                if shutdown.borrow().is_some() {
                    return;
                }
            }
        };
        let staging = async {
            match &declaration.location {
                DeclarationLocation::Git { .. } => stage_release_download(package, &archive).await,
                DeclarationLocation::Local { .. } => {
                    stage_local_release_package(package, resolved.source_root(), &archive).await
                }
            }
        };
        let staged = tokio::select! {
            result = staging => result,
            () = shutdown_requested => {
                if local_package {
                    Err(PackageError::new(
                        "operation_cancelled",
                        "release_copy",
                        "local release package copy was cancelled by daemon shutdown",
                    ))
                } else {
                    Err(PackageError::new(
                        "release_package_download_failed",
                        "download",
                        "release download was cancelled by daemon shutdown",
                    ))
                }
            }
        };
        staging_cancellation.complete();
        match staged {
            Ok(bytes) => self.logger.emit(
                LogLevel::Info,
                "oll::plugin::package",
                succeeded_event,
                correlation_id,
                json!({
                    "package_phase": phase,
                    "plugin_id": resolved.plugin_id.as_str(),
                    "plugin_name": resolved.plugin_name.as_str(),
                    "package_operation_id": operation_id,
                    "install_generation": generation.to_string(),
                    "release_id": release_id,
                    "target": package.target,
                    "bytes": bytes,
                    "duration_ms": staging_started.elapsed().as_millis(),
                }),
            ),
            Err(error) => {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::plugin::package",
                    failed_event,
                    correlation_id,
                    json!({
                        "package_phase": phase,
                        "plugin_id": resolved.plugin_id.as_str(),
                        "plugin_name": resolved.plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "install_generation": generation.to_string(),
                        "release_id": release_id,
                        "target": package.target,
                        "error_code": error.code(),
                        "duration_ms": staging_started.elapsed().as_millis(),
                    }),
                );
                return Err(error);
            }
        }

        let install_for_extract = install.to_owned();
        let extraction_cancelled = Arc::new(AtomicBool::new(false));
        let _cancel_extraction_on_drop =
            CancelBlockingArchiveOnDrop(Arc::clone(&extraction_cancelled));
        let shutdown = self.shutdown.clone();
        let publisher = resolved.publisher.clone();
        let (extraction, candidate_guard) = tokio::task::spawn_blocking(move || {
            let cancelled =
                || extraction_cancelled.load(Ordering::Acquire) || shutdown.borrow().is_some();
            let result = extract_release_archive(
                &archive,
                archive_kind,
                &install_for_extract,
                &publisher,
                &cancelled,
            );
            (result, candidate_guard)
        })
        .await
        .map_err(|_| {
            PackageError::new(
                "archive_unsafe",
                "archive",
                "release extraction task failed unexpectedly",
            )
        })?;
        extraction?;
        Ok((candidate_guard, release_id))
    }
}

struct CancelBlockingArchiveOnDrop(Arc<AtomicBool>);

impl Drop for CancelBlockingArchiveOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_archive_task_owner_requests_cooperative_cancellation() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let owner = CancelBlockingArchiveOnDrop(Arc::clone(&cancelled));
        assert!(!cancelled.load(Ordering::Acquire));

        drop(owner);

        assert!(cancelled.load(Ordering::Acquire));
    }
}
