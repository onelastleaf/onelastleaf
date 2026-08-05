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
    ArchiveKind, ReleaseIndex, extract_release_archive, local_target, stage_release_download,
};

use super::validation::validate_platform_archive;
use super::*;

impl CandidateBuilder {
    pub(super) async fn materialize_release(
        &self,
        request: ReleaseMaterialization<'_>,
    ) -> Result<DirectoryGuard, PackageError> {
        let ReleaseMaterialization {
            declaration,
            resolved,
            operation_id,
            correlation_id,
            generation,
            install,
            candidate_guard,
        } = request;
        let release_id = declaration.release.as_deref().ok_or_else(|| {
            PackageError::new(
                "release_selection_required",
                "release_index",
                "release-mode declaration must select an opaque release ID",
            )
        })?;
        let release_source = fs::read_to_string(
            resolved.checkout.source_root.join("oll-release.json"),
        )
        .map_err(|error| {
            PackageError::io(
                "release_index_missing",
                "release_index",
                "selected repository has no readable oll-release.json",
                error,
            )
        })?;
        let index = ReleaseIndex::parse(&release_source, &resolved.publisher)?;
        let artifact = index.select(release_id, local_target()?)?;
        let archive_kind = artifact.archive;
        validate_platform_archive(archive_kind)?;
        let archive = resolved.staging.join(match archive_kind {
            ArchiveKind::TarGz => "release.tar.gz",
            ArchiveKind::Zip => "release.zip",
        });
        let download_started = Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::package",
            "plugin_package_release_download_started",
            correlation_id,
            json!({
                "package_phase": "release_download",
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "build_log_path": resolved.build_log.display().to_string(),
                "release_id": release_id,
                "target": artifact.target,
                "expected_bytes": artifact.size_bytes,
            }),
        );
        let mut download_cancellation = PhaseCancellationGuard::new(
            Arc::clone(&self.logger),
            "plugin_package_release_download_failed",
            correlation_id,
            json!({
                "package_phase": "release_download",
                "plugin_id": resolved.plugin_id.as_str(),
                "plugin_name": resolved.plugin_name.as_str(),
                "package_operation_id": operation_id,
                "install_generation": generation.to_string(),
                "release_id": release_id,
                "target": artifact.target,
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
        let download = tokio::select! {
            result = stage_release_download(artifact, &archive) => result,
            () = shutdown_requested => {
                Err(PackageError::new(
                    "artifact_download_failed",
                    "download",
                    "release download was cancelled by daemon shutdown",
                ))
            }
        };
        download_cancellation.complete();
        match download {
            Ok(()) => self.logger.emit(
                LogLevel::Info,
                "oll::plugin::package",
                "plugin_package_release_download_succeeded",
                correlation_id,
                json!({
                    "package_phase": "release_download",
                    "plugin_id": resolved.plugin_id.as_str(),
                    "plugin_name": resolved.plugin_name.as_str(),
                    "package_operation_id": operation_id,
                    "install_generation": generation.to_string(),
                    "release_id": release_id,
                    "target": artifact.target,
                    "bytes": artifact.size_bytes,
                    "duration_ms": download_started.elapsed().as_millis(),
                }),
            ),
            Err(error) => {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::plugin::package",
                    "plugin_package_release_download_failed",
                    correlation_id,
                    json!({
                        "package_phase": "release_download",
                        "plugin_id": resolved.plugin_id.as_str(),
                        "plugin_name": resolved.plugin_name.as_str(),
                        "package_operation_id": operation_id,
                        "install_generation": generation.to_string(),
                        "release_id": release_id,
                        "target": artifact.target,
                        "error_code": error.code(),
                        "duration_ms": download_started.elapsed().as_millis(),
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
        Ok(candidate_guard)
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
