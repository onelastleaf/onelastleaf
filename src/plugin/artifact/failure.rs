use std::borrow::Cow;

use serde_json::json;
use time::OffsetDateTime;

use crate::{
    node::logging::LogLevel,
    plugin::{ArtifactPublishIntent, JobState, PluginArtifactId, PluginError},
};

use super::{ArtifactPublisher, ArtifactSession, filesystem, transfer::PendingTransfer};

impl ArtifactSession {
    pub(super) async fn fail_transfer(
        &mut self,
        artifact_id: PluginArtifactId,
        code: &str,
        error: &PluginError,
    ) -> Result<(), PluginError> {
        if let Some(pending) = self.transfers.remove(&artifact_id) {
            self.publisher.release_transfer(artifact_id);
            self.fail_removed_transfer(pending, code, error).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn fail_transfer_for_test(
        &mut self,
        artifact_id: PluginArtifactId,
        code: &str,
        error: &PluginError,
    ) -> Result<(), PluginError> {
        self.fail_transfer(artifact_id, code, error).await
    }

    pub(super) async fn fail_removed_transfer(
        &self,
        pending: PendingTransfer,
        code: &str,
        error: &PluginError,
    ) -> Result<(), PluginError> {
        self.fail_job_and_cleanup(&pending, OffsetDateTime::now_utc(), code, Some(error))
            .await
    }

    pub(super) async fn fail_job_and_cleanup(
        &self,
        pending: &PendingTransfer,
        now: OffsetDateTime,
        code: &str,
        error: Option<&PluginError>,
    ) -> Result<(), PluginError> {
        let cleanup = pending.remove_staging();
        let message = durable_failure_message(code, error);
        let durable = match self.publisher.store.get_job(pending.job_id()).await {
            Ok(job) if !job.state.is_terminal() => self
                .publisher
                .store
                .finish_job(
                    job.job_id,
                    job.plugin_instance_id,
                    JobState::Failed,
                    None,
                    Some(code),
                    Some(message.as_ref()),
                    now,
                )
                .await
                .map(|_| ()),
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        self.publisher.logger.emit(
            LogLevel::Warn,
            "oll::plugin::artifact",
            "plugin_artifact_transfer_failed",
            pending.correlation_id(),
            json!({
                "plugin_id": self.plugin_id.as_str(),
                "plugin_instance_id": self.plugin_instance_id.to_string(),
                "job_id": pending.job_id().to_string(),
                "artifact_id": pending.artifact_id().to_string(),
                "error_code": code,
            }),
        );
        durable?;
        cleanup
    }
}

impl ArtifactPublisher {
    pub(super) async fn fail_publish_intent(
        &self,
        intent: &ArtifactPublishIntent,
        now: OffsetDateTime,
        code: &str,
    ) -> Result<(), PluginError> {
        let cleanup = match intent.destination.parent() {
            Some(parent) if filesystem::unchanged_cached_directory(parent)? => {
                filesystem::remove_staging_if_present(&intent.staging_path)
            }
            _ => Ok(()),
        };
        let message = durable_failure_message(code, None);
        let durable = match self.store.get_job(intent.job_id).await {
            Ok(job) if !job.state.is_terminal() => self
                .store
                .finish_job(
                    job.job_id,
                    job.plugin_instance_id,
                    JobState::Failed,
                    None,
                    Some(code),
                    Some(message.as_ref()),
                    now,
                )
                .await
                .map(|_| ()),
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        let discard = if cleanup.is_ok() && durable.is_ok() {
            self.store
                .discard_artifact_publish_intent(intent.artifact_id)
                .await
                .map(|_| ())
        } else {
            Ok(())
        };
        self.logger.emit(
            LogLevel::Warn,
            "oll::plugin::artifact",
            "plugin_artifact_transfer_failed",
            &intent.correlation_id,
            json!({
                "plugin_id": intent.plugin_id.as_str(),
                "job_id": intent.job_id.to_string(),
                "artifact_id": intent.artifact_id.to_string(),
                "error_code": code,
            }),
        );
        durable?;
        cleanup?;
        discard
    }

    pub(super) fn log_recovery_deferred(
        &self,
        intent: &ArtifactPublishIntent,
        error: &PluginError,
    ) {
        self.logger.emit(
            LogLevel::Warn,
            "oll::plugin::artifact",
            "plugin_artifact_recovery_deferred",
            &intent.correlation_id,
            json!({
                "plugin_id": intent.plugin_id.as_str(),
                "job_id": intent.job_id.to_string(),
                "artifact_id": intent.artifact_id.to_string(),
                "error_code": error.code(),
            }),
        );
    }
}

fn durable_failure_message<'a>(code: &str, error: Option<&'a PluginError>) -> Cow<'a, str> {
    if let Some(PluginError::InvalidArgument(message)) = error
        && safe_validation_message(code, message)
    {
        return Cow::Borrowed(message);
    }

    Cow::Borrowed(match code {
        "artifact_chunk_invalid" => "artifact chunk validation failed",
        "artifact_staging_write_failed" => "artifact staging write failed",
        "artifact_validation_failed" => "artifact staging validation failed",
        "artifact_download_directory_changed" => {
            "artifact download directory changed during transfer"
        }
        "artifact_destination_collision" => {
            "artifact destination was unavailable for no-replace publication"
        }
        "artifact_publish_intent_failed" => "artifact publication intent could not be persisted",
        "artifact_session_ended" | "plugin_session_ended" => {
            "artifact transfer ended before publication"
        }
        _ => "artifact operation failed",
    })
}

fn safe_validation_message(code: &str, message: &str) -> bool {
    matches!(
        (code, message),
        (
            "artifact_chunk_invalid",
            "artifact chunks must have contiguous zero-based indexes"
                | "artifact chunk is empty or exceeds the advertised limit"
                | "artifact byte count overflowed"
                | "artifact chunks exceed the declared size"
                | "artifact chunk count overflowed"
                | "artifact has too many chunks"
                | "artifact chunk sizes cannot satisfy the declared size and count"
        ) | (
            "artifact_validation_failed",
            "artifact transfer ended before its declared chunks and bytes arrived"
                | "artifact SHA-256 does not match its declaration"
        )
    )
}

impl Drop for ArtifactSession {
    fn drop(&mut self) {
        for pending in self.transfers.values() {
            self.publisher.release_transfer(pending.artifact_id());
            let _ = pending.remove_staging();
        }
    }
}
