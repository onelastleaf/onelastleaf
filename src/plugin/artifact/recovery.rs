use serde_json::json;
use time::OffsetDateTime;

use crate::{
    node::logging::LogLevel,
    plugin::{ArtifactPublishIntent, JobState, PluginError},
};

use super::{
    ArtifactPublisher, ArtifactRecoveryReport, artifact_matches_intent,
    filesystem::{
        PublishOutcome, VerifiedFile, cleanup_orphan_staging, inspect_file, intent_paths_are_valid,
        owned_staging_path, publish_staging_async, remove_staging_if_present,
    },
};

impl ArtifactPublisher {
    pub(super) async fn recover_startup(
        &self,
        now: OffsetDateTime,
        startup_correlation_id: &str,
    ) -> Result<ArtifactRecoveryReport, PluginError> {
        let intents = self.store.artifact_publish_intents().await?;
        let intent_count = intents.len();
        let started_at = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::artifact",
            "plugin_artifact_startup_recovery_started",
            startup_correlation_id,
            json!({ "intent_count": intent_count }),
        );
        let recovery = async {
            let mut report = ArtifactRecoveryReport::default();
            for intent in intents {
                if self.recover_recorded_intent(&intent, now).await? {
                    report.recovered += 1;
                } else {
                    report.failed += 1;
                }
            }
            // Every intent was finalized or discarded above. If processing had
            // stopped with a durable intent still present, this method would have
            // returned before orphan cleanup.
            cleanup_orphan_staging(&self.recovery_roots)?;
            Ok(report)
        }
        .await;
        self.logger.emit(
            if recovery.is_ok() {
                LogLevel::Info
            } else {
                LogLevel::Error
            },
            "oll::plugin::artifact",
            if recovery.is_ok() {
                "plugin_artifact_startup_recovery_succeeded"
            } else {
                "plugin_artifact_startup_recovery_failed"
            },
            startup_correlation_id,
            json!({
                "intent_count": intent_count,
                "artifact_intents_recovered": recovery.as_ref().ok().map(|report| report.recovered),
                "artifact_intents_failed": recovery.as_ref().ok().map(|report| report.failed),
                "error_code": recovery.as_ref().err().map(PluginError::code),
                "duration_ms": started_at.elapsed().as_millis(),
            }),
        );
        recovery
    }

    pub(super) async fn recover_plugin_intents(
        &self,
        plugin_id: &crate::plugin::PluginId,
        now: OffsetDateTime,
    ) -> Result<(), PluginError> {
        let intents = self.store.artifact_publish_intents().await?;
        for intent in intents
            .into_iter()
            .filter(|intent| &intent.plugin_id == plugin_id)
        {
            self.recover_recorded_intent(&intent, now).await?;
        }
        if self
            .store
            .artifact_publish_intents()
            .await?
            .iter()
            .any(|intent| &intent.plugin_id == plugin_id)
        {
            return Err(PluginError::FailedPrecondition(format!(
                "plugin {plugin_id} still has an unfinished artifact publication"
            )));
        }
        Ok(())
    }

    async fn recover_recorded_intent(
        &self,
        intent: &ArtifactPublishIntent,
        now: OffsetDateTime,
    ) -> Result<bool, PluginError> {
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::artifact",
            "plugin_artifact_recovery_started",
            &intent.correlation_id,
            json!({
                "plugin_id": intent.plugin_id.as_str(),
                "job_id": intent.job_id.to_string(),
                "artifact_id": intent.artifact_id.to_string(),
                "bytes": intent.size_bytes,
            }),
        );
        let outcome = match self.recover_intent(intent, now).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.log_recovery_error(intent, &error);
                return Err(error);
            }
        };
        match outcome {
            RecoveryOutcome::Recovered => {
                self.logger.emit(
                    LogLevel::Info,
                    "oll::plugin::artifact",
                    "plugin_artifact_recovery_completed",
                    &intent.correlation_id,
                    json!({
                        "plugin_id": intent.plugin_id.as_str(),
                        "job_id": intent.job_id.to_string(),
                        "artifact_id": intent.artifact_id.to_string(),
                        "outcome": "recovered",
                    }),
                );
                Ok(true)
            }
            RecoveryOutcome::Contradictory(reason) => {
                if let Err(error) = self.fail_recovery_intent(intent, now, reason).await {
                    self.log_recovery_error(intent, &error);
                    return Err(error);
                }
                self.logger.emit(
                    LogLevel::Warn,
                    "oll::plugin::artifact",
                    "plugin_artifact_recovery_completed",
                    &intent.correlation_id,
                    json!({
                        "plugin_id": intent.plugin_id.as_str(),
                        "job_id": intent.job_id.to_string(),
                        "artifact_id": intent.artifact_id.to_string(),
                        "outcome": "failed",
                        "error_code": "artifact_recovery_failed",
                    }),
                );
                Ok(false)
            }
        }
    }

    async fn recover_intent(
        &self,
        intent: &ArtifactPublishIntent,
        now: OffsetDateTime,
    ) -> Result<RecoveryOutcome, PluginError> {
        if !intent_paths_are_valid(intent) || !self.intent_uses_unchanged_root(intent)? {
            return Ok(RecoveryOutcome::Contradictory(
                "artifact recovery paths are invalid",
            ));
        }
        match inspect_file(&intent.destination, intent.size_bytes, &intent.sha256)? {
            VerifiedFile::Matching => {
                remove_staging_if_present(&intent.staging_path)?;
            }
            VerifiedFile::Contradictory => {
                return Ok(RecoveryOutcome::Contradictory(
                    "artifact destination contradicts its durable intent",
                ));
            }
            VerifiedFile::Missing => {
                match inspect_file(&intent.staging_path, intent.size_bytes, &intent.sha256)? {
                    VerifiedFile::Matching => match publish_staging_async(intent.clone()).await? {
                        PublishOutcome::Published | PublishOutcome::AlreadyMatching => {}
                        PublishOutcome::Collision => {
                            return Ok(RecoveryOutcome::Contradictory(
                                "artifact destination appeared during recovery",
                            ));
                        }
                    },
                    VerifiedFile::Missing => {
                        return Ok(RecoveryOutcome::Contradictory(
                            "artifact staging and destination are both missing",
                        ));
                    }
                    VerifiedFile::Contradictory => {
                        return Ok(RecoveryOutcome::Contradictory(
                            "artifact staging contradicts its durable intent",
                        ));
                    }
                }
            }
        }
        let artifact = self
            .store
            .finalize_artifact_publish(intent.artifact_id, now)
            .await?;
        if !artifact_matches_intent(&artifact, intent) {
            return Err(PluginError::CorruptStore(
                "stored plugin artifact contradicts its recovery intent".to_owned(),
            ));
        }
        Ok(RecoveryOutcome::Recovered)
    }

    async fn fail_recovery_intent(
        &self,
        intent: &ArtifactPublishIntent,
        now: OffsetDateTime,
        message: &str,
    ) -> Result<(), PluginError> {
        let cleanup = if self.intent_uses_unchanged_root(intent)?
            && owned_staging_path(&intent.staging_path, intent.artifact_id)
        {
            remove_staging_if_present(&intent.staging_path)
        } else {
            Ok(())
        };
        let durable = match self.store.get_job(intent.job_id).await {
            Ok(job) if !job.state.is_terminal() => self
                .store
                .finish_job(
                    job.job_id,
                    job.plugin_instance_id,
                    JobState::Failed,
                    None,
                    Some("artifact_recovery_failed"),
                    Some(message),
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
        durable?;
        cleanup?;
        discard
    }

    fn log_recovery_error(&self, intent: &ArtifactPublishIntent, error: &PluginError) {
        self.logger.emit(
            LogLevel::Warn,
            "oll::plugin::artifact",
            "plugin_artifact_recovery_failed",
            &intent.correlation_id,
            json!({
                "plugin_id": intent.plugin_id.as_str(),
                "job_id": intent.job_id.to_string(),
                "artifact_id": intent.artifact_id.to_string(),
                "error_code": error.code(),
            }),
        );
    }

    fn intent_uses_unchanged_root(
        &self,
        intent: &ArtifactPublishIntent,
    ) -> Result<bool, PluginError> {
        let Some(parent) = intent.destination.parent() else {
            return Ok(false);
        };
        if !self.recovery_roots.iter().any(|root| root == parent) {
            return Ok(false);
        }
        super::filesystem::unchanged_cached_directory(parent)
    }
}

enum RecoveryOutcome {
    Recovered,
    Contradictory(&'static str),
}
