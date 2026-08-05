mod failure;
mod filesystem;
mod publication;
mod recovery;
mod transfer;

#[cfg(test)]
pub(super) use filesystem::{PublishTestHook, install_publish_test_hook};

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
};

use serde_json::json;
use time::OffsetDateTime;
use tokio::time::Instant;

use crate::{
    node::logging::{LogLevel, NodeLogger},
    protocol::oll,
};

use self::{filesystem::choose_destination, transfer::PendingTransfer};
use super::{
    ArtifactPublishIntent, PluginArtifact, PluginArtifactId, PluginError, PluginId,
    PluginInstanceId, PluginStore,
    protocol::{decode_plugin_artifact_id, encode_plugin_artifact_id},
};

pub(crate) const MAX_ARTIFACT_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ArtifactPublisher {
    store: PluginStore,
    download_dir: std::path::PathBuf,
    recovery_roots: Vec<std::path::PathBuf>,
    maximum_chunk_bytes: usize,
    logger: Arc<NodeLogger>,
    active_transfers: Arc<Mutex<HashSet<PluginArtifactId>>>,
    publications: publication::PublicationTracker,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ArtifactRecoveryReport {
    pub recovered: u64,
    pub failed: u64,
}

pub(crate) struct ArtifactSession {
    publisher: ArtifactPublisher,
    plugin_id: PluginId,
    plugin_instance_id: PluginInstanceId,
    transfers: HashMap<PluginArtifactId, PendingTransfer>,
}

struct TransferClaimGuard {
    publisher: ArtifactPublisher,
    artifact_id: PluginArtifactId,
}

impl ArtifactPublisher {
    pub async fn initialize(
        store: PluginStore,
        configured_download_dir: &Path,
        maximum_chunk_bytes: usize,
        logger: Arc<NodeLogger>,
        now: OffsetDateTime,
        startup_correlation_id: &str,
    ) -> Result<(Self, ArtifactRecoveryReport), PluginError> {
        if maximum_chunk_bytes == 0 {
            return Err(PluginError::InvalidArgument(
                "maximum artifact chunk bytes must be greater than zero".to_owned(),
            ));
        }
        let previous = store.artifact_download_dir().await?;
        let download_dir = filesystem::prepare_download_directory(configured_download_dir)?;
        let mut recovery_roots = vec![download_dir.clone()];
        if let Some(previous) = previous
            && previous != download_dir
            && filesystem::unchanged_cached_directory(&previous)?
        {
            recovery_roots.push(previous);
        }
        let publications = publication::PublicationTracker::new(Arc::clone(&logger));
        let publisher = Self {
            store,
            download_dir,
            recovery_roots,
            maximum_chunk_bytes,
            logger,
            active_transfers: Arc::new(Mutex::new(HashSet::new())),
            publications,
        };
        // Recovery is deliberately part of initialization. The node cannot mark
        // preceding-process jobs failed until this method returns.
        let report = publisher
            .recover_startup(now, startup_correlation_id)
            .await?;
        // Publish the new startup cache only after old-root orphan cleanup and
        // absolute-path intent recovery have completed. A crash before this
        // write therefore retains the old root for the next cleanup attempt.
        publisher
            .store
            .cache_artifact_download_dir(&publisher.download_dir)
            .await?;
        Ok((publisher, report))
    }

    pub fn maximum_chunk_bytes(&self) -> usize {
        self.maximum_chunk_bytes
    }

    #[cfg(test)]
    pub(crate) fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    pub fn session(
        &self,
        plugin_id: PluginId,
        plugin_instance_id: PluginInstanceId,
    ) -> ArtifactSession {
        ArtifactSession {
            publisher: self.clone(),
            plugin_id,
            plugin_instance_id,
            transfers: HashMap::new(),
        }
    }

    pub(crate) async fn settle_plugin_publications(
        &self,
        plugin_id: &PluginId,
    ) -> Result<(), PluginError> {
        self.publications.wait_for_plugin(plugin_id).await?;
        self.recover_plugin_intents(plugin_id, OffsetDateTime::now_utc())
            .await
    }

    pub(crate) async fn shutdown_publications(
        &self,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        self.publications.shutdown(deadline, correlation_id).await
    }
}

impl ArtifactSession {
    pub async fn start_transfer(
        &mut self,
        request: &oll::ArtifactTransferStart,
        correlation_id: &str,
    ) -> Result<oll::ArtifactTransferAccepted, PluginError> {
        let pending = PendingTransfer::start(
            request,
            &self.plugin_id,
            self.plugin_instance_id,
            &self.publisher.store,
            &self.publisher.download_dir,
            self.publisher.maximum_chunk_bytes,
            correlation_id,
        )
        .await?;
        let artifact_id = pending.artifact_id();
        let claimed = self
            .publisher
            .active_transfers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(artifact_id);
        if !claimed {
            pending.remove_staging()?;
            return Err(PluginError::AlreadyExists(format!(
                "plugin artifact {artifact_id} already has an active transfer in this deployment"
            )));
        }

        self.publisher.logger.emit(
            LogLevel::Info,
            "oll::plugin::artifact",
            "plugin_artifact_transfer_started",
            pending.correlation_id(),
            json!({
                "plugin_id": self.plugin_id.as_str(),
                "plugin_instance_id": self.plugin_instance_id.to_string(),
                "job_id": pending.job_id().to_string(),
                "artifact_id": artifact_id.to_string(),
                "bytes": pending.size_bytes(),
                "chunk_count": pending.chunk_count(),
            }),
        );
        self.transfers.insert(artifact_id, pending);
        Ok(oll::ArtifactTransferAccepted {
            artifact_id: Some(encode_plugin_artifact_id(artifact_id)),
        })
    }

    pub async fn receive_chunk(
        &mut self,
        chunk: &oll::ArtifactTransferChunk,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        let artifact_id = decode_plugin_artifact_id(chunk.artifact_id.as_ref(), "artifact_id")?;
        let pending = self.transfers.get(&artifact_id).ok_or_else(|| {
            PluginError::NotFound(format!(
                "plugin artifact {artifact_id} has no active transfer"
            ))
        })?;
        pending.validate_correlation(correlation_id)?;
        let validation = pending.validate_chunk(
            chunk.chunk_index,
            &chunk.data,
            self.publisher.maximum_chunk_bytes,
        );
        if let Err(error) = validation {
            self.fail_transfer(artifact_id, "artifact_chunk_invalid", &error)
                .await?;
            return Err(error);
        }

        let write = self
            .transfers
            .get_mut(&artifact_id)
            .expect("validated active artifact transfer")
            .write_chunk(&chunk.data)
            .await;
        if let Err(error) = write {
            self.fail_transfer(artifact_id, "artifact_staging_write_failed", &error)
                .await?;
            return Err(error);
        }
        Ok(())
    }

    pub async fn complete_transfer(
        &mut self,
        request: &oll::ArtifactTransferComplete,
        correlation_id: &str,
        now: OffsetDateTime,
    ) -> Result<(oll::ArtifactStored, PluginArtifact), PluginError> {
        let artifact_id = decode_plugin_artifact_id(request.artifact_id.as_ref(), "artifact_id")?;
        self.transfers
            .get(&artifact_id)
            .ok_or_else(|| {
                PluginError::NotFound(format!(
                    "plugin artifact {artifact_id} has no active transfer"
                ))
            })?
            .validate_correlation(correlation_id)?;
        let mut pending = self.transfers.remove(&artifact_id).ok_or_else(|| {
            PluginError::NotFound(format!(
                "plugin artifact {artifact_id} has no active transfer"
            ))
        })?;
        let _claim = TransferClaimGuard {
            publisher: self.publisher.clone(),
            artifact_id,
        };
        if let Err(error) = pending.finish_staging().await {
            self.fail_removed_transfer(pending, "artifact_validation_failed", &error)
                .await?;
            return Err(error);
        }
        let root_error = match pending.download_root_is_unchanged() {
            Ok(true) => None,
            Ok(false) => Some(PluginError::FailedPrecondition(
                "artifact download directory changed during transfer".to_owned(),
            )),
            Err(error) => Some(error),
        };
        if let Some(error) = root_error {
            self.fail_removed_transfer(pending, "artifact_download_directory_changed", &error)
                .await?;
            return Err(error);
        }

        let destination = match choose_destination(
            &self.publisher.download_dir,
            pending.file_name(),
            artifact_id,
        ) {
            Ok(destination) => destination,
            Err(error) => {
                self.fail_removed_transfer(pending, "artifact_destination_collision", &error)
                    .await?;
                return Err(error);
            }
        };
        let intent = pending.publish_intent(destination);
        if let Err(error) = self.publisher.store.prepare_artifact_publish(&intent).await {
            match self
                .publisher
                .store
                .artifact_publish_intent(artifact_id)
                .await
            {
                Ok(Some(stored)) if stored == intent => {}
                Ok(_) => {
                    self.fail_removed_transfer(pending, "artifact_publish_intent_failed", &error)
                        .await?;
                    return Err(error);
                }
                Err(_) => {
                    self.publisher.log_recovery_deferred(&intent, &error);
                    return Err(error);
                }
            }
        }
        // The durable intent now owns staging cleanup and crash recovery. Until
        // this point PendingTransfer::drop removes the private file if this
        // future is cancelled during validation, flush, or SQL admission.
        pending.relinquish_staging();

        // Once the SQL intent is durable, publication is owned by the
        // ArtifactPublisher rather than this session observer. Dropping an RPC
        // or aborting session cleanup cannot abandon a hard-link operation
        // after it has made the destination visible.
        let artifact = self.publisher.publish_owned(intent, now).await?;
        Ok((
            oll::ArtifactStored {
                artifact_id: Some(encode_plugin_artifact_id(artifact_id)),
            },
            artifact,
        ))
    }

    pub async fn abort_all(
        &mut self,
        error_code: &str,
        now: OffsetDateTime,
    ) -> Result<(), PluginError> {
        let transfers = self
            .transfers
            .drain()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for pending in transfers {
            self.publisher.release_transfer(pending.artifact_id());
            if let Err(error) = self
                .fail_job_and_cleanup(&pending, now, error_code, None)
                .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl ArtifactPublisher {
    fn release_transfer(&self, artifact_id: PluginArtifactId) {
        self.active_transfers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&artifact_id);
    }
}

impl Drop for TransferClaimGuard {
    fn drop(&mut self) {
        self.publisher.release_transfer(self.artifact_id);
    }
}

fn artifact_matches_intent(artifact: &PluginArtifact, intent: &ArtifactPublishIntent) -> bool {
    artifact.artifact_id == intent.artifact_id
        && artifact.job_id == intent.job_id
        && artifact.plugin_id == intent.plugin_id
        && artifact.file_name == intent.file_name
        && artifact.media_type == intent.media_type
        && artifact.size_bytes == intent.size_bytes
        && artifact.sha256 == intent.sha256
        && artifact.destination == intent.destination
}
