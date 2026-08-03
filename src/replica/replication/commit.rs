use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        store::{NewBlob, NewBlobSource},
        watcher::ReplicaRuntime,
    },
    blobs::validate_candidate_blobs,
    candidate_build::build_candidate,
    operations::sync_operations,
    types::{ReplicationCandidate, ReplicationCommit},
};

impl ReplicaRuntime {
    pub(crate) async fn commit_replication_candidate(
        &self,
        input: ReplicationCandidate,
        correlation_id: &str,
    ) -> Result<ReplicationCommit, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let current = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        if current.generation_id != input.base_generation_id
            || self.store.active_state_token(current.generation_id).await? != input.base_state_token
        {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed during synchronization".to_owned(),
            ));
        }
        if input.object_updates.is_empty() && input.blobs.is_empty() {
            return Ok(ReplicationCommit::AlreadySatisfied);
        }

        let transferred_bytes = input
            .object_updates
            .values()
            .try_fold(0_u64, |total, bytes| {
                total
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                        ReplicaError::InvalidArgument("replication payload is too large".to_owned())
                    })?)
                    .ok_or_else(|| {
                        ReplicaError::InvalidArgument(
                            "replication byte count overflowed".to_owned(),
                        )
                    })
            })?
            .checked_add(input.blobs.values().try_fold(0_u64, |total, blob| {
                total.checked_add(blob.size_bytes).ok_or_else(|| {
                    ReplicaError::InvalidArgument("replication byte count overflowed".to_owned())
                })
            })?)
            .ok_or_else(|| {
                ReplicaError::InvalidArgument("replication byte count overflowed".to_owned())
            })?;
        let object_count = u64::try_from(input.object_updates.len())
            .map_err(|_| ReplicaError::InvalidArgument("too many replica objects".to_owned()))?;
        let blob_count = u64::try_from(input.blobs.len())
            .map_err(|_| ReplicaError::InvalidArgument("too many replica blobs".to_owned()))?;
        let before_paths = current.projected_paths()?;
        let mut candidate = build_candidate(&current, &input.object_updates)?;
        validate_candidate_blobs(&self.store, &candidate, &input.blobs, &[]).await?;
        let new_blobs = input
            .blobs
            .iter()
            .map(|(sha256, blob)| NewBlob {
                sha256: sha256.clone(),
                source: NewBlobSource::File {
                    path: blob.path.to_path_buf(),
                    size_bytes: blob.size_bytes,
                },
            })
            .collect::<Vec<_>>();
        candidate.generation_id = Uuid::new_v4();
        candidate.projection_generation = candidate
            .projection_generation
            .checked_add(1)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("projection generation overflow".to_owned())
            })?;
        let after_paths = candidate.projected_paths()?;
        let operations = sync_operations(
            &current,
            &candidate,
            &before_paths,
            &after_paths,
            correlation_id,
        );
        self.store
            .build_sync_generation(current.generation_id, &candidate, &new_blobs, &operations)
            .await?;
        drop(input.blobs);
        if let Err(error) = self
            .store
            .activate_sync_generation(current.generation_id, input.base_state_token, &candidate)
            .await
        {
            let _ = self
                .store
                .discard_inactive_generation(candidate.generation_id)
                .await;
            return Err(error);
        }
        *self.state.write().await = Some(candidate.clone());

        match self.project_complete(&candidate).await {
            Ok(()) => {
                if let Err(error) = self
                    .store
                    .clear_projection_pending(candidate.generation_id)
                    .await
                {
                    self.log_failure(
                        "sync_projection_marker_clear_failed",
                        correlation_id,
                        &error,
                    );
                }
            }
            Err(error) => self.log_failure("sync_projection_failed", correlation_id, &error),
        }
        if let Err(error) = self
            .store
            .discard_inactive_generation(current.generation_id)
            .await
        {
            self.log_failure("sync_old_generation_cleanup_failed", correlation_id, &error);
        }
        Ok(ReplicationCommit::Committed {
            object_count,
            blob_count,
            transferred_bytes,
        })
    }
}
