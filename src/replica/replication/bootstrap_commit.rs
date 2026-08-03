use uuid::Uuid;

use super::{
    super::{
        ReplicaError, identity,
        model::{merge_local_only_disk, scan_working_tree},
        store::{IdentityTransitionKind, NewBlob, NewBlobSource},
        watcher::ReplicaRuntime,
    },
    blobs::validate_candidate_blobs,
    bootstrap_build::build_bootstrap_replica,
    types::{BootstrapCandidate, ReplicationCommit},
};

impl ReplicaRuntime {
    pub(crate) async fn commit_bootstrap_candidate(
        &self,
        input: BootstrapCandidate,
        _commit_guard: &tokio::sync::OwnedMutexGuard<()>,
        writer_node_id: Uuid,
        correlation_id: &str,
    ) -> Result<ReplicationCommit, ReplicaError> {
        if self.state.read().await.is_some() || self.store.active_generation_id().await?.is_some() {
            return Err(ReplicaError::RevisionConflict(
                "replica initialized while bootstrap was in progress".to_owned(),
            ));
        }
        let object_count = u64::try_from(input.object_updates.len())
            .map_err(|_| ReplicaError::InvalidArgument("too many bootstrap objects".to_owned()))?;
        let blob_count = u64::try_from(input.blobs.len())
            .map_err(|_| ReplicaError::InvalidArgument("too many bootstrap blobs".to_owned()))?;
        let transferred_bytes = input
            .object_updates
            .values()
            .try_fold(0_u64, |total, bytes| {
                total
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                        ReplicaError::InvalidArgument("bootstrap payload is too large".to_owned())
                    })?)
                    .ok_or_else(|| {
                        ReplicaError::InvalidArgument(
                            "bootstrap transferred byte count overflowed".to_owned(),
                        )
                    })
            })?
            .checked_add(input.blobs.values().try_fold(0_u64, |total, blob| {
                total.checked_add(blob.size_bytes).ok_or_else(|| {
                    ReplicaError::InvalidArgument(
                        "bootstrap transferred byte count overflowed".to_owned(),
                    )
                })
            })?)
            .ok_or_else(|| {
                ReplicaError::InvalidArgument(
                    "bootstrap transferred byte count overflowed".to_owned(),
                )
            })?;
        let mut candidate =
            build_bootstrap_replica(input.claim_id, input.replica_id, &input.object_updates)?;
        let root = self.root.clone();
        let disk = tokio::task::spawn_blocking(move || scan_working_tree(&root))
            .await
            .map_err(|error| {
                ReplicaError::Internal(format!("bootstrap working-tree scan failed: {error}"))
            })??;
        let local = merge_local_only_disk(&candidate, &disk, writer_node_id, correlation_id)?;
        candidate = local.replica;
        let received_blobs = input.blobs;
        let local_blobs = local
            .blobs
            .into_iter()
            .filter(|blob| !received_blobs.contains_key(&blob.sha256))
            .collect::<Vec<_>>();
        validate_candidate_blobs(&self.store, &candidate, &received_blobs, &local_blobs).await?;
        let mut blobs = received_blobs
            .iter()
            .map(|(sha256, blob)| NewBlob {
                sha256: sha256.clone(),
                source: NewBlobSource::File {
                    path: blob.path.to_path_buf(),
                    size_bytes: blob.size_bytes,
                },
            })
            .collect::<Vec<_>>();
        blobs.extend(local_blobs);
        self.store
            .build_inactive_generation(&candidate, &blobs, &local.operations, &[])
            .await?;
        drop(received_blobs);
        if let Err(error) = identity::activate_candidate(
            &self.store,
            &self.config_root,
            None,
            &candidate,
            IdentityTransitionKind::Bootstrap,
            true,
        )
        .await
        {
            let _ = self
                .store
                .discard_inactive_generation(candidate.generation_id)
                .await;
            return Err(error);
        }
        *self.state.write().await = Some(candidate.clone());
        self.identities
            .advance_epoch()
            .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        match self.project_complete(&candidate).await {
            Ok(()) => {
                if let Err(error) = self
                    .store
                    .clear_projection_pending(candidate.generation_id)
                    .await
                {
                    self.log_failure(
                        "bootstrap_projection_marker_clear_failed",
                        correlation_id,
                        &error,
                    );
                }
            }
            Err(error) => self.log_failure("bootstrap_projection_failed", correlation_id, &error),
        }
        Ok(ReplicationCommit::Committed {
            object_count,
            blob_count,
            transferred_bytes,
        })
    }
}
