use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use loro::{ExportMode, Frontiers, LoroDoc, VersionVector};
use sha2::{Digest, Sha256};
use tempfile::TempPath;
use uuid::Uuid;

use super::{
    ReplicaError,
    classification::encode_text,
    identity,
    model::{
        decode_catalog_snapshot, generate_loro_peer_id, get_entry_record, import_loro_doc,
        merge_local_only_disk, recompute_live_catalog_revisions, scan_working_tree,
        validate_document_snapshot, validate_loaded_replica, write_entry_record,
    },
    store::{IdentityTransitionKind, NewBlob, NewBlobSource},
    types::{
        ActiveReplica, CatalogEntry, DocumentObject, EntryData, OperationKind, OperationRecord,
        OperationSource,
    },
    watcher::ReplicaRuntime,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ReplicaObject {
    Catalog,
    Document(Uuid),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaObjectSummary {
    pub object: ReplicaObject,
    pub version_vector: VersionVector,
    pub frontier: Frontiers,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicaInventory {
    pub generation_id: Uuid,
    pub state_token: [u8; 32],
    pub replica_id: Uuid,
    pub objects: Vec<ReplicaObjectSummary>,
    pub blobs: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExportedReplicaObject {
    pub payload: Vec<u8>,
    pub resulting_version_vector: VersionVector,
    pub payload_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub(crate) struct BootstrapSource {
    pub inventory: ReplicaInventory,
    pub objects: BTreeMap<ReplicaObject, ExportedReplicaObject>,
}

#[derive(Debug)]
pub(crate) struct BootstrapCandidate {
    pub claim_id: Uuid,
    pub replica_id: Uuid,
    pub object_updates: BTreeMap<ReplicaObject, Vec<u8>>,
    pub blobs: BTreeMap<String, StagedBlob>,
}

#[derive(Debug)]
pub(crate) struct ReplicationCandidate {
    pub base_generation_id: Uuid,
    pub base_state_token: [u8; 32],
    pub object_updates: BTreeMap<ReplicaObject, Vec<u8>>,
    pub blobs: BTreeMap<String, StagedBlob>,
}

#[derive(Debug)]
pub(crate) struct StagedBlob {
    pub path: TempPath,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub(crate) enum ReplicaUpdateValidationError {
    Decode,
    Import,
    Invalid(ReplicaError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicationCommit {
    AlreadySatisfied,
    Committed {
        object_count: u64,
        blob_count: u64,
        transferred_bytes: u64,
    },
}

impl ReplicaRuntime {
    pub(crate) async fn capture_replica_inventory(&self) -> Result<ReplicaInventory, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let replica = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        let state_token = self.store.active_state_token(replica.generation_id).await?;
        let mut objects = Vec::with_capacity(1 + replica.documents.len());
        objects.push(object_summary(
            ReplicaObject::Catalog,
            &replica.catalog_loro,
        )?);
        for (document_id, document) in &replica.documents {
            objects.push(object_summary(
                ReplicaObject::Document(*document_id),
                &document.loro,
            )?);
        }
        let mut blobs = BTreeMap::new();
        for version in replica
            .entries
            .values()
            .filter_map(CatalogEntry::binary)
            .flat_map(|binary| binary.versions.values())
        {
            match blobs.insert(version.sha256.clone(), version.size_bytes) {
                Some(size) if size != version.size_bytes => {
                    return Err(ReplicaError::CorruptStore(
                        "one retained blob hash has contradictory sizes".to_owned(),
                    ));
                }
                _ => {}
            }
        }
        Ok(ReplicaInventory {
            generation_id: replica.generation_id,
            state_token,
            replica_id: replica.replica_id,
            objects,
            blobs,
        })
    }

    pub(crate) async fn export_replica_updates(
        &self,
        object: ReplicaObject,
        from: &VersionVector,
    ) -> Result<ExportedReplicaObject, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        export_object(replica, object, from)
    }

    pub(crate) async fn capture_bootstrap_source(&self) -> Result<BootstrapSource, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let replica = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        let state_token = self.store.active_state_token(replica.generation_id).await?;
        let mut objects = Vec::with_capacity(1 + replica.documents.len());
        let mut payloads = BTreeMap::new();
        for object in std::iter::once(ReplicaObject::Catalog).chain(
            replica
                .documents
                .keys()
                .copied()
                .map(ReplicaObject::Document),
        ) {
            let exported = export_object(&replica, object, &VersionVector::default())?;
            let document = match object {
                ReplicaObject::Catalog => import_loro_doc(&replica.catalog_loro, 0)?,
                ReplicaObject::Document(document_id) => import_loro_doc(
                    &replica
                        .documents
                        .get(&document_id)
                        .expect("bootstrap object came from retained document keys")
                        .loro,
                    0,
                )?,
            };
            objects.push(ReplicaObjectSummary {
                object,
                version_vector: document.oplog_vv(),
                frontier: document.oplog_frontiers(),
            });
            payloads.insert(object, exported);
        }
        let mut blobs = BTreeMap::new();
        for version in replica
            .entries
            .values()
            .filter_map(CatalogEntry::binary)
            .flat_map(|binary| binary.versions.values())
        {
            match blobs.insert(version.sha256.clone(), version.size_bytes) {
                Some(size) if size != version.size_bytes => {
                    return Err(ReplicaError::CorruptStore(
                        "one retained blob hash has contradictory sizes".to_owned(),
                    ));
                }
                _ => {}
            }
        }
        Ok(BootstrapSource {
            inventory: ReplicaInventory {
                generation_id: replica.generation_id,
                state_token,
                replica_id: replica.replica_id,
                objects,
                blobs,
            },
            objects: payloads,
        })
    }

    pub(crate) async fn stage_replication_blob(
        &self,
        sha256: &str,
        path: &Path,
    ) -> Result<u64, ReplicaError> {
        let size_bytes = self.store.blob_size(sha256).await?;
        self.store.write_blob_to_path(sha256, path).await?;
        Ok(size_bytes)
    }

    pub(crate) async fn validate_replica_update(
        &self,
        object: ReplicaObject,
        payload: &[u8],
    ) -> Result<ReplicaObjectSummary, ReplicaUpdateValidationError> {
        let metadata = LoroDoc::decode_import_blob_meta(payload, true)
            .map_err(|_| ReplicaUpdateValidationError::Decode)?;
        if metadata.mode.is_snapshot() {
            return Err(ReplicaUpdateValidationError::Invalid(
                ReplicaError::InvalidArgument(
                    "sync payload must be a Loro update batch, not a snapshot".to_owned(),
                ),
            ));
        }
        let state = self.state.read().await;
        let document = match (state.as_ref(), object) {
            (Some(replica), ReplicaObject::Catalog) => import_loro_doc(&replica.catalog_loro, 0)
                .map_err(ReplicaUpdateValidationError::Invalid)?,
            (Some(replica), ReplicaObject::Document(document_id)) => {
                match replica.documents.get(&document_id) {
                    Some(document) => import_loro_doc(&document.loro, 0)
                        .map_err(ReplicaUpdateValidationError::Invalid)?,
                    None => empty_replication_object(object, 0)
                        .map_err(ReplicaUpdateValidationError::Invalid)?,
                }
            }
            (None, _) => empty_replication_object(object, 0)
                .map_err(ReplicaUpdateValidationError::Invalid)?,
        };
        let status = document
            .import_with(payload, "sync")
            .map_err(|_| ReplicaUpdateValidationError::Import)?;
        if status.pending.is_some() {
            return Err(ReplicaUpdateValidationError::Import);
        }
        let snapshot = document.export(ExportMode::Snapshot).map_err(|_| {
            ReplicaUpdateValidationError::Invalid(ReplicaError::InvalidArgument(
                "merged Loro object cannot be encoded".to_owned(),
            ))
        })?;
        match object {
            ReplicaObject::Catalog => {
                decode_catalog_snapshot(&snapshot).map_err(|error| {
                    ReplicaUpdateValidationError::Invalid(ReplicaError::InvalidArgument(
                        error.to_string(),
                    ))
                })?;
            }
            ReplicaObject::Document(_) => {
                validate_document_snapshot(&snapshot).map_err(|error| {
                    ReplicaUpdateValidationError::Invalid(ReplicaError::InvalidArgument(
                        error.to_string(),
                    ))
                })?;
            }
        }
        Ok(ReplicaObjectSummary {
            object,
            version_vector: document.oplog_vv(),
            frontier: document.oplog_frontiers(),
        })
    }

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

fn export_object(
    replica: &ActiveReplica,
    object: ReplicaObject,
    from: &VersionVector,
) -> Result<ExportedReplicaObject, ReplicaError> {
    let bytes = match object {
        ReplicaObject::Catalog => &replica.catalog_loro,
        ReplicaObject::Document(document_id) => {
            &replica
                .documents
                .get(&document_id)
                .ok_or_else(|| {
                    ReplicaError::NotFound("requested replica object is not retained".to_owned())
                })?
                .loro
        }
    };
    let document = import_loro_doc(bytes, replica.loro_peer_id)?;
    let export_mode = if from.iter().next().is_none() {
        ExportMode::all_updates()
    } else {
        ExportMode::updates(from)
    };
    let payload = document
        .export(export_mode)
        .map_err(|_| ReplicaError::Internal("cannot encode Loro update batch".to_owned()))?;
    let resulting_version_vector = document.oplog_vv();
    let payload_sha256 = Sha256::digest(&payload).into();
    Ok(ExportedReplicaObject {
        payload,
        resulting_version_vector,
        payload_sha256,
    })
}

fn build_bootstrap_replica(
    generation_id: Uuid,
    replica_id: Uuid,
    updates: &BTreeMap<ReplicaObject, Vec<u8>>,
) -> Result<ActiveReplica, ReplicaError> {
    let catalog_update = updates.get(&ReplicaObject::Catalog).ok_or_else(|| {
        ReplicaError::InvalidArgument("bootstrap is missing the catalog update".to_owned())
    })?;
    let catalog = empty_replication_object(ReplicaObject::Catalog, 0)?;
    require_complete_import(&catalog, catalog_update, "bootstrap catalog")?;
    let initial_catalog = catalog.export(ExportMode::Snapshot).map_err(|_| {
        ReplicaError::InvalidArgument("bootstrap catalog cannot be encoded".to_owned())
    })?;
    let (root_catalog_node_id, entries) = decode_catalog_snapshot(&initial_catalog)
        .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
    let referenced_documents = entries
        .values()
        .filter_map(CatalogEntry::document)
        .map(|document| document.document_id)
        .collect::<BTreeSet<_>>();
    if updates.len() != referenced_documents.len().saturating_add(1)
        || updates.keys().any(|object| match object {
            ReplicaObject::Catalog => false,
            ReplicaObject::Document(document_id) => !referenced_documents.contains(document_id),
        })
    {
        return Err(ReplicaError::InvalidArgument(
            "bootstrap object set differs from the catalog references".to_owned(),
        ));
    }

    let mut excluded_peers = catalog.oplog_vv().keys().copied().collect::<BTreeSet<_>>();
    let mut imported_documents = BTreeMap::new();
    for document_id in referenced_documents {
        let update = updates
            .get(&ReplicaObject::Document(document_id))
            .ok_or_else(|| {
                ReplicaError::InvalidArgument(
                    "bootstrap catalog references a missing document update".to_owned(),
                )
            })?;
        let document = empty_replication_object(ReplicaObject::Document(document_id), 0)?;
        require_complete_import(&document, update, "bootstrap document")?;
        excluded_peers.extend(document.oplog_vv().keys().copied());
        let snapshot = document.export(ExportMode::Snapshot).map_err(|_| {
            ReplicaError::InvalidArgument("bootstrap document cannot be encoded".to_owned())
        })?;
        validate_document_snapshot(&snapshot)
            .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
        imported_documents.insert(document_id, document);
    }
    let loro_peer_id = generate_loro_peer_id(&excluded_peers)?;
    catalog
        .set_peer_id(loro_peer_id)
        .map_err(|_| ReplicaError::Internal("cannot assign bootstrap Loro peer ID".to_owned()))?;
    let catalog_loro = catalog
        .export(ExportMode::Snapshot)
        .map_err(|_| ReplicaError::Internal("cannot encode bootstrap catalog".to_owned()))?;
    let mut documents = BTreeMap::new();
    for (document_id, document) in imported_documents {
        document.set_peer_id(loro_peer_id).map_err(|_| {
            ReplicaError::Internal("cannot assign bootstrap document peer ID".to_owned())
        })?;
        let snapshot = document
            .export(ExportMode::Snapshot)
            .map_err(|_| ReplicaError::Internal("cannot encode bootstrap document".to_owned()))?;
        documents.insert(document_id, DocumentObject::new(document_id, snapshot));
    }
    for entry in entries.values() {
        let Some(metadata) = entry.document() else {
            continue;
        };
        let document = documents.get(&metadata.document_id).ok_or_else(|| {
            ReplicaError::InvalidArgument("bootstrap document object is missing".to_owned())
        })?;
        let loro = validate_document_snapshot(&document.loro)
            .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
        let text = loro.get_text("content").to_string();
        let (bytes, promoted) =
            encode_text(&text, &metadata.encoding, metadata.has_byte_order_mark)?;
        if promoted || u64::try_from(bytes.len()).ok() != Some(metadata.size_bytes) {
            return Err(ReplicaError::InvalidArgument(
                "bootstrap document bytes differ from catalog metadata".to_owned(),
            ));
        }
    }
    let lamport_clock = entries
        .values()
        .filter_map(CatalogEntry::binary)
        .flat_map(|binary| binary.versions.keys())
        .map(|stamp| stamp.lamport_clock)
        .max()
        .unwrap_or(0);
    let mut candidate = ActiveReplica {
        generation_id,
        replica_id,
        loro_peer_id,
        root_catalog_node_id,
        catalog_loro,
        lamport_clock,
        projection_generation: 1,
        entries,
        documents,
    };
    let paths = candidate.projected_paths()?;
    recompute_live_catalog_revisions(&mut candidate, &paths);
    validate_loaded_replica(&candidate)
        .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
    Ok(candidate)
}

fn object_summary(
    object: ReplicaObject,
    bytes: &[u8],
) -> Result<ReplicaObjectSummary, ReplicaError> {
    let document = import_loro_doc(bytes, 0)?;
    Ok(ReplicaObjectSummary {
        object,
        version_vector: document.oplog_vv(),
        frontier: document.oplog_frontiers(),
    })
}

fn empty_replication_object(object: ReplicaObject, peer_id: u64) -> Result<LoroDoc, ReplicaError> {
    let document = super::model::new_loro_doc(peer_id)?;
    match object {
        ReplicaObject::Catalog => {
            let _ = document.get_tree("tree");
            let _ = document.get_map("catalog");
            let _ = document.get_map("entries");
        }
        ReplicaObject::Document(_) => {
            let _ = document.get_text("content");
            let _ = document.get_map("data");
        }
    }
    Ok(document)
}

fn build_candidate(
    current: &ActiveReplica,
    updates: &BTreeMap<ReplicaObject, Vec<u8>>,
) -> Result<ActiveReplica, ReplicaError> {
    let catalog = import_loro_doc(&current.catalog_loro, current.loro_peer_id)?;
    catalog.set_next_commit_origin("sync");
    if let Some(update) = updates.get(&ReplicaObject::Catalog) {
        require_complete_import(&catalog, update, "catalog")?;
    }
    let mut catalog_bytes = catalog
        .export(ExportMode::Snapshot)
        .map_err(|_| ReplicaError::Internal("cannot encode merged catalog".to_owned()))?;
    let (mut root_catalog_node_id, mut entries) = decode_catalog_snapshot(&catalog_bytes)
        .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
    let referenced_documents = entries
        .values()
        .filter_map(CatalogEntry::document)
        .map(|document| document.document_id)
        .collect::<BTreeSet<_>>();
    if updates.keys().any(|object| {
        matches!(object, ReplicaObject::Document(id) if !referenced_documents.contains(id))
    }) {
        return Err(ReplicaError::InvalidArgument(
            "received document update is not referenced by the merged catalog".to_owned(),
        ));
    }
    let mut documents = BTreeMap::new();
    for document_id in referenced_documents {
        let document = match current.documents.get(&document_id) {
            Some(existing) => import_loro_doc(&existing.loro, current.loro_peer_id)?,
            None => empty_replication_object(
                ReplicaObject::Document(document_id),
                current.loro_peer_id,
            )?,
        };
        document.set_next_commit_origin("sync");
        if let Some(update) = updates.get(&ReplicaObject::Document(document_id)) {
            require_complete_import(&document, update, "document")?;
        } else if !current.documents.contains_key(&document_id) {
            return Err(ReplicaError::InvalidArgument(
                "merged catalog references a document absent from the round".to_owned(),
            ));
        }
        let snapshot = document
            .export(ExportMode::Snapshot)
            .map_err(|_| ReplicaError::Internal("cannot encode merged document".to_owned()))?;
        validate_document_snapshot(&snapshot)
            .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
        documents.insert(document_id, DocumentObject::new(document_id, snapshot));
    }

    let records = catalog.get_map("entries");
    let mut metadata_changed = false;
    for entry in entries.values_mut() {
        let EntryData::Document(metadata) = &mut entry.data else {
            continue;
        };
        let document = documents.get(&metadata.document_id).ok_or_else(|| {
            ReplicaError::InvalidArgument("merged document object is missing".to_owned())
        })?;
        let loro = validate_document_snapshot(&document.loro)
            .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
        let text = loro.get_text("content").to_string();
        let (encoded, promoted) =
            encode_text(&text, &metadata.encoding, metadata.has_byte_order_mark)?;
        if promoted {
            metadata.encoding = encoding_rs::UTF_8.name().to_owned();
            metadata.has_byte_order_mark = false;
        }
        let size = u64::try_from(encoded.len()).map_err(|_| {
            ReplicaError::InvalidArgument("merged document byte size overflows u64".to_owned())
        })?;
        if promoted || metadata.size_bytes != size {
            metadata.size_bytes = size;
            write_entry_record(&get_entry_record(&records, entry.catalog_node_id)?, entry)?;
            metadata_changed = true;
        }
    }
    if metadata_changed {
        catalog.commit();
        catalog_bytes = catalog
            .export(ExportMode::Snapshot)
            .map_err(|_| ReplicaError::Internal("cannot encode merged catalog".to_owned()))?;
        (root_catalog_node_id, entries) = decode_catalog_snapshot(&catalog_bytes)
            .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
    }

    let lamport_clock = entries
        .values()
        .filter_map(CatalogEntry::binary)
        .flat_map(|binary| binary.versions.keys())
        .map(|stamp| stamp.lamport_clock)
        .max()
        .unwrap_or(current.lamport_clock)
        .max(current.lamport_clock);
    let mut candidate = ActiveReplica {
        generation_id: current.generation_id,
        replica_id: current.replica_id,
        loro_peer_id: current.loro_peer_id,
        root_catalog_node_id,
        catalog_loro: catalog_bytes,
        lamport_clock,
        projection_generation: current.projection_generation,
        entries,
        documents,
    };
    let paths = candidate.projected_paths()?;
    recompute_live_catalog_revisions(&mut candidate, &paths);
    validate_loaded_replica(&candidate)
        .map_err(|error| ReplicaError::InvalidArgument(error.to_string()))?;
    Ok(candidate)
}

fn require_complete_import(
    document: &LoroDoc,
    update: &[u8],
    object: &'static str,
) -> Result<(), ReplicaError> {
    let metadata = LoroDoc::decode_import_blob_meta(update, true).map_err(|_| {
        ReplicaError::InvalidArgument(format!("{object} Loro update cannot be decoded"))
    })?;
    if metadata.mode.is_snapshot() {
        return Err(ReplicaError::InvalidArgument(format!(
            "{object} sync payload is a snapshot instead of an update batch"
        )));
    }
    let status = document.import_with(update, "sync").map_err(|_| {
        ReplicaError::InvalidArgument(format!("{object} Loro update cannot be imported"))
    })?;
    if status.pending.is_some() {
        return Err(ReplicaError::InvalidArgument(format!(
            "{object} Loro update has missing dependencies"
        )));
    }
    Ok(())
}

async fn validate_candidate_blobs(
    store: &super::store::ReplicaStore,
    candidate: &ActiveReplica,
    received: &BTreeMap<String, StagedBlob>,
    local: &[NewBlob],
) -> Result<(), ReplicaError> {
    let references = candidate
        .entries
        .values()
        .filter_map(CatalogEntry::binary)
        .flat_map(|binary| binary.versions.values())
        .map(|version| (version.sha256.as_str(), version.size_bytes))
        .collect::<BTreeMap<_, _>>();
    if received
        .keys()
        .any(|sha256| !references.contains_key(sha256.as_str()))
        || local
            .iter()
            .any(|blob| !references.contains_key(blob.sha256.as_str()))
    {
        return Err(ReplicaError::InvalidArgument(
            "received blob is not referenced by the merged catalog".to_owned(),
        ));
    }
    for (sha256, expected_size) in references {
        if let Some(blob) = received.get(sha256) {
            if blob.size_bytes != expected_size {
                return Err(ReplicaError::InvalidArgument(
                    "received blob hash or size differs from catalog metadata".to_owned(),
                ));
            }
        } else if let Some(blob) = local.iter().find(|blob| blob.sha256 == sha256) {
            if blob.size_bytes()? != expected_size {
                return Err(ReplicaError::InvalidArgument(
                    "local bootstrap blob size differs from catalog metadata".to_owned(),
                ));
            }
        } else if store.blob_size(sha256).await? != expected_size {
            return Err(ReplicaError::CorruptStore(
                "retained blob size differs from catalog metadata".to_owned(),
            ));
        }
    }
    Ok(())
}

fn sync_operations(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &std::collections::HashMap<Uuid, String>,
    after_paths: &std::collections::HashMap<Uuid, String>,
    correlation_id: &str,
) -> Vec<OperationRecord> {
    let mut documents = BTreeSet::new();
    documents.extend(
        before
            .entries
            .values()
            .filter_map(CatalogEntry::document)
            .map(|doc| doc.document_id),
    );
    documents.extend(
        after
            .entries
            .values()
            .filter_map(CatalogEntry::document)
            .map(|doc| doc.document_id),
    );
    let now = time::OffsetDateTime::now_utc();
    documents
        .into_iter()
        .filter_map(|document_id| {
            let before_entry = before.entries.values().find(|entry| {
                !entry.deleted
                    && entry
                        .document()
                        .is_some_and(|doc| doc.document_id == document_id)
            });
            let after_entry = after.entries.values().find(|entry| {
                !entry.deleted
                    && entry
                        .document()
                        .is_some_and(|doc| doc.document_id == document_id)
            });
            let kind = match (before_entry, after_entry) {
                (None, Some(_)) => OperationKind::Create,
                (Some(_), None) => OperationKind::Delete,
                (Some(before_entry), Some(after_entry)) => {
                    let before_path = before_paths.get(&before_entry.catalog_node_id);
                    let after_path = after_paths.get(&after_entry.catalog_node_id);
                    let before_revision =
                        before.documents.get(&document_id).map(|doc| doc.revision);
                    let after_revision = after.documents.get(&document_id).map(|doc| doc.revision);
                    if before_path != after_path {
                        OperationKind::Move
                    } else if before_revision != after_revision
                        || before_entry.catalog_revision != after_entry.catalog_revision
                    {
                        OperationKind::Update
                    } else {
                        return None;
                    }
                }
                (None, None) => return None,
            };
            let entry = after_entry.or(before_entry)?;
            Some(OperationRecord {
                timestamp: now,
                operation_id: Uuid::new_v4().to_string(),
                source: OperationSource::Sync,
                kind,
                catalog_node_id: entry.catalog_node_id,
                document_id,
                path_before: before_entry
                    .and_then(|entry| before_paths.get(&entry.catalog_node_id).cloned()),
                path_after: after_entry
                    .and_then(|entry| after_paths.get(&entry.catalog_node_id).cloned()),
                correlation_id: correlation_id.to_owned(),
            })
        })
        .collect()
}
