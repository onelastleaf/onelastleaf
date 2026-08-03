use std::path::Path;

use loro::{ExportMode, LoroDoc};

use super::{
    super::{
        ReplicaError,
        model::{decode_catalog_snapshot, import_loro_doc, validate_document_snapshot},
        watcher::ReplicaRuntime,
    },
    candidate_build::empty_replication_object,
    types::{ReplicaObject, ReplicaObjectSummary, ReplicaUpdateValidationError},
};

impl ReplicaRuntime {
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
}
