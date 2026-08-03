use loro::{ExportMode, VersionVector};
use sha2::{Digest, Sha256};

use super::{
    super::{ReplicaError, model::import_loro_doc, types::ActiveReplica},
    types::{ExportedReplicaObject, ReplicaObject},
};

pub(super) fn export_object(
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
