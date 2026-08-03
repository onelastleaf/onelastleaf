use std::collections::{BTreeMap, BTreeSet};

use loro::ExportMode;
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        model::{
            decode_catalog_snapshot, generate_loro_peer_id, recompute_live_catalog_revisions,
            validate_document_snapshot, validate_loaded_replica,
        },
        types::{ActiveReplica, CatalogEntry, DocumentObject},
    },
    candidate_build::{empty_replication_object, require_complete_import},
    types::ReplicaObject,
};

pub(super) fn build_bootstrap_replica(
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
