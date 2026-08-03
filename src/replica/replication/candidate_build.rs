use std::collections::{BTreeMap, BTreeSet};

use loro::{ExportMode, LoroDoc};

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        model::{
            decode_catalog_snapshot, get_entry_record, import_loro_doc, new_loro_doc,
            recompute_live_catalog_revisions, validate_document_snapshot, validate_loaded_replica,
            write_entry_record,
        },
        types::{ActiveReplica, CatalogEntry, DocumentObject, EntryData},
    },
    types::{ReplicaObject, ReplicaObjectSummary},
};

pub(super) fn object_summary(
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

pub(super) fn empty_replication_object(
    object: ReplicaObject,
    peer_id: u64,
) -> Result<LoroDoc, ReplicaError> {
    let document = new_loro_doc(peer_id)?;
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

pub(super) fn build_candidate(
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

pub(super) fn require_complete_import(
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
