use std::collections::BTreeSet;

use loro::{ContainerType, LoroDoc};
use sha2::Digest;

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        types::{ActiveReplica, CatalogEntry, EntryData},
    },
    catalog::decode_catalog_snapshot,
    catalog_schema::validate_root_schema,
    loro::import_loro_doc,
};

pub fn validate_document_snapshot(bytes: &[u8]) -> Result<LoroDoc, ReplicaError> {
    let doc = import_loro_doc(bytes, 0).map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("document Loro snapshot cannot be decoded: {error}"))
    })?;
    validate_root_schema(
        &doc,
        &[
            ("content", ContainerType::Text),
            ("data", ContainerType::Map),
        ],
        "document",
    )?;
    Ok(doc)
}

pub(crate) fn validate_loaded_replica(replica: &ActiveReplica) -> Result<(), ReplicaError> {
    let (root_catalog_node_id, decoded_entries) = decode_catalog_snapshot(&replica.catalog_loro)
        .map_err(|error| {
            ReplicaError::CorruptStore(format!("stored catalog Loro snapshot is invalid: {error}"))
        })?;
    if root_catalog_node_id != replica.root_catalog_node_id {
        return Err(ReplicaError::CorruptStore(
            "stored catalog root differs from replica metadata".to_owned(),
        ));
    }
    if decoded_entries.len() != replica.entries.len()
        || decoded_entries.iter().any(|(id, decoded)| {
            replica
                .entries
                .get(id)
                .is_none_or(|stored| !entries_equivalent(decoded, stored))
        })
    {
        return Err(ReplicaError::CorruptStore(
            "SQL catalog rows differ from the catalog Loro snapshot".to_owned(),
        ));
    }

    let paths = replica.projected_paths()?;
    for (id, entry) in &replica.entries {
        let mut expected = entry.clone();
        if let Some(path) = paths.get(id) {
            expected.recompute_revision_at_path(path);
        } else {
            expected.recompute_revision();
        }
        if expected.catalog_revision != entry.catalog_revision {
            return Err(ReplicaError::CorruptStore(
                "stored CatalogRevision does not match catalog state".to_owned(),
            ));
        }
    }

    let referenced_documents = replica
        .entries
        .values()
        .filter_map(CatalogEntry::document)
        .map(|document| document.document_id)
        .collect::<BTreeSet<_>>();
    if referenced_documents.len() != replica.documents.len()
        || replica
            .documents
            .keys()
            .any(|document_id| !referenced_documents.contains(document_id))
    {
        return Err(ReplicaError::CorruptStore(
            "stored document object set differs from catalog references".to_owned(),
        ));
    }
    for document in replica.documents.values() {
        let loro = validate_document_snapshot(&document.loro).map_err(|error| {
            ReplicaError::CorruptStore(format!("stored document Loro snapshot is invalid: {error}"))
        })?;
        let catalog_document = replica
            .entries
            .values()
            .filter_map(CatalogEntry::document)
            .find(|entry| entry.document_id == document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore(
                    "stored document object has no catalog metadata".to_owned(),
                )
            })?;
        let content = loro.get_text("content").to_string();
        let (encoded, promoted) = encode_text(
            &content,
            &catalog_document.encoding,
            catalog_document.has_byte_order_mark,
        )?;
        let encoded_size = u64::try_from(encoded.len()).map_err(|_| {
            ReplicaError::CorruptStore("stored document size overflows u64".to_owned())
        })?;
        if promoted || encoded_size != catalog_document.size_bytes {
            return Err(ReplicaError::CorruptStore(
                "stored document size or encoding differs from its catalog metadata".to_owned(),
            ));
        }
        let expected: [u8; 32] = sha2::Sha256::digest(&document.loro).into();
        if expected != document.revision {
            return Err(ReplicaError::CorruptStore(
                "stored DocumentRevision does not match document state".to_owned(),
            ));
        }
    }
    Ok(())
}

fn entries_equivalent(left: &CatalogEntry, right: &CatalogEntry) -> bool {
    if left.catalog_node_id != right.catalog_node_id
        || left.parent_catalog_node_id != right.parent_catalog_node_id
        || left.loro_tree_id != right.loro_tree_id
        || left.name != right.name
        || left.deleted != right.deleted
    {
        return false;
    }
    match (&left.data, &right.data) {
        (EntryData::Directory, EntryData::Directory) => true,
        (EntryData::Document(left), EntryData::Document(right)) => {
            left.document_id == right.document_id
                && left.media_type == right.media_type
                && left.encoding == right.encoding
                && left.has_byte_order_mark == right.has_byte_order_mark
                && left.size_bytes == right.size_bytes
        }
        (EntryData::Binary(left), EntryData::Binary(right)) => {
            left.binary_id == right.binary_id
                && left.media_type == right.media_type
                && left.versions.len() == right.versions.len()
                && left.versions.iter().all(|(stamp, left)| {
                    right.versions.get(stamp).is_some_and(|right| {
                        left.sha256 == right.sha256
                            && left.size_bytes == right.size_bytes
                            && left.media_type == right.media_type
                    })
                })
        }
        _ => false,
    }
}
