use std::collections::BTreeMap;

use loro::{Container, ExportMode, LoroMap, TreeID, UpdateOptions, ValueOrContainer};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        store::{NewBlob, NewBlobSource},
        types::{
            ActiveReplica, BinaryEntry, BinaryStamp, BinaryVersion, CatalogEntry, DocumentEntry,
            DocumentObject, EntryData,
        },
    },
    DiskEntry, DiskEntryData,
    loro::new_loro_doc,
    support::{loro_encode_error, loro_error},
};

pub(super) fn create_catalog_entry(
    replica: &mut ActiveReplica,
    disk_entry: &DiskEntry,
    catalog_node_id: Uuid,
    parent_catalog_node_id: Uuid,
    tree_id: TreeID,
    writer_node_id: Uuid,
    blobs: &mut Vec<NewBlob>,
) -> Result<CatalogEntry, ReplicaError> {
    let data = match &disk_entry.data {
        DiskEntryData::Directory => EntryData::Directory,
        DiskEntryData::Text(text) => {
            let document_id = Uuid::new_v4();
            let doc = new_loro_doc(replica.loro_peer_id)?;
            doc.set_next_commit_origin("filesystem");
            let _data = doc.get_map("data");
            let content = doc.get_text("content");
            content
                .update(&text.text, UpdateOptions::default())
                .map_err(|_| {
                    ReplicaError::Internal("cannot initialize Loro text content".to_owned())
                })?;
            doc.commit();
            let loro = doc
                .export(ExportMode::Snapshot)
                .map_err(loro_encode_error)?;
            replica
                .documents
                .insert(document_id, DocumentObject::new(document_id, loro));
            EntryData::Document(DocumentEntry {
                document_id,
                media_type: text.media_type.clone(),
                encoding: text.encoding.clone(),
                has_byte_order_mark: text.has_byte_order_mark,
                size_bytes: text.size_bytes,
            })
        }
        DiskEntryData::Binary(binary) => {
            replica.lamport_clock = replica
                .lamport_clock
                .checked_add(1)
                .ok_or_else(|| ReplicaError::CorruptStore("Lamport clock overflow".to_owned()))?;
            let binary_id = Uuid::new_v4();
            let stamp = BinaryStamp {
                lamport_clock: replica.lamport_clock,
                writer_node_id,
            };
            let size_bytes = u64::try_from(binary.bytes.len())
                .map_err(|_| ReplicaError::InvalidArgument("binary is too large".to_owned()))?;
            blobs.push(NewBlob {
                sha256: binary.sha256.clone(),
                source: NewBlobSource::Bytes(binary.bytes.clone()),
            });
            EntryData::Binary(BinaryEntry {
                binary_id,
                media_type: binary.media_type.clone(),
                versions: BTreeMap::from([(
                    stamp,
                    BinaryVersion {
                        sha256: binary.sha256.clone(),
                        size_bytes,
                        media_type: binary.media_type.clone(),
                    },
                )]),
            })
        }
    };
    let mut entry = CatalogEntry {
        catalog_node_id,
        parent_catalog_node_id,
        loro_tree_id: tree_id.to_string(),
        name: disk_entry.name.clone(),
        deleted: false,
        catalog_revision: [0; 32],
        data,
    };
    entry.recompute_revision();
    Ok(entry)
}

pub(crate) fn write_entry_record(
    record: &LoroMap,
    entry: &CatalogEntry,
) -> Result<(), ReplicaError> {
    record
        .insert("catalog_node_id", entry.catalog_node_id.to_string())
        .map_err(loro_error)?;
    record
        .insert(
            "parent_catalog_node_id",
            entry.parent_catalog_node_id.to_string(),
        )
        .map_err(loro_error)?;
    record
        .insert("name", entry.name.as_str())
        .map_err(loro_error)?;
    record
        .insert("deleted", entry.deleted)
        .map_err(loro_error)?;
    match &entry.data {
        EntryData::Directory => {
            record.insert("kind", "directory").map_err(loro_error)?;
        }
        EntryData::Document(document) => {
            record.insert("kind", "document").map_err(loro_error)?;
            record
                .insert("document_id", document.document_id.to_string())
                .map_err(loro_error)?;
            record
                .insert("media_type", document.media_type.as_str())
                .map_err(loro_error)?;
            record
                .insert("encoding", document.encoding.as_str())
                .map_err(loro_error)?;
            record
                .insert("has_byte_order_mark", document.has_byte_order_mark)
                .map_err(loro_error)?;
            record
                .insert("size_bytes", document.size_bytes.to_string())
                .map_err(loro_error)?;
        }
        EntryData::Binary(binary) => {
            record.insert("kind", "binary").map_err(loro_error)?;
            record
                .insert("binary_id", binary.binary_id.to_string())
                .map_err(loro_error)?;
            record
                .insert("media_type", binary.media_type.as_str())
                .map_err(loro_error)?;
            let versions = match record.get("binary_versions") {
                Some(ValueOrContainer::Container(Container::Map(map))) => map,
                Some(_) => {
                    return Err(ReplicaError::CorruptStore(
                        "binary_versions is not a LoroMap".to_owned(),
                    ));
                }
                None => record
                    .insert_container("binary_versions", LoroMap::new())
                    .map_err(loro_error)?,
            };
            for (stamp, version) in &binary.versions {
                let key = format!("{}@{}", stamp.lamport_clock, stamp.writer_node_id);
                let version_map = match versions.get(&key) {
                    Some(ValueOrContainer::Container(Container::Map(map))) => map,
                    Some(_) => {
                        return Err(ReplicaError::CorruptStore(
                            "binary version record is not a LoroMap".to_owned(),
                        ));
                    }
                    None => versions
                        .insert_container(&key, LoroMap::new())
                        .map_err(loro_error)?,
                };
                version_map
                    .insert("lamport_clock", stamp.lamport_clock.to_string())
                    .map_err(loro_error)?;
                version_map
                    .insert("writer_node_id", stamp.writer_node_id.to_string())
                    .map_err(loro_error)?;
                version_map
                    .insert("sha256", version.sha256.as_str())
                    .map_err(loro_error)?;
                version_map
                    .insert("size_bytes", version.size_bytes.to_string())
                    .map_err(loro_error)?;
                version_map
                    .insert("media_type", version.media_type.as_str())
                    .map_err(loro_error)?;
            }
        }
    }
    Ok(())
}
