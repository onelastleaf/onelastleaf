use std::collections::HashMap;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::protocol::oll;

use super::super::{
    ReplicaError,
    model::validate_name,
    types::{ActiveReplica, CatalogEntry, EntryData},
};

pub(super) fn api_elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn required_document_path(
    path: Option<oll::DocumentPath>,
    field: &'static str,
) -> Result<String, ReplicaError> {
    let path = path
        .ok_or_else(|| invalid(&format!("{field} must be specified")))?
        .value;
    validate_namespace_path(&path)?;
    Ok(path)
}

pub(super) fn validate_namespace_path(path: &str) -> Result<(), ReplicaError> {
    if path == "/" {
        return Ok(());
    }
    if !path.starts_with('/') || path.ends_with('/') {
        return Err(invalid(
            "document path must be absolute without a trailing slash",
        ));
    }
    for segment in path[1..].split('/') {
        validate_name(segment)?;
    }
    Ok(())
}

pub(super) fn required_entry<'a>(
    replica: &'a ActiveReplica,
    path: &str,
) -> Result<&'a CatalogEntry, ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root is not an ordinary catalog entry"));
    }
    replica
        .entry_at_path(path)?
        .ok_or_else(|| ReplicaError::NotFound("document path was not found".to_owned()))
}

pub(super) fn directory_metadata(
    replica: &ActiveReplica,
    path: &str,
) -> Result<oll::NodeMetadata, ReplicaError> {
    if path == "/" {
        let mut hash = Sha256::new();
        hash.update(b"oll.catalog.root");
        hash.update(replica.root_catalog_node_id.as_bytes());
        return Ok(oll::NodeMetadata {
            path: Some(oll::DocumentPath {
                value: "/".to_owned(),
            }),
            kind: oll::NodeKind::Directory as i32,
            catalog_revision: Some(oll::CatalogRevision {
                token: hash.finalize().to_vec(),
            }),
            media_type: None,
            size_bytes: 0,
            node_id: Some(oll::CatalogNodeId {
                value: replica.root_catalog_node_id.to_string(),
            }),
            document_id: None,
            binary_id: None,
            document_revision: None,
            encoding: None,
            has_byte_order_mark: false,
        });
    }
    let entry = required_entry(replica, path)?;
    if !matches!(entry.data, EntryData::Directory) {
        return Err(invalid("path must name a directory"));
    }
    node_metadata(replica, entry, path)
}

pub(super) fn node_metadata(
    replica: &ActiveReplica,
    entry: &CatalogEntry,
    path: &str,
) -> Result<oll::NodeMetadata, ReplicaError> {
    let mut metadata = oll::NodeMetadata {
        path: Some(oll::DocumentPath {
            value: path.to_owned(),
        }),
        kind: oll::NodeKind::Unspecified as i32,
        catalog_revision: Some(oll::CatalogRevision {
            token: entry.catalog_revision.to_vec(),
        }),
        media_type: None,
        size_bytes: 0,
        node_id: Some(oll::CatalogNodeId {
            value: entry.catalog_node_id.to_string(),
        }),
        document_id: None,
        binary_id: None,
        document_revision: None,
        encoding: None,
        has_byte_order_mark: false,
    };
    match &entry.data {
        EntryData::Directory => metadata.kind = oll::NodeKind::Directory as i32,
        EntryData::Document(document) => {
            let object = replica
                .documents
                .get(&document.document_id)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
                })?;
            metadata.kind = oll::NodeKind::Document as i32;
            metadata.media_type = Some(document.media_type.clone());
            metadata.size_bytes = document.size_bytes;
            metadata.document_id = Some(oll::DocumentId {
                value: document.document_id.to_string(),
            });
            metadata.document_revision = Some(oll::DocumentRevision {
                token: object.revision.to_vec(),
            });
            metadata.encoding = Some(document.encoding.clone());
            metadata.has_byte_order_mark = document.has_byte_order_mark;
        }
        EntryData::Binary(binary) => {
            let (_, version) = binary.winning_version().ok_or_else(|| {
                ReplicaError::CorruptStore("catalog binary has no winning version".to_owned())
            })?;
            metadata.kind = oll::NodeKind::Binary as i32;
            metadata.media_type = Some(binary.media_type.clone());
            metadata.size_bytes = version.size_bytes;
            metadata.binary_id = Some(oll::BinaryId {
                value: binary.binary_id.to_string(),
            });
        }
    }
    Ok(metadata)
}

pub(super) fn directory_tree_node(
    replica: &ActiveReplica,
    id: Uuid,
    path: &str,
    paths: &HashMap<Uuid, String>,
) -> Result<oll::DirectoryTreeNode, ReplicaError> {
    let metadata = if id == replica.root_catalog_node_id {
        directory_metadata(replica, "/")?
    } else {
        let entry = replica.entries.get(&id).ok_or_else(|| {
            ReplicaError::CorruptStore("directory-tree entry is missing".to_owned())
        })?;
        node_metadata(replica, entry, path)?
    };
    let mut child_entries = replica
        .entries
        .values()
        .filter(|entry| !entry.deleted && entry.parent_catalog_node_id == id)
        .collect::<Vec<_>>();
    child_entries.sort_by_key(|entry| paths.get(&entry.catalog_node_id).cloned());
    let children = child_entries
        .into_iter()
        .map(|entry| {
            let child_path = paths.get(&entry.catalog_node_id).ok_or_else(|| {
                ReplicaError::CorruptStore("directory-tree child path is missing".to_owned())
            })?;
            if matches!(entry.data, EntryData::Directory) {
                directory_tree_node(replica, entry.catalog_node_id, child_path, paths)
            } else {
                Ok(oll::DirectoryTreeNode {
                    metadata: Some(node_metadata(replica, entry, child_path)?),
                    children: Vec::new(),
                })
            }
        })
        .collect::<Result<Vec<_>, ReplicaError>>()?;
    Ok(oll::DirectoryTreeNode {
        metadata: Some(metadata),
        children,
    })
}

pub(super) fn invalid(message: &str) -> ReplicaError {
    ReplicaError::InvalidArgument(message.to_owned())
}
