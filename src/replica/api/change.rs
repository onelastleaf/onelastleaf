use std::collections::{BTreeSet, HashMap};

use uuid::Uuid;

use crate::protocol::oll;

use super::super::{
    ReplicaError,
    types::{ActiveReplica, EntryData, OperationKind, OperationRecord, OperationSource},
};

pub(super) fn changed_projection_paths(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    touched: &mut BTreeSet<Uuid>,
    body_touched: &BTreeSet<Uuid>,
) -> Vec<String> {
    let ids = before
        .entries
        .keys()
        .chain(after.entries.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut paths = BTreeSet::new();
    for id in ids {
        if before_paths.get(&id) != after_paths.get(&id) {
            touched.insert(id);
            if let Some(path) = before_paths.get(&id) {
                paths.insert(path.clone());
            }
            if let Some(path) = after_paths.get(&id) {
                paths.insert(path.clone());
            }
        }
    }
    for id in body_touched {
        if let Some(path) = before_paths.get(id) {
            paths.insert(path.clone());
        }
        if let Some(path) = after_paths.get(id) {
            paths.insert(path.clone());
        }
    }
    paths.into_iter().collect()
}

pub(super) fn document_operations(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    operation_id: &str,
    source: OperationSource,
    correlation_id: &str,
) -> Vec<OperationRecord> {
    let document_ids = before
        .documents
        .keys()
        .chain(after.documents.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let timestamp = time::OffsetDateTime::now_utc();
    let mut records = Vec::new();
    for document_id in document_ids {
        let before_entry = before.entries.values().find(|entry| {
            entry
                .document()
                .is_some_and(|document| document.document_id == document_id)
        });
        let after_entry = after.entries.values().find(|entry| {
            entry
                .document()
                .is_some_and(|document| document.document_id == document_id)
        });
        let before_live = before_entry.filter(|entry| !entry.deleted);
        let after_live = after_entry.filter(|entry| !entry.deleted);
        let path_before =
            before_live.and_then(|entry| before_paths.get(&entry.catalog_node_id).cloned());
        let path_after =
            after_live.and_then(|entry| after_paths.get(&entry.catalog_node_id).cloned());
        let revision_changed = before.documents.get(&document_id).map(|doc| doc.revision)
            != after.documents.get(&document_id).map(|doc| doc.revision);
        let catalog_changed = before_entry.map(|entry| entry.catalog_revision)
            != after_entry.map(|entry| entry.catalog_revision);
        let kind = match (before_live, after_live) {
            (None, Some(_)) => Some(OperationKind::Create),
            (Some(_), None) => Some(OperationKind::Delete),
            (Some(_), Some(_)) if path_before != path_after => Some(OperationKind::Move),
            (Some(_), Some(_)) if revision_changed || catalog_changed => {
                Some(OperationKind::Update)
            }
            _ => None,
        };
        let Some(kind) = kind else {
            continue;
        };
        let catalog_node_id = after_entry
            .or(before_entry)
            .map(|entry| entry.catalog_node_id)
            .expect("a changed document has a catalog entry");
        records.push(OperationRecord {
            timestamp,
            operation_id: operation_id.to_owned(),
            source,
            kind,
            catalog_node_id,
            document_id,
            path_before,
            path_after,
            correlation_id: correlation_id.to_owned(),
        });
    }
    records
}

pub(super) fn updated_node(
    replica: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    id: Uuid,
) -> Result<oll::UpdatedNode, ReplicaError> {
    let entry = replica
        .entries
        .get(&id)
        .ok_or_else(|| ReplicaError::CorruptStore("updated catalog entry is missing".to_owned()))?;
    let (document_id, document_revision, binary_id) = match &entry.data {
        EntryData::Directory => (None, None, None),
        EntryData::Document(document) => {
            let object = replica
                .documents
                .get(&document.document_id)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("updated document object is missing".to_owned())
                })?;
            (
                Some(oll::DocumentId {
                    value: document.document_id.to_string(),
                }),
                Some(oll::DocumentRevision {
                    token: object.revision.to_vec(),
                }),
                None,
            )
        }
        EntryData::Binary(binary) => (
            None,
            None,
            Some(oll::BinaryId {
                value: binary.binary_id.to_string(),
            }),
        ),
    };
    Ok(oll::UpdatedNode {
        path: after_paths
            .get(&id)
            .or_else(|| before_paths.get(&id))
            .map(|path| oll::DocumentPath {
                value: path.clone(),
            }),
        catalog_node_id: Some(oll::CatalogNodeId {
            value: id.to_string(),
        }),
        catalog_revision: Some(oll::CatalogRevision {
            token: entry.catalog_revision.to_vec(),
        }),
        document_id,
        document_revision,
        binary_id,
        deleted: entry.deleted,
    })
}
