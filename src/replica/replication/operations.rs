use std::collections::{BTreeSet, HashMap};

use uuid::Uuid;

use super::super::types::{
    ActiveReplica, CatalogEntry, OperationKind, OperationRecord, OperationSource,
};

pub(super) fn sync_operations(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
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
