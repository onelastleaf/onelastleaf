use std::collections::{BTreeMap, BTreeSet};

use loro::{LoroDoc, LoroMap, LoroTree, TreeParentId, UpdateOptions};
use uuid::Uuid;

use crate::protocol::oll;

use super::{
    super::{
        ReplicaError,
        model::{
            get_entry_record, import_loro_doc, new_loro_doc, parent_namespace_path, parse_tree_id,
            validate_name, write_entry_record,
        },
        types::{
            ActiveReplica, CatalogEntry, DocumentEntry, EntryData, OperationSource,
            portable_name_key,
        },
    },
    catalog::{invalid, required_document_path, required_entry},
    crdt_mutation::apply_crdt_operation,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_mutation(
    replica: &mut ActiveReplica,
    catalog: &LoroDoc,
    tree: &LoroTree,
    entries: &LoroMap,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    touched: &mut BTreeSet<Uuid>,
    body_touched: &mut BTreeSet<Uuid>,
    catalog_changed: &mut bool,
    mutation: oll::document_mutation::Mutation,
    source: OperationSource,
) -> Result<(), ReplicaError> {
    match mutation {
        oll::document_mutation::Mutation::CreateDirectory(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            create_entry(
                replica, tree, entries, &path, None, source, documents, touched,
            )?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::CreateDocument(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            if mutation.media_type.is_empty() || mutation.media_type.contains('\0') {
                return Err(invalid(
                    "document media_type must be non-empty and NUL-free",
                ));
            }
            create_entry(
                replica,
                tree,
                entries,
                &path,
                Some((mutation.content, mutation.media_type)),
                source,
                documents,
                touched,
            )?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::ReplaceDocument(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            let (catalog_node_id, document_id, doc) = editable_document(replica, documents, &path)?;
            doc.set_next_commit_origin(source.as_str());
            doc.get_text("content")
                .update(&mutation.content, UpdateOptions::default())
                .map_err(|_| invalid("document text replacement exceeded its update budget"))?;
            if let Some(media_type) = mutation.media_type {
                if media_type.is_empty() || media_type.contains('\0') {
                    return Err(invalid(
                        "document media_type must be non-empty and NUL-free",
                    ));
                }
                let entry = replica.entries.get_mut(&catalog_node_id).ok_or_else(|| {
                    ReplicaError::CorruptStore("edited catalog entry is missing".to_owned())
                })?;
                let EntryData::Document(document) = &mut entry.data else {
                    unreachable!()
                };
                document.media_type = media_type;
                write_entry_record(&get_entry_record(entries, catalog_node_id)?, entry)?;
                *catalog_changed = true;
            }
            let _ = document_id;
            touched.insert(catalog_node_id);
            body_touched.insert(catalog_node_id);
        }
        oll::document_mutation::Mutation::SpliceDocumentText(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            let (catalog_node_id, _, doc) = editable_document(replica, documents, &path)?;
            let index = usize_index(mutation.scalar_index, "scalar_index")?;
            let count = usize_index(mutation.delete_scalar_count, "delete_scalar_count")?;
            let content = doc.get_text("content");
            validate_range(index, count, content.len_unicode(), "document text range")?;
            doc.set_next_commit_origin(source.as_str());
            content
                .splice(index, count, &mutation.insert_text)
                .map_err(|_| invalid("document text splice range is invalid"))?;
            touched.insert(catalog_node_id);
            body_touched.insert(catalog_node_id);
        }
        oll::document_mutation::Mutation::DeleteNode(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            delete_entry(replica, tree, entries, &path, mutation.recursive, touched)?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::MoveNode(mutation) => {
            let source_path = required_document_path(mutation.source, "source")?;
            let destination = required_document_path(mutation.destination, "destination")?;
            move_entry(replica, tree, entries, &source_path, &destination, touched)?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::ApplyCrdtOperations(mutation) => {
            let path = required_document_path(mutation.document, "document")?;
            if mutation.operations.is_empty() {
                return Err(invalid("CRDT mutation must contain at least one operation"));
            }
            let (catalog_node_id, _, doc) = editable_document(replica, documents, &path)?;
            doc.set_next_commit_origin(source.as_str());
            for operation in mutation.operations {
                apply_crdt_operation(
                    &doc,
                    operation
                        .operation
                        .ok_or_else(|| invalid("CRDT operation must be specified"))?,
                )?;
            }
            touched.insert(catalog_node_id);
        }
    }
    let _ = catalog;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    path: &str,
    document: Option<(String, String)>,
    source: OperationSource,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root cannot be created"));
    }
    if replica.entry_at_path(path)?.is_some() {
        return Err(ReplicaError::AlreadyExists(
            "document path already exists".to_owned(),
        ));
    }
    let parent_path = parent_namespace_path(path)?;
    let parent_id = if parent_path == "/" {
        replica.root_catalog_node_id
    } else {
        let parent = required_entry(replica, parent_path)?;
        if !matches!(parent.data, EntryData::Directory) {
            return Err(invalid("new entry parent must be a directory"));
        }
        parent.catalog_node_id
    };
    let name = path
        .rsplit('/')
        .next()
        .ok_or_else(|| invalid("invalid path"))?;
    validate_name(name)?;
    reject_local_name_collision(replica, parent_id, name, None)?;
    let tree_id = if parent_id == replica.root_catalog_node_id {
        tree.create(TreeParentId::Root)
    } else {
        let parent = replica
            .entries
            .get(&parent_id)
            .ok_or_else(|| ReplicaError::CorruptStore("new entry parent is missing".to_owned()))?;
        tree.create(parse_tree_id(&parent.loro_tree_id)?)
    }
    .map_err(loro_failure)?;
    let catalog_node_id = Uuid::new_v4();
    tree.get_meta(tree_id)
        .map_err(loro_failure)?
        .insert("catalog_node_id", catalog_node_id.to_string())
        .map_err(loro_failure)?;
    let data = if let Some((content, media_type)) = document {
        let document_id = Uuid::new_v4();
        let doc = new_loro_doc(replica.loro_peer_id)?;
        doc.set_next_commit_origin(source.as_str());
        let _data = doc.get_map("data");
        doc.get_text("content")
            .update(&content, UpdateOptions::default())
            .map_err(|_| invalid("cannot initialize document text"))?;
        documents.insert(document_id, doc);
        EntryData::Document(DocumentEntry {
            document_id,
            media_type,
            encoding: encoding_rs::UTF_8.name().to_owned(),
            has_byte_order_mark: false,
            size_bytes: u64::try_from(content.len())
                .map_err(|_| invalid("document content is too large"))?,
        })
    } else {
        EntryData::Directory
    };
    let mut entry = CatalogEntry {
        catalog_node_id,
        parent_catalog_node_id: parent_id,
        loro_tree_id: tree_id.to_string(),
        name: name.to_owned(),
        deleted: false,
        catalog_revision: [0; 32],
        data,
    };
    entry.recompute_revision();
    let record = entries
        .insert_container(&catalog_node_id.to_string(), LoroMap::new())
        .map_err(loro_failure)?;
    write_entry_record(&record, &entry)?;
    replica.entries.insert(catalog_node_id, entry);
    touched.insert(catalog_node_id);
    Ok(())
}

fn editable_document(
    replica: &ActiveReplica,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    path: &str,
) -> Result<(Uuid, Uuid, LoroDoc), ReplicaError> {
    let entry = required_entry(replica, path)?;
    let document = entry
        .document()
        .ok_or_else(|| invalid("document mutation path must name a text document"))?;
    if let std::collections::btree_map::Entry::Vacant(slot) = documents.entry(document.document_id)
    {
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
            })?;
        slot.insert(import_loro_doc(&object.loro, replica.loro_peer_id)?);
    }
    let doc = documents
        .get(&document.document_id)
        .cloned()
        .ok_or_else(|| ReplicaError::Internal("editable document is missing".to_owned()))?;
    Ok((entry.catalog_node_id, document.document_id, doc))
}

fn delete_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    path: &str,
    recursive: bool,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root cannot be deleted"));
    }
    let target = required_entry(replica, path)?;
    let target_id = target.catalog_node_id;
    let paths = replica.projected_paths()?;
    let mut targets = paths
        .iter()
        .filter_map(|(id, candidate)| {
            (candidate == path || candidate.starts_with(&format!("{path}/"))).then_some(*id)
        })
        .collect::<Vec<_>>();
    if targets.len() > 1 && !recursive {
        return Err(invalid(
            "non-empty directory deletion requires recursive=true",
        ));
    }
    targets.sort_by_key(|id| {
        std::cmp::Reverse(
            paths
                .get(id)
                .map_or(0, |candidate| candidate.matches('/').count()),
        )
    });
    for id in targets {
        let entry = replica
            .entries
            .get_mut(&id)
            .ok_or_else(|| ReplicaError::CorruptStore("deletion target is missing".to_owned()))?;
        tree.delete(parse_tree_id(&entry.loro_tree_id)?)
            .map_err(loro_failure)?;
        entry.deleted = true;
        entry.recompute_revision();
        write_entry_record(&get_entry_record(entries, id)?, entry)?;
        touched.insert(id);
    }
    if !touched.contains(&target_id) {
        return Err(ReplicaError::Internal(
            "catalog deletion did not include its target".to_owned(),
        ));
    }
    Ok(())
}

fn move_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    source: &str,
    destination: &str,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if source == "/" || destination == "/" {
        return Err(invalid("replica root cannot be moved or replaced"));
    }
    if source == destination {
        return Ok(());
    }
    if replica.entry_at_path(destination)?.is_some() {
        return Err(ReplicaError::AlreadyExists(
            "move destination already exists".to_owned(),
        ));
    }
    if destination.starts_with(&format!("{source}/")) {
        return Err(invalid("catalog entry cannot be moved beneath itself"));
    }
    let target_id = required_entry(replica, source)?.catalog_node_id;
    let parent_path = parent_namespace_path(destination)?;
    let parent_id = if parent_path == "/" {
        replica.root_catalog_node_id
    } else {
        let parent = required_entry(replica, parent_path)?;
        if !matches!(parent.data, EntryData::Directory) {
            return Err(invalid("move destination parent must be a directory"));
        }
        parent.catalog_node_id
    };
    let name = destination
        .rsplit('/')
        .next()
        .ok_or_else(|| invalid("move destination is invalid"))?;
    validate_name(name)?;
    reject_local_name_collision(replica, parent_id, name, Some(target_id))?;
    let parent = if parent_id == replica.root_catalog_node_id {
        TreeParentId::Root
    } else {
        TreeParentId::Node(parse_tree_id(
            &replica
                .entries
                .get(&parent_id)
                .ok_or_else(|| ReplicaError::CorruptStore("move parent is missing".to_owned()))?
                .loro_tree_id,
        )?)
    };
    let entry = replica
        .entries
        .get_mut(&target_id)
        .ok_or_else(|| ReplicaError::CorruptStore("move target is missing".to_owned()))?;
    tree.mov(parse_tree_id(&entry.loro_tree_id)?, parent)
        .map_err(loro_failure)?;
    entry.parent_catalog_node_id = parent_id;
    entry.name = name.to_owned();
    entry.recompute_revision();
    write_entry_record(&get_entry_record(entries, target_id)?, entry)?;
    touched.insert(target_id);
    Ok(())
}

fn reject_local_name_collision(
    replica: &ActiveReplica,
    parent_id: Uuid,
    name: &str,
    except: Option<Uuid>,
) -> Result<(), ReplicaError> {
    let key = portable_name_key(name);
    if replica.entries.values().any(|entry| {
        !entry.deleted
            && entry.parent_catalog_node_id == parent_id
            && Some(entry.catalog_node_id) != except
            && portable_name_key(&entry.name) == key
    }) {
        Err(ReplicaError::AlreadyExists(
            "a sibling already has the same portable name".to_owned(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn api_uuid(value: Option<&str>, field: &str) -> Result<Uuid, ReplicaError> {
    let value = value.ok_or_else(|| invalid(&format!("{field} must be specified")))?;
    let id = Uuid::parse_str(value)
        .map_err(|_| invalid(&format!("{field} must be a canonical UUID v4")))?;
    if id.get_version_num() != 4 || id.to_string() != value {
        return Err(invalid(&format!("{field} must be a canonical UUID v4")));
    }
    Ok(id)
}

pub(super) fn usize_index(value: u64, field: &str) -> Result<usize, ReplicaError> {
    usize::try_from(value).map_err(|_| invalid(&format!("{field} is too large")))
}

pub(super) fn validate_range(
    index: usize,
    count: usize,
    length: usize,
    field: &str,
) -> Result<(), ReplicaError> {
    if index > length || count > length.saturating_sub(index) {
        Err(invalid(&format!("{field} is out of bounds")))
    } else {
        Ok(())
    }
}

pub(super) fn loro_failure(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::InvalidArgument(format!(
        "Loro rejected the validated CRDT operation: {error}"
    ))
}
