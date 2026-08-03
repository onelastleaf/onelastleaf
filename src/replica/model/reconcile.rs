use std::collections::{BTreeMap, BTreeSet, HashMap};

use loro::{ExportMode, LoroMap, TreeParentId, UpdateOptions};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        store::{NewBlob, NewBlobSource},
        types::{
            ActiveReplica, BinaryStamp, BinaryVersion, DocumentObject, EntryData, OperationKind,
            OperationRecord, OperationSource,
        },
    },
    DiskEntryData, DiskSnapshot, ModelChange,
    catalog_record::{create_catalog_entry, write_entry_record},
    loro::{get_entry_record, import_loro_doc, parse_tree_id},
    namespace::{entry_kind_name, parent_namespace_path, path_depth, sorted_disk_entries},
    revisions::recompute_live_catalog_revisions,
    support::{loro_encode_error, loro_error},
};

pub fn reconcile_disk(
    current: &ActiveReplica,
    disk: &DiskSnapshot,
    writer_node_id: Uuid,
    correlation_id: &str,
) -> Result<ModelChange, ReplicaError> {
    let mut replica = current.clone();
    let before_paths = current.projected_paths()?;
    let catalog = import_loro_doc(&replica.catalog_loro, replica.loro_peer_id)?;
    catalog.set_next_commit_origin("filesystem");
    let tree = catalog.get_tree("tree");
    let entries_map = catalog.get_map("entries");
    let mut blobs = Vec::new();
    let mut operations = Vec::new();
    let mut changed = false;

    let mut existing_by_path = before_paths
        .iter()
        .map(|(id, path)| (path.clone(), *id))
        .collect::<BTreeMap<_, _>>();
    let mut removals = Vec::new();
    for (path, id) in &existing_by_path {
        let entry = replica.entries.get(id).ok_or_else(|| {
            ReplicaError::CorruptStore("projected catalog entry is missing".to_owned())
        })?;
        match disk.entries.get(path) {
            None => removals.push((path.clone(), *id, false)),
            Some(disk_entry) if disk_entry.data.kind_name() != entry_kind_name(&entry.data) => {
                removals.push((path.clone(), *id, true));
            }
            Some(_) => {}
        }
    }
    removals.sort_by_key(|(path, _, _)| std::cmp::Reverse(path_depth(path)));
    for (path, id, replacement) in removals {
        let entry = replica.entries.get_mut(&id).ok_or_else(|| {
            ReplicaError::CorruptStore("catalog removal target is missing".to_owned())
        })?;
        let tree_id = parse_tree_id(&entry.loro_tree_id)?;
        if !entry.deleted {
            tree.delete(tree_id).map_err(loro_error)?;
            entry.deleted = true;
            entry.recompute_revision();
            write_entry_record(&get_entry_record(&entries_map, id)?, entry)?;
            if let Some(document) = entry.document() {
                operations.push(OperationRecord {
                    timestamp: time::OffsetDateTime::now_utc(),
                    operation_id: Uuid::new_v4().to_string(),
                    source: OperationSource::Filesystem,
                    kind: if replacement {
                        OperationKind::Replace
                    } else {
                        OperationKind::Delete
                    },
                    catalog_node_id: id,
                    document_id: document.document_id,
                    path_before: Some(path.clone()),
                    path_after: None,
                    correlation_id: correlation_id.to_owned(),
                });
            }
            changed = true;
        }
        existing_by_path.remove(&path);
    }

    for (path, id) in existing_by_path.clone() {
        let Some(disk_entry) = disk.entries.get(&path) else {
            continue;
        };
        let entry = replica.entries.get_mut(&id).ok_or_else(|| {
            ReplicaError::CorruptStore("catalog update target is missing".to_owned())
        })?;
        match (&mut entry.data, &disk_entry.data) {
            (EntryData::Directory, DiskEntryData::Directory) => {}
            (EntryData::Document(document), DiskEntryData::Text(text)) => {
                let object = replica
                    .documents
                    .get(&document.document_id)
                    .ok_or_else(|| {
                        ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
                    })?
                    .clone();
                let doc = import_loro_doc(&object.loro, replica.loro_peer_id)?;
                let content = doc.get_text("content");
                let content_changed = content.to_string() != text.text;
                let metadata_changed = document.encoding != text.encoding
                    || document.has_byte_order_mark != text.has_byte_order_mark
                    || document.media_type != text.media_type
                    || document.size_bytes != text.size_bytes;
                if content_changed || metadata_changed {
                    if content_changed {
                        doc.set_next_commit_origin("filesystem");
                        content
                            .update(&text.text, UpdateOptions::default())
                            .map_err(|_| {
                                ReplicaError::Internal(
                                    "Loro text diff exceeded its update budget".to_owned(),
                                )
                            })?;
                        doc.commit();
                        let loro = doc
                            .export(ExportMode::Snapshot)
                            .map_err(loro_encode_error)?;
                        replica.documents.insert(
                            document.document_id,
                            DocumentObject::new(document.document_id, loro),
                        );
                    }
                    document.encoding = text.encoding.clone();
                    document.has_byte_order_mark = text.has_byte_order_mark;
                    document.media_type = text.media_type.clone();
                    document.size_bytes = text.size_bytes;
                    let operation = OperationRecord {
                        timestamp: time::OffsetDateTime::now_utc(),
                        operation_id: Uuid::new_v4().to_string(),
                        source: OperationSource::Filesystem,
                        kind: OperationKind::Update,
                        catalog_node_id: id,
                        document_id: document.document_id,
                        path_before: Some(path.clone()),
                        path_after: Some(path),
                        correlation_id: correlation_id.to_owned(),
                    };
                    entry.recompute_revision();
                    write_entry_record(&get_entry_record(&entries_map, id)?, entry)?;
                    operations.push(operation);
                    changed = true;
                }
            }
            (EntryData::Binary(binary), DiskEntryData::Binary(file)) => {
                let equal = binary.winning_version().is_some_and(|(_, version)| {
                    version.sha256 == file.sha256
                        && version.size_bytes == u64::try_from(file.bytes.len()).unwrap_or(u64::MAX)
                        && version.media_type == file.media_type
                });
                if !equal {
                    replica.lamport_clock =
                        replica.lamport_clock.checked_add(1).ok_or_else(|| {
                            ReplicaError::CorruptStore("Lamport clock overflow".to_owned())
                        })?;
                    let stamp = BinaryStamp {
                        lamport_clock: replica.lamport_clock,
                        writer_node_id,
                    };
                    binary.media_type = file.media_type.clone();
                    binary.versions.insert(
                        stamp,
                        BinaryVersion {
                            sha256: file.sha256.clone(),
                            size_bytes: u64::try_from(file.bytes.len()).map_err(|_| {
                                ReplicaError::InvalidArgument("binary is too large".to_owned())
                            })?,
                            media_type: file.media_type.clone(),
                        },
                    );
                    blobs.push(NewBlob {
                        sha256: file.sha256.clone(),
                        source: NewBlobSource::Bytes(file.bytes.clone()),
                    });
                    entry.recompute_revision();
                    write_entry_record(&get_entry_record(&entries_map, id)?, entry)?;
                    changed = true;
                }
            }
            _ => {
                return Err(ReplicaError::Internal(
                    "kind replacement was not removed before update".to_owned(),
                ));
            }
        }
    }

    let mut disk_path_ids = existing_by_path;
    let current_paths = replica.projected_paths()?;
    for (id, path) in current_paths {
        if !replica.entries[&id].deleted {
            disk_path_ids.entry(path).or_insert(id);
        }
    }
    let mut tree_ids = replica
        .entries
        .values()
        .filter(|entry| !entry.deleted)
        .map(|entry| Ok((entry.catalog_node_id, parse_tree_id(&entry.loro_tree_id)?)))
        .collect::<Result<HashMap<_, _>, ReplicaError>>()?;
    for disk_entry in sorted_disk_entries(disk) {
        if disk_path_ids.contains_key(&disk_entry.namespace_path) {
            continue;
        }
        let parent_path = parent_namespace_path(&disk_entry.namespace_path)?;
        let parent_id = if parent_path == "/" {
            replica.root_catalog_node_id
        } else {
            *disk_path_ids.get(parent_path).ok_or_else(|| {
                ReplicaError::InvalidArgument(format!(
                    "working-tree parent is not a managed directory: {parent_path}"
                ))
            })?
        };
        if parent_id != replica.root_catalog_node_id
            && !matches!(
                replica.entries.get(&parent_id).map(|entry| &entry.data),
                Some(EntryData::Directory)
            )
        {
            return Err(ReplicaError::InvalidArgument(format!(
                "working-tree parent is not a directory: {parent_path}"
            )));
        }
        let tree_id = if parent_id == replica.root_catalog_node_id {
            tree.create(TreeParentId::Root)
        } else {
            tree.create(*tree_ids.get(&parent_id).ok_or_else(|| {
                ReplicaError::CorruptStore("catalog parent tree node is missing".to_owned())
            })?)
        }
        .map_err(loro_error)?;
        let id = Uuid::new_v4();
        tree.get_meta(tree_id)
            .map_err(loro_error)?
            .insert("catalog_node_id", id.to_string())
            .map_err(loro_error)?;
        let entry = create_catalog_entry(
            &mut replica,
            disk_entry,
            id,
            parent_id,
            tree_id,
            writer_node_id,
            &mut blobs,
        )?;
        let record = entries_map
            .insert_container(&id.to_string(), LoroMap::new())
            .map_err(loro_error)?;
        write_entry_record(&record, &entry)?;
        if let Some(document) = entry.document() {
            operations.push(OperationRecord {
                timestamp: time::OffsetDateTime::now_utc(),
                operation_id: Uuid::new_v4().to_string(),
                source: OperationSource::Filesystem,
                kind: OperationKind::Create,
                catalog_node_id: id,
                document_id: document.document_id,
                path_before: None,
                path_after: Some(disk_entry.namespace_path.clone()),
                correlation_id: correlation_id.to_owned(),
            });
        }
        replica.entries.insert(id, entry);
        disk_path_ids.insert(disk_entry.namespace_path.clone(), id);
        tree_ids.insert(id, tree_id);
        changed = true;
    }

    let mut projection_paths = BTreeSet::new();
    if changed {
        catalog.commit();
        replica.catalog_loro = catalog
            .export(ExportMode::Snapshot)
            .map_err(loro_encode_error)?;
        let after_paths = replica.projected_paths()?;
        recompute_live_catalog_revisions(&mut replica, &after_paths);
        for (id, before) in &before_paths {
            match after_paths.get(id) {
                Some(after) if after != before => {
                    projection_paths.insert(before.clone());
                    projection_paths.insert(after.clone());
                }
                None if disk.entries.contains_key(before) => {
                    projection_paths.insert(before.clone());
                }
                _ => {}
            }
        }
        for (id, after) in &after_paths {
            if !before_paths.contains_key(id) {
                let disk_path = disk_path_ids
                    .iter()
                    .find_map(|(path, candidate)| (*candidate == *id).then_some(path));
                if disk_path.is_some_and(|path| path != after) {
                    projection_paths.insert(after.clone());
                    projection_paths.insert(disk_path.unwrap().clone());
                }
            }
        }
        if !projection_paths.is_empty() {
            replica.projection_generation = replica
                .projection_generation
                .checked_add(1)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("projection generation overflow".to_owned())
                })?;
        }
    }

    Ok(ModelChange {
        replica,
        blobs,
        operations,
        projection_paths: projection_paths.into_iter().collect(),
        changed,
    })
}
