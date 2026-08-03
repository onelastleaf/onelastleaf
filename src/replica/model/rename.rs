use std::collections::{BTreeSet, HashMap};

use loro::{ExportMode, TreeParentId};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        types::{ActiveReplica, EntryData, OperationKind, OperationRecord, OperationSource},
    },
    ModelChange,
    catalog_record::write_entry_record,
    loro::{get_entry_record, import_loro_doc, parse_tree_id},
    namespace::{parent_namespace_path, validate_name},
    revisions::recompute_live_catalog_revisions,
    support::{loro_encode_error, loro_error},
};

pub fn apply_reliable_rename(
    current: &ActiveReplica,
    source: &str,
    destination: &str,
    correlation_id: &str,
) -> Result<Option<ModelChange>, ReplicaError> {
    let before_paths = current.projected_paths()?;
    let by_path = before_paths
        .iter()
        .map(|(id, path)| (path.as_str(), *id))
        .collect::<HashMap<_, _>>();
    let Some(target_id) = by_path.get(source).copied() else {
        return Ok(None);
    };
    if by_path.contains_key(destination) {
        return Ok(None);
    }
    let parent_path = parent_namespace_path(destination)?;
    let parent_id = if parent_path == "/" {
        current.root_catalog_node_id
    } else {
        let Some(parent_id) = by_path.get(parent_path).copied() else {
            return Ok(None);
        };
        if !matches!(
            current.entries.get(&parent_id).map(|entry| &entry.data),
            Some(EntryData::Directory)
        ) {
            return Ok(None);
        }
        parent_id
    };
    let name = destination
        .rsplit('/')
        .next()
        .ok_or_else(|| ReplicaError::InvalidArgument("rename destination is invalid".to_owned()))?;
    validate_name(name)?;

    let mut replica = current.clone();
    let catalog = import_loro_doc(&replica.catalog_loro, replica.loro_peer_id)?;
    catalog.set_next_commit_origin("filesystem");
    let tree = catalog.get_tree("tree");
    let entries_map = catalog.get_map("entries");
    let parent_tree_id = if parent_id == replica.root_catalog_node_id {
        TreeParentId::Root
    } else {
        TreeParentId::Node(parse_tree_id(
            &replica
                .entries
                .get(&parent_id)
                .ok_or_else(|| ReplicaError::CorruptStore("rename parent is missing".to_owned()))?
                .loro_tree_id,
        )?)
    };
    let entry = replica
        .entries
        .get_mut(&target_id)
        .ok_or_else(|| ReplicaError::CorruptStore("rename target is missing".to_owned()))?;
    let target_tree_id = parse_tree_id(&entry.loro_tree_id)?;
    if entry.parent_catalog_node_id != parent_id {
        tree.mov(target_tree_id, parent_tree_id)
            .map_err(loro_error)?;
    }
    entry.parent_catalog_node_id = parent_id;
    entry.name = name.to_owned();
    entry.recompute_revision();
    write_entry_record(&get_entry_record(&entries_map, target_id)?, entry)?;
    catalog.commit();
    replica.catalog_loro = catalog
        .export(ExportMode::Snapshot)
        .map_err(loro_encode_error)?;

    let after_paths = replica.projected_paths()?;
    recompute_live_catalog_revisions(&mut replica, &after_paths);
    let mut projection_paths = BTreeSet::new();
    for (id, before) in &before_paths {
        match after_paths.get(id) {
            Some(after) if after != before => {
                projection_paths.insert(before.clone());
                projection_paths.insert(after.clone());
            }
            None => {
                projection_paths.insert(before.clone());
            }
            _ => {}
        }
    }
    for (id, after) in &after_paths {
        if !before_paths.contains_key(id) {
            projection_paths.insert(after.clone());
        }
    }
    if after_paths
        .get(&target_id)
        .is_some_and(|path| path != destination)
    {
        projection_paths.insert(destination.to_owned());
    }
    if !projection_paths.is_empty() {
        replica.projection_generation =
            replica
                .projection_generation
                .checked_add(1)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("projection generation overflow".to_owned())
                })?;
    }
    let mut operations = Vec::new();
    for (id, before) in &before_paths {
        let Some(after) = after_paths.get(id) else {
            continue;
        };
        if before == after {
            continue;
        }
        let Some(document) = replica.entries[id].document() else {
            continue;
        };
        operations.push(OperationRecord {
            timestamp: time::OffsetDateTime::now_utc(),
            operation_id: Uuid::new_v4().to_string(),
            source: OperationSource::Filesystem,
            kind: OperationKind::Move,
            catalog_node_id: *id,
            document_id: document.document_id,
            path_before: Some(before.clone()),
            path_after: Some(after.clone()),
            correlation_id: correlation_id.to_owned(),
        });
    }
    Ok(Some(ModelChange {
        replica,
        blobs: Vec::new(),
        operations,
        projection_paths: projection_paths.into_iter().collect(),
        changed: true,
    }))
}
