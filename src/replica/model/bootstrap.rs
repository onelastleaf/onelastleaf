use std::collections::{BTreeMap, BTreeSet, HashMap};

use loro::{ExportMode, LoroMap, TreeParentId};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        types::{
            ActiveReplica, EntryData, OperationKind, OperationRecord, OperationSource,
            portable_name_key,
        },
    },
    DiskSnapshot, ModelChange,
    catalog_record::{create_catalog_entry, write_entry_record},
    loro::{import_loro_doc, parse_tree_id},
    namespace::{parent_namespace_path, sorted_disk_entries, validate_name},
    revisions::recompute_live_catalog_revisions,
    support::{loro_encode_error, loro_error},
    validation::validate_loaded_replica,
};

pub(crate) fn merge_local_only_disk(
    current: &ActiveReplica,
    disk: &DiskSnapshot,
    writer_node_id: Uuid,
    correlation_id: &str,
) -> Result<ModelChange, ReplicaError> {
    let mut replica = current.clone();
    let catalog = import_loro_doc(&replica.catalog_loro, replica.loro_peer_id)?;
    catalog.set_next_commit_origin("filesystem");
    let tree = catalog.get_tree("tree");
    let entries_map = catalog.get_map("entries");
    let mut remote_occupancy = remote_portable_occupancy(&replica)?;
    for (id, path) in replica.projected_paths()? {
        remote_occupancy
            .entry(portable_namespace_key(&path)?)
            .or_default()
            .insert(id);
    }
    let mut local_parent_ids = HashMap::from([("/".to_owned(), replica.root_catalog_node_id)]);
    let mut tree_ids = replica
        .entries
        .values()
        .filter(|entry| !entry.deleted)
        .map(|entry| Ok((entry.catalog_node_id, parse_tree_id(&entry.loro_tree_id)?)))
        .collect::<Result<HashMap<_, _>, ReplicaError>>()?;
    let mut blobs = Vec::new();
    let mut created_documents = Vec::new();
    let mut changed = false;

    for disk_entry in sorted_disk_entries(disk) {
        let portable_path = portable_namespace_key(&disk_entry.namespace_path)?;
        if let Some(remote_ids) = remote_occupancy.get(&portable_path) {
            if remote_ids.len() == 1 {
                let id = *remote_ids
                    .first()
                    .expect("single-entry remote occupancy is nonempty");
                if matches!(
                    replica.entries.get(&id).map(|entry| &entry.data),
                    Some(EntryData::Directory)
                ) {
                    local_parent_ids.insert(disk_entry.namespace_path.clone(), id);
                }
            }
            continue;
        }
        let parent_path = parent_namespace_path(&disk_entry.namespace_path)?;
        let Some(parent_id) = local_parent_ids.get(parent_path).copied() else {
            continue;
        };
        let parent_tree_id = if parent_id == replica.root_catalog_node_id {
            TreeParentId::Root
        } else {
            TreeParentId::Node(*tree_ids.get(&parent_id).ok_or_else(|| {
                ReplicaError::CorruptStore(
                    "bootstrap catalog parent tree node is missing".to_owned(),
                )
            })?)
        };
        let tree_id = tree.create(parent_tree_id).map_err(loro_error)?;
        let catalog_node_id = Uuid::new_v4();
        tree.get_meta(tree_id)
            .map_err(loro_error)?
            .insert("catalog_node_id", catalog_node_id.to_string())
            .map_err(loro_error)?;
        let entry = create_catalog_entry(
            &mut replica,
            disk_entry,
            catalog_node_id,
            parent_id,
            tree_id,
            writer_node_id,
            &mut blobs,
        )?;
        let record = entries_map
            .insert_container(&catalog_node_id.to_string(), LoroMap::new())
            .map_err(loro_error)?;
        write_entry_record(&record, &entry)?;
        if let Some(document) = entry.document() {
            created_documents.push((catalog_node_id, document.document_id));
        }
        if matches!(entry.data, EntryData::Directory) {
            local_parent_ids.insert(disk_entry.namespace_path.clone(), catalog_node_id);
        }
        tree_ids.insert(catalog_node_id, tree_id);
        replica.entries.insert(catalog_node_id, entry);
        changed = true;
    }

    if changed {
        catalog.commit();
        replica.catalog_loro = catalog
            .export(ExportMode::Snapshot)
            .map_err(loro_encode_error)?;
    }
    let paths = replica.projected_paths()?;
    recompute_live_catalog_revisions(&mut replica, &paths);
    validate_loaded_replica(&replica)?;
    let timestamp = time::OffsetDateTime::now_utc();
    let operations = created_documents
        .into_iter()
        .map(|(catalog_node_id, document_id)| OperationRecord {
            timestamp,
            operation_id: Uuid::new_v4().to_string(),
            source: OperationSource::Filesystem,
            kind: OperationKind::Create,
            catalog_node_id,
            document_id,
            path_before: None,
            path_after: paths.get(&catalog_node_id).cloned(),
            correlation_id: correlation_id.to_owned(),
        })
        .collect();
    Ok(ModelChange {
        replica,
        blobs,
        operations,
        projection_paths: Vec::new(),
        changed,
    })
}

fn remote_portable_occupancy(
    replica: &ActiveReplica,
) -> Result<BTreeMap<String, BTreeSet<Uuid>>, ReplicaError> {
    let mut paths = HashMap::from([(replica.root_catalog_node_id, "/".to_owned())]);
    let mut pending = replica
        .entries
        .values()
        .filter(|entry| !entry.deleted)
        .collect::<Vec<_>>();
    while !pending.is_empty() {
        let before = pending.len();
        pending.retain(|entry| {
            let Some(parent) = paths.get(&entry.parent_catalog_node_id) else {
                return true;
            };
            let segment = portable_name_key(&entry.name);
            let path = if parent == "/" {
                format!("/{segment}")
            } else {
                format!("{parent}/{segment}")
            };
            paths.insert(entry.catalog_node_id, path);
            false
        });
        if pending.len() == before {
            return Err(ReplicaError::CorruptStore(
                "live catalog paths cannot be resolved during bootstrap".to_owned(),
            ));
        }
    }
    let mut occupancy = BTreeMap::<String, BTreeSet<Uuid>>::new();
    for (id, path) in paths {
        if id != replica.root_catalog_node_id {
            occupancy.entry(path).or_default().insert(id);
        }
    }
    Ok(occupancy)
}

fn portable_namespace_key(path: &str) -> Result<String, ReplicaError> {
    if path == "/" {
        return Ok("/".to_owned());
    }
    if !path.starts_with('/') {
        return Err(ReplicaError::Internal(
            "namespace path is not absolute".to_owned(),
        ));
    }
    let mut segments = Vec::new();
    for segment in path[1..].split('/') {
        validate_name(segment)?;
        segments.push(portable_name_key(segment));
    }
    Ok(format!("/{}", segments.join("/")))
}
