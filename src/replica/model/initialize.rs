use std::collections::{BTreeMap, BTreeSet, HashMap};

use loro::{ExportMode, LoroMap, TreeID, TreeParentId};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        types::{
            ActiveReplica, CATALOG_FORMAT_VERSION, OperationKind, OperationRecord, OperationSource,
        },
    },
    DiskSnapshot, ModelChange,
    catalog_record::{create_catalog_entry, write_entry_record},
    loro::{generate_loro_peer_id, new_loro_doc},
    namespace::{parent_namespace_path, sorted_disk_entries},
    revisions::recompute_live_catalog_revisions,
    support::{loro_encode_error, loro_error},
};

pub fn initialize_from_disk(
    disk: &DiskSnapshot,
    writer_node_id: Uuid,
    correlation_id: &str,
) -> Result<ModelChange, ReplicaError> {
    if disk.is_empty() {
        return Err(ReplicaError::InvalidArgument(
            "cannot initialize an empty working tree".to_owned(),
        ));
    }
    let loro_peer_id = generate_loro_peer_id(&BTreeSet::new())?;
    let replica_id = Uuid::new_v4();
    let root_catalog_node_id = Uuid::new_v4();
    let generation_id = Uuid::new_v4();
    let catalog = new_loro_doc(loro_peer_id)?;
    catalog.set_next_commit_origin("filesystem");
    let tree = catalog.get_tree("tree");
    let catalog_meta = catalog.get_map("catalog");
    let entries_map = catalog.get_map("entries");
    catalog_meta
        .insert("format_version", CATALOG_FORMAT_VERSION)
        .map_err(loro_error)?;
    catalog_meta
        .insert("root_catalog_node_id", root_catalog_node_id.to_string())
        .map_err(loro_error)?;

    let mut replica = ActiveReplica {
        generation_id,
        replica_id,
        loro_peer_id,
        root_catalog_node_id,
        catalog_loro: Vec::new(),
        lamport_clock: 0,
        projection_generation: 0,
        entries: BTreeMap::new(),
        documents: BTreeMap::new(),
    };
    let mut blobs = Vec::new();
    let mut path_ids = HashMap::from([("/".to_owned(), root_catalog_node_id)]);
    let mut tree_ids = HashMap::<Uuid, TreeID>::new();

    for disk_entry in sorted_disk_entries(disk) {
        let parent_path = parent_namespace_path(&disk_entry.namespace_path)?;
        let parent_id = path_ids.get(parent_path).copied().ok_or_else(|| {
            ReplicaError::InvalidArgument(format!(
                "working-tree parent is missing for {}",
                disk_entry.namespace_path
            ))
        })?;
        let parent_tree_id = if parent_id == root_catalog_node_id {
            TreeParentId::Root
        } else {
            TreeParentId::Node(*tree_ids.get(&parent_id).ok_or_else(|| {
                ReplicaError::Internal("catalog parent has no LoroTree node".to_owned())
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
        path_ids.insert(disk_entry.namespace_path.clone(), catalog_node_id);
        tree_ids.insert(catalog_node_id, tree_id);
        replica.entries.insert(catalog_node_id, entry);
    }
    catalog.commit();
    replica.catalog_loro = catalog
        .export(ExportMode::Snapshot)
        .map_err(loro_encode_error)?;

    let paths = replica.projected_paths()?;
    recompute_live_catalog_revisions(&mut replica, &paths);
    let timestamp = time::OffsetDateTime::now_utc();
    let operations = replica
        .entries
        .values()
        .filter_map(|entry| {
            entry.document().map(|document| OperationRecord {
                timestamp,
                operation_id: Uuid::new_v4().to_string(),
                source: OperationSource::Filesystem,
                kind: OperationKind::Create,
                catalog_node_id: entry.catalog_node_id,
                document_id: document.document_id,
                path_before: None,
                path_after: paths.get(&entry.catalog_node_id).cloned(),
                correlation_id: correlation_id.to_owned(),
            })
        })
        .collect();
    let disk_paths = disk.entries.keys().cloned().collect::<BTreeSet<_>>();
    let projected_paths = paths.into_values().collect::<BTreeSet<_>>();
    let projection_paths = disk_paths
        .symmetric_difference(&projected_paths)
        .cloned()
        .collect::<Vec<_>>();
    if !projection_paths.is_empty() {
        replica.projection_generation = 1;
    }
    Ok(ModelChange {
        replica,
        blobs,
        operations,
        projection_paths,
        changed: true,
    })
}
