use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Component, Path},
};

use encoding_rs::{Encoding, UTF_8};
use getrandom::fill as fill_random;
use loro::{
    Container, ContainerType, ExportMode, LoroDoc, LoroMap, LoroValue, TreeID, TreeParentId,
    UpdateOptions, ValueOrContainer,
};
use sha2::Digest;
use uuid::Uuid;
use walkdir::WalkDir;

use super::{
    ReplicaError,
    classification::{BinaryFile, ClassifiedFile, DecodedText, classify_path},
    store::{NewBlob, NewBlobSource},
    types::{
        ActiveReplica, BinaryEntry, BinaryStamp, BinaryVersion, CATALOG_FORMAT_VERSION,
        CatalogEntry, DocumentEntry, DocumentObject, EntryData, OperationKind, OperationRecord,
        OperationSource, parse_uuid_v4,
    },
};

#[derive(Clone, Debug)]
pub struct DiskSnapshot {
    pub entries: BTreeMap<String, DiskEntry>,
}

impl DiskSnapshot {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct DiskEntry {
    pub namespace_path: String,
    pub name: String,
    pub data: DiskEntryData,
}

#[derive(Clone, Debug)]
pub enum DiskEntryData {
    Directory,
    Text(DecodedText),
    Binary(BinaryFile),
}

impl DiskEntryData {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::Text(_) => "document",
            Self::Binary(_) => "binary",
        }
    }
}

#[derive(Debug)]
pub struct ModelChange {
    pub replica: ActiveReplica,
    pub blobs: Vec<NewBlob>,
    pub operations: Vec<OperationRecord>,
    pub projection_paths: Vec<String>,
    pub changed: bool,
}

pub fn scan_working_tree(root: &Path) -> Result<DiskSnapshot, ReplicaError> {
    let mut entries = BTreeMap::new();
    for item in WalkDir::new(root)
        .follow_links(false)
        .sort_by_file_name()
        .min_depth(1)
    {
        let item = item.map_err(|error| {
            ReplicaError::InvalidArgument(format!("cannot scan working-tree entry: {error}"))
        })?;
        let relative = item.path().strip_prefix(root).map_err(|_| {
            ReplicaError::Internal("working-tree walker escaped its root".to_owned())
        })?;
        let namespace_path = namespace_path(relative)?;
        let name = relative
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ReplicaError::InvalidArgument(
                    "working-tree entry name is not valid UTF-8".to_owned(),
                )
            })?
            .to_owned();
        let file_type = item.file_type();
        let data = if file_type.is_dir() {
            DiskEntryData::Directory
        } else if file_type.is_file() {
            match classify_path(item.path())? {
                ClassifiedFile::Text(text) => DiskEntryData::Text(text),
                ClassifiedFile::Binary(binary) => DiskEntryData::Binary(binary),
            }
        } else {
            return Err(ReplicaError::InvalidArgument(format!(
                "unsupported special working-tree entry at {namespace_path}"
            )));
        };
        let entry = DiskEntry {
            namespace_path: namespace_path.clone(),
            name,
            data,
        };
        if entries.insert(namespace_path, entry).is_some() {
            return Err(ReplicaError::InvalidArgument(
                "working-tree scan produced a duplicate path".to_owned(),
            ));
        }
    }
    Ok(DiskSnapshot { entries })
}

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

pub fn decode_catalog_snapshot(
    bytes: &[u8],
) -> Result<(Uuid, BTreeMap<Uuid, CatalogEntry>), ReplicaError> {
    let doc = import_loro_doc(bytes, 0).map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("catalog Loro snapshot cannot be decoded: {error}"))
    })?;
    validate_root_schema(
        &doc,
        &[
            ("tree", ContainerType::Tree),
            ("catalog", ContainerType::Map),
            ("entries", ContainerType::Map),
        ],
        "catalog",
    )?;
    let tree = doc
        .try_get_tree("tree")
        .ok_or_else(|| invalid_snapshot("catalog tree root is missing"))?;
    let catalog = doc
        .try_get_map("catalog")
        .ok_or_else(|| invalid_snapshot("catalog metadata root is missing"))?;
    let entries_map = doc
        .try_get_map("entries")
        .ok_or_else(|| invalid_snapshot("catalog entries root is missing"))?;
    require_exact_map_fields(
        &catalog,
        &["format_version", "root_catalog_node_id"],
        "catalog metadata",
    )?;
    let format_version = map_i64(&catalog, "format_version")?;
    if format_version != CATALOG_FORMAT_VERSION {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog has an unsupported format_version".to_owned(),
        ));
    }
    let root_catalog_node_id = parse_uuid_v4(
        &map_string(&catalog, "root_catalog_node_id")?,
        "root_catalog_node_id",
    )
    .map_err(snapshot_from_store_error)?;

    let mut tree_nodes = HashMap::new();
    for tree_id in tree.nodes() {
        let metadata = tree.get_meta(tree_id).map_err(loro_error)?;
        require_exact_map_fields(&metadata, &["catalog_node_id"], "catalog tree metadata")?;
        let catalog_node_id = parse_uuid_v4(
            &map_string(&metadata, "catalog_node_id")?,
            "catalog_node_id",
        )
        .map_err(snapshot_from_store_error)?;
        if tree_nodes.insert(catalog_node_id, tree_id).is_some() {
            return Err(ReplicaError::InvalidSnapshot(
                "catalog tree repeats a CatalogNodeId".to_owned(),
            ));
        }
    }

    let mut entries = BTreeMap::new();
    let mut decode_error = None;
    entries_map.for_each(|key, value| {
        if decode_error.is_some() {
            return;
        }
        let result = (|| {
            let id = parse_uuid_v4(key, "catalog entry key").map_err(snapshot_from_store_error)?;
            let ValueOrContainer::Container(Container::Map(record)) = value else {
                return Err(ReplicaError::InvalidSnapshot(
                    "catalog entry is not a LoroMap".to_owned(),
                ));
            };
            let catalog_node_id =
                parse_uuid_v4(&map_string(&record, "catalog_node_id")?, "catalog_node_id")
                    .map_err(snapshot_from_store_error)?;
            if id != catalog_node_id {
                return Err(ReplicaError::InvalidSnapshot(
                    "catalog entry key does not match catalog_node_id".to_owned(),
                ));
            }
            let parent_catalog_node_id = parse_uuid_v4(
                &map_string(&record, "parent_catalog_node_id")?,
                "parent_catalog_node_id",
            )
            .map_err(snapshot_from_store_error)?;
            let name = map_string(&record, "name")?;
            validate_name(&name)?;
            let deleted = map_bool(&record, "deleted")?;
            let kind = map_string(&record, "kind")?;
            let required_fields: &[&str] = match kind.as_str() {
                "directory" => &[
                    "catalog_node_id",
                    "parent_catalog_node_id",
                    "name",
                    "deleted",
                    "kind",
                ],
                "document" => &[
                    "catalog_node_id",
                    "parent_catalog_node_id",
                    "name",
                    "deleted",
                    "kind",
                    "document_id",
                    "media_type",
                    "encoding",
                    "has_byte_order_mark",
                    "size_bytes",
                ],
                "binary" => &[
                    "catalog_node_id",
                    "parent_catalog_node_id",
                    "name",
                    "deleted",
                    "kind",
                    "binary_id",
                    "media_type",
                    "binary_versions",
                ],
                _ => {
                    return Err(ReplicaError::InvalidSnapshot(
                        "catalog entry has an unknown kind".to_owned(),
                    ));
                }
            };
            require_exact_map_fields(&record, required_fields, "catalog entry")?;
            let tree_id = tree_nodes.get(&id).copied().ok_or_else(|| {
                ReplicaError::InvalidSnapshot("catalog entry has no LoroTree node".to_owned())
            })?;
            if tree.is_node_deleted(&tree_id).map_err(loro_error)? != deleted {
                return Err(ReplicaError::InvalidSnapshot(
                    "catalog entry deletion state contradicts LoroTree".to_owned(),
                ));
            }
            if !deleted {
                match tree.parent(tree_id) {
                    Some(TreeParentId::Root) if parent_catalog_node_id == root_catalog_node_id => {}
                    Some(TreeParentId::Node(parent)) => {
                        let parent_meta = tree.get_meta(parent).map_err(loro_error)?;
                        let parent_id = parse_uuid_v4(
                            &map_string(&parent_meta, "catalog_node_id")?,
                            "parent catalog_node_id",
                        )
                        .map_err(snapshot_from_store_error)?;
                        if parent_id != parent_catalog_node_id {
                            return Err(ReplicaError::InvalidSnapshot(
                                "catalog parent contradicts LoroTree".to_owned(),
                            ));
                        }
                    }
                    _ => {
                        return Err(ReplicaError::InvalidSnapshot(
                            "live catalog entry has an invalid LoroTree parent".to_owned(),
                        ));
                    }
                }
            }
            let data = match kind.as_str() {
                "directory" => EntryData::Directory,
                "document" => {
                    let encoding = map_string(&record, "encoding")?;
                    if Encoding::for_label(encoding.as_bytes()).is_none() {
                        return Err(ReplicaError::InvalidSnapshot(
                            "document entry has an unknown encoding".to_owned(),
                        ));
                    }
                    EntryData::Document(DocumentEntry {
                        document_id: parse_uuid_v4(
                            &map_string(&record, "document_id")?,
                            "document_id",
                        )
                        .map_err(snapshot_from_store_error)?,
                        media_type: map_string(&record, "media_type")?,
                        encoding,
                        has_byte_order_mark: map_bool(&record, "has_byte_order_mark")?,
                        size_bytes: map_u64_string(&record, "size_bytes")?,
                    })
                }
                "binary" => EntryData::Binary(BinaryEntry {
                    binary_id: parse_uuid_v4(&map_string(&record, "binary_id")?, "binary_id")
                        .map_err(snapshot_from_store_error)?,
                    media_type: map_string(&record, "media_type")?,
                    versions: decode_binary_versions(&record)?,
                }),
                _ => unreachable!(),
            };
            let mut entry = CatalogEntry {
                catalog_node_id,
                parent_catalog_node_id,
                loro_tree_id: tree_id.to_string(),
                name,
                deleted,
                catalog_revision: [0; 32],
                data,
            };
            entry.recompute_revision();
            if entries.insert(id, entry).is_some() {
                return Err(ReplicaError::InvalidSnapshot(
                    "catalog repeats an entry".to_owned(),
                ));
            }
            Ok(())
        })();
        if let Err(error) = result {
            decode_error = Some(error);
        }
    });
    if let Some(error) = decode_error {
        return Err(error);
    }
    if entries.len() != tree_nodes.len() {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog LoroTree contains an undeclared node".to_owned(),
        ));
    }
    if entries.contains_key(&root_catalog_node_id) {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog root is also present as an ordinary entry".to_owned(),
        ));
    }
    let mut document_ids = BTreeSet::new();
    let mut binary_ids = BTreeSet::new();
    for entry in entries.values() {
        if entry.parent_catalog_node_id != root_catalog_node_id {
            let parent = entries.get(&entry.parent_catalog_node_id).ok_or_else(|| {
                ReplicaError::InvalidSnapshot(
                    "catalog entry references a missing parent".to_owned(),
                )
            })?;
            if !matches!(parent.data, EntryData::Directory) {
                return Err(ReplicaError::InvalidSnapshot(
                    "catalog entry parent is not a directory".to_owned(),
                ));
            }
        }
        match &entry.data {
            EntryData::Directory => {}
            EntryData::Document(document) => {
                if !document_ids.insert(document.document_id) {
                    return Err(ReplicaError::InvalidSnapshot(
                        "catalog repeats a DocumentId".to_owned(),
                    ));
                }
            }
            EntryData::Binary(binary) => {
                if !binary_ids.insert(binary.binary_id) {
                    return Err(ReplicaError::InvalidSnapshot(
                        "catalog repeats a BinaryId".to_owned(),
                    ));
                }
                let (_, winning) = binary.winning_version().ok_or_else(|| {
                    ReplicaError::InvalidSnapshot(
                        "catalog binary has no retained version".to_owned(),
                    )
                })?;
                if winning.media_type != binary.media_type {
                    return Err(ReplicaError::InvalidSnapshot(
                        "catalog binary media type contradicts its winning version".to_owned(),
                    ));
                }
            }
        }
    }
    let candidate = ActiveReplica {
        generation_id: Uuid::new_v4(),
        replica_id: Uuid::new_v4(),
        loro_peer_id: 0,
        root_catalog_node_id,
        catalog_loro: bytes.to_vec(),
        lamport_clock: 0,
        projection_generation: 0,
        entries: entries.clone(),
        documents: BTreeMap::new(),
    };
    candidate.projected_paths().map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("invalid catalog topology: {error}"))
    })?;
    Ok((root_catalog_node_id, entries))
}

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
        validate_document_snapshot(&document.loro).map_err(|error| {
            ReplicaError::CorruptStore(format!("stored document Loro snapshot is invalid: {error}"))
        })?;
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

pub fn normalize_imported_encodings(
    catalog_bytes: &[u8],
    peer: u64,
    entries: &mut BTreeMap<Uuid, CatalogEntry>,
    documents: &BTreeMap<Uuid, DocumentObject>,
) -> Result<Vec<u8>, ReplicaError> {
    let catalog = import_loro_doc(catalog_bytes, peer)?;
    let entries_map = catalog.get_map("entries");
    let mut changed = false;
    for entry in entries.values_mut() {
        let EntryData::Document(document) = &mut entry.data else {
            continue;
        };
        let object = documents.get(&document.document_id).ok_or_else(|| {
            ReplicaError::InvalidSnapshot("catalog references a missing document".to_owned())
        })?;
        let doc = validate_document_snapshot(&object.loro)?;
        let text = doc.get_text("content").to_string();
        let (_, promoted) = super::classification::encode_text(
            &text,
            &document.encoding,
            document.has_byte_order_mark,
        )?;
        if promoted {
            if !changed {
                catalog.set_next_commit_origin("snapshot_import");
            }
            document.encoding = UTF_8.name().to_owned();
            document.has_byte_order_mark = false;
            document.size_bytes = u64::try_from(text.len()).map_err(|_| {
                ReplicaError::InvalidSnapshot("promoted document size overflow".to_owned())
            })?;
            entry.recompute_revision();
            write_entry_record(
                &get_entry_record(&entries_map, entry.catalog_node_id)?,
                entry,
            )?;
            changed = true;
        }
    }
    if changed {
        catalog.commit();
    }
    catalog
        .export(ExportMode::Snapshot)
        .map_err(loro_encode_error)
}

pub fn generate_loro_peer_id(excluded: &BTreeSet<u64>) -> Result<u64, ReplicaError> {
    loop {
        let mut bytes = [0_u8; 8];
        fill_random(&mut bytes).map_err(|error| {
            ReplicaError::Internal(format!("cannot generate Loro peer identity: {error}"))
        })?;
        let peer = u64::from_ne_bytes(bytes);
        if peer != u64::MAX && !excluded.contains(&peer) {
            return Ok(peer);
        }
    }
}

fn create_catalog_entry(
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

fn decode_binary_versions(
    record: &LoroMap,
) -> Result<BTreeMap<BinaryStamp, BinaryVersion>, ReplicaError> {
    let Some(ValueOrContainer::Container(Container::Map(versions))) = record.get("binary_versions")
    else {
        return Err(ReplicaError::InvalidSnapshot(
            "binary entry has no binary_versions LoroMap".to_owned(),
        ));
    };
    let mut decoded = BTreeMap::new();
    let mut decode_error = None;
    versions.for_each(|key, value| {
        if decode_error.is_some() {
            return;
        }
        let result = (|| {
            let ValueOrContainer::Container(Container::Map(version)) = value else {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version is not a LoroMap".to_owned(),
                ));
            };
            require_exact_map_fields(
                &version,
                &[
                    "lamport_clock",
                    "writer_node_id",
                    "sha256",
                    "size_bytes",
                    "media_type",
                ],
                "binary version",
            )?;
            let stamp = BinaryStamp {
                lamport_clock: map_u64_string(&version, "lamport_clock")?,
                writer_node_id: parse_uuid_v4(
                    &map_string(&version, "writer_node_id")?,
                    "writer_node_id",
                )
                .map_err(snapshot_from_store_error)?,
            };
            if key != format!("{}@{}", stamp.lamport_clock, stamp.writer_node_id) {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version key contradicts its stamp".to_owned(),
                ));
            }
            let sha256 = map_string(&version, "sha256")?;
            validate_sha256(&sha256)?;
            let value = BinaryVersion {
                sha256,
                size_bytes: map_u64_string(&version, "size_bytes")?,
                media_type: map_string(&version, "media_type")?,
            };
            if decoded.insert(stamp, value).is_some() {
                return Err(ReplicaError::InvalidSnapshot(
                    "binary version stamp is duplicated".to_owned(),
                ));
            }
            Ok(())
        })();
        if let Err(error) = result {
            decode_error = Some(error);
        }
    });
    if let Some(error) = decode_error {
        return Err(error);
    }
    if decoded.is_empty() {
        return Err(ReplicaError::InvalidSnapshot(
            "binary entry has no retained version".to_owned(),
        ));
    }
    Ok(decoded)
}

fn validate_root_schema(
    doc: &LoroDoc,
    expected: &[(&str, ContainerType)],
    object: &str,
) -> Result<(), ReplicaError> {
    let value = doc.get_value();
    let roots = value.as_map().ok_or_else(|| {
        ReplicaError::InvalidSnapshot(format!("{object} Loro roots are not a map"))
    })?;
    if roots.len() != expected.len() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "{object} Loro snapshot has an unexpected root set"
        )));
    }
    for (name, expected_type) in expected {
        let Some(LoroValue::Container(container_id)) = roots.get(*name) else {
            return Err(ReplicaError::InvalidSnapshot(format!(
                "{object} Loro root {name} is missing"
            )));
        };
        if container_id.container_type() != *expected_type {
            return Err(ReplicaError::InvalidSnapshot(format!(
                "{object} Loro root {name} has the wrong container type"
            )));
        }
    }
    Ok(())
}

fn require_exact_map_fields(
    map: &LoroMap,
    expected: &[&str],
    object: &str,
) -> Result<(), ReplicaError> {
    let mut actual = BTreeSet::new();
    map.for_each(|key, _| {
        actual.insert(key.to_owned());
    });
    let expected = expected
        .iter()
        .map(|field| (*field).to_owned())
        .collect::<BTreeSet<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(ReplicaError::InvalidSnapshot(format!(
            "{object} fields do not match its schema"
        )))
    }
}

fn invalid_snapshot(message: &str) -> ReplicaError {
    ReplicaError::InvalidSnapshot(message.to_owned())
}

pub(crate) fn new_loro_doc(peer: u64) -> Result<LoroDoc, ReplicaError> {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer).map_err(loro_error)?;
    Ok(doc)
}

pub(crate) fn import_loro_doc(bytes: &[u8], peer: u64) -> Result<LoroDoc, ReplicaError> {
    let doc = new_loro_doc(peer)?;
    doc.import(bytes).map_err(loro_error)?;
    Ok(doc)
}

pub(crate) fn get_entry_record(entries: &LoroMap, id: Uuid) -> Result<LoroMap, ReplicaError> {
    match entries.get(&id.to_string()) {
        Some(ValueOrContainer::Container(Container::Map(map))) => Ok(map),
        Some(_) => Err(ReplicaError::CorruptStore(
            "catalog entry record is not a LoroMap".to_owned(),
        )),
        None => Err(ReplicaError::CorruptStore(
            "catalog entry record is missing".to_owned(),
        )),
    }
}

fn map_string(map: &LoroMap, key: &'static str) -> Result<String, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::String(value))) if !value.is_empty() => {
            Ok(value.to_string())
        }
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be a non-empty string"
        ))),
    }
}

fn map_i64(map: &LoroMap, key: &'static str) -> Result<i64, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::I64(value))) => Ok(value),
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be an integer"
        ))),
    }
}

fn map_bool(map: &LoroMap, key: &'static str) -> Result<bool, ReplicaError> {
    match map.get(key) {
        Some(ValueOrContainer::Value(LoroValue::Bool(value))) => Ok(value),
        _ => Err(ReplicaError::InvalidSnapshot(format!(
            "catalog field {key} must be boolean"
        ))),
    }
}

fn map_u64_string(map: &LoroMap, key: &'static str) -> Result<u64, ReplicaError> {
    map_string(map, key)?.parse().map_err(|_| {
        ReplicaError::InvalidSnapshot(format!("catalog field {key} must be a u64 string"))
    })
}

pub(crate) fn parse_tree_id(value: &str) -> Result<TreeID, ReplicaError> {
    TreeID::try_from(value)
        .map_err(|_| ReplicaError::CorruptStore("invalid LoroTree node ID".to_owned()))
}

fn sorted_disk_entries(disk: &DiskSnapshot) -> Vec<&DiskEntry> {
    let mut entries = disk.entries.values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        path_depth(&left.namespace_path)
            .cmp(&path_depth(&right.namespace_path))
            .then_with(|| left.namespace_path.cmp(&right.namespace_path))
    });
    entries
}

fn namespace_path(relative: &Path) -> Result<String, ReplicaError> {
    let mut segments = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Err(ReplicaError::InvalidArgument(
                "working-tree path contains a non-normal segment".to_owned(),
            ));
        };
        let segment = segment.to_str().ok_or_else(|| {
            ReplicaError::InvalidArgument(
                "working-tree path contains a non-UTF-8 segment".to_owned(),
            )
        })?;
        validate_name(segment)?;
        segments.push(segment);
    }
    if segments.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

pub fn absolute_to_namespace(root: &Path, path: &Path) -> Result<String, ReplicaError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        ReplicaError::InvalidArgument("working-tree path lies outside replica_root".to_owned())
    })?;
    namespace_path(relative)
}

pub(crate) fn validate_name(name: &str) -> Result<(), ReplicaError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains('/') || name.contains('\0') {
        return Err(ReplicaError::InvalidArgument(
            "catalog name is not a valid document-path segment".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn parent_namespace_path(path: &str) -> Result<&str, ReplicaError> {
    let index = path.rfind('/').ok_or_else(|| {
        ReplicaError::Internal("namespace path has no slash separator".to_owned())
    })?;
    if index == 0 {
        Ok("/")
    } else {
        Ok(&path[..index])
    }
}

fn path_depth(path: &str) -> usize {
    path.bytes().filter(|byte| *byte == b'/').count()
}

fn entry_kind_name(data: &EntryData) -> &'static str {
    match data {
        EntryData::Directory => "directory",
        EntryData::Document(_) => "document",
        EntryData::Binary(_) => "binary",
    }
}

pub(crate) fn recompute_live_catalog_revisions(
    replica: &mut ActiveReplica,
    paths: &HashMap<Uuid, String>,
) {
    for entry in replica.entries.values_mut() {
        if let Some(path) = paths.get(&entry.catalog_node_id) {
            entry.recompute_revision_at_path(path);
        } else {
            entry.recompute_revision();
        }
    }
}

fn validate_sha256(value: &str) -> Result<(), ReplicaError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplicaError::InvalidSnapshot(
            "SHA-256 must be 64 lower-case hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn loro_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::CorruptStore(format!("Loro operation failed: {error}"))
}

fn loro_encode_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::Internal(format!("cannot encode Loro snapshot: {error}"))
}

fn snapshot_from_store_error(error: ReplicaError) -> ReplicaError {
    ReplicaError::InvalidSnapshot(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn initial_scan_creates_fixed_loro_roots_and_stable_objects() {
        let directory = TempDir::new().unwrap();
        fs::create_dir(directory.path().join("notes")).unwrap();
        fs::File::create(directory.path().join("notes/a.md"))
            .unwrap()
            .write_all(b"hello")
            .unwrap();
        let disk = scan_working_tree(directory.path()).unwrap();
        let change = initialize_from_disk(&disk, Uuid::new_v4(), "test-correlation").unwrap();
        let catalog =
            import_loro_doc(&change.replica.catalog_loro, change.replica.loro_peer_id).unwrap();
        assert_eq!(
            map_i64(&catalog.get_map("catalog"), "format_version").unwrap(),
            1
        );
        assert_eq!(change.replica.documents.len(), 1);
        let document = change.replica.documents.values().next().unwrap();
        let doc = import_loro_doc(&document.loro, change.replica.loro_peer_id).unwrap();
        assert_eq!(doc.get_text("content").to_string(), "hello");
        assert!(doc.get_map("data").is_empty());
    }
}
