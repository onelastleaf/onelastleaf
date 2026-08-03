use std::collections::{BTreeMap, BTreeSet, HashMap};

use loro::{Container, ContainerType, TreeParentId, ValueOrContainer};
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        classification::is_supported_text_encoding,
        types::{
            ActiveReplica, BinaryEntry, CATALOG_FORMAT_VERSION, CatalogEntry, DocumentEntry,
            EntryData, parse_uuid_v4,
        },
    },
    catalog_schema::{
        decode_binary_versions, invalid_snapshot, require_exact_map_fields, validate_root_schema,
    },
    loro::{import_loro_doc, map_bool, map_i64, map_string, map_u64_string},
    namespace::validate_name,
    support::{loro_error, snapshot_from_store_error},
};

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
                    if !is_supported_text_encoding(&encoding) {
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
