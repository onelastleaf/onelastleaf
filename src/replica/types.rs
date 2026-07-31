use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
};

use sha2::{Digest, Sha256};
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use super::ReplicaError;

pub const CATALOG_FORMAT_VERSION: i64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaStatus {
    Uninitialized,
    InitializedEmpty { replica_id: Uuid },
    InitializedPopulated { replica_id: Uuid },
}

#[derive(Clone, Debug)]
pub struct ActiveReplica {
    pub generation_id: Uuid,
    pub replica_id: Uuid,
    pub loro_peer_id: u64,
    pub root_catalog_node_id: Uuid,
    pub catalog_loro: Vec<u8>,
    pub lamport_clock: u64,
    pub projection_generation: u64,
    pub entries: BTreeMap<Uuid, CatalogEntry>,
    pub documents: BTreeMap<Uuid, DocumentObject>,
}

impl ActiveReplica {
    pub fn status(&self) -> ReplicaStatus {
        if self.entries.values().any(|entry| !entry.deleted) {
            ReplicaStatus::InitializedPopulated {
                replica_id: self.replica_id,
            }
        } else {
            ReplicaStatus::InitializedEmpty {
                replica_id: self.replica_id,
            }
        }
    }

    pub fn visible_count(&self) -> usize {
        self.entries.values().filter(|entry| !entry.deleted).count()
    }

    pub fn projected_paths(&self) -> Result<HashMap<Uuid, String>, ReplicaError> {
        projected_paths(self.root_catalog_node_id, &self.entries)
    }

    pub fn entry_at_path(&self, path: &str) -> Result<Option<&CatalogEntry>, ReplicaError> {
        let paths = self.projected_paths()?;
        Ok(paths
            .into_iter()
            .find_map(|(id, candidate)| (candidate == path).then(|| self.entries.get(&id)))
            .flatten())
    }
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    pub catalog_node_id: Uuid,
    pub parent_catalog_node_id: Uuid,
    pub loro_tree_id: String,
    pub name: String,
    pub deleted: bool,
    pub catalog_revision: [u8; 32],
    pub data: EntryData,
}

impl CatalogEntry {
    pub fn recompute_revision(&mut self) {
        self.recompute_revision_for_path(None);
    }

    pub fn recompute_revision_at_path(&mut self, path: &str) {
        self.recompute_revision_for_path(Some(path));
    }

    fn recompute_revision_for_path(&mut self, path: Option<&str>) {
        let mut hash = Sha256::new();
        hash_field(&mut hash, self.catalog_node_id.as_bytes());
        hash_field(&mut hash, self.parent_catalog_node_id.as_bytes());
        hash_field(&mut hash, self.name.as_bytes());
        if let Some(path) = path {
            hash_field(&mut hash, path.as_bytes());
        }
        hash_field(&mut hash, &[u8::from(self.deleted)]);
        match &self.data {
            EntryData::Directory => hash_field(&mut hash, b"directory"),
            EntryData::Document(document) => {
                hash_field(&mut hash, b"document");
                hash_field(&mut hash, document.document_id.as_bytes());
                hash_field(&mut hash, document.media_type.as_bytes());
                hash_field(&mut hash, document.encoding.as_bytes());
                hash_field(&mut hash, &[u8::from(document.has_byte_order_mark)]);
            }
            EntryData::Binary(binary) => {
                hash_field(&mut hash, b"binary");
                hash_field(&mut hash, binary.binary_id.as_bytes());
                hash_field(&mut hash, binary.media_type.as_bytes());
                for (stamp, version) in &binary.versions {
                    hash_field(&mut hash, &stamp.lamport_clock.to_be_bytes());
                    hash_field(&mut hash, stamp.writer_node_id.as_bytes());
                    hash_field(&mut hash, version.sha256.as_bytes());
                    hash_field(&mut hash, &version.size_bytes.to_be_bytes());
                    hash_field(&mut hash, version.media_type.as_bytes());
                }
            }
        }
        self.catalog_revision = hash.finalize().into();
    }

    pub fn document(&self) -> Option<&DocumentEntry> {
        match &self.data {
            EntryData::Document(document) => Some(document),
            EntryData::Directory | EntryData::Binary(_) => None,
        }
    }

    pub fn binary(&self) -> Option<&BinaryEntry> {
        match &self.data {
            EntryData::Binary(binary) => Some(binary),
            EntryData::Directory | EntryData::Document(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum EntryData {
    Directory,
    Document(DocumentEntry),
    Binary(BinaryEntry),
}

#[derive(Clone, Debug)]
pub struct DocumentEntry {
    pub document_id: Uuid,
    pub media_type: String,
    pub encoding: String,
    pub has_byte_order_mark: bool,
    pub size_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BinaryEntry {
    pub binary_id: Uuid,
    pub media_type: String,
    pub versions: BTreeMap<BinaryStamp, BinaryVersion>,
}

impl BinaryEntry {
    pub fn winning_version(&self) -> Option<(&BinaryStamp, &BinaryVersion)> {
        self.versions.last_key_value()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BinaryStamp {
    pub lamport_clock: u64,
    pub writer_node_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct BinaryVersion {
    pub sha256: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Clone, Debug)]
pub struct DocumentObject {
    pub document_id: Uuid,
    pub loro: Vec<u8>,
    pub revision: [u8; 32],
}

impl DocumentObject {
    pub fn new(document_id: Uuid, loro: Vec<u8>) -> Self {
        let revision = Sha256::digest(&loro).into();
        Self {
            document_id,
            loro,
            revision,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationSource {
    Filesystem,
    Plugin,
    Sync,
    SnapshotImport,
}

impl OperationSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filesystem => "filesystem",
            Self::Plugin => "plugin",
            Self::Sync => "sync",
            Self::SnapshotImport => "snapshot_import",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ReplicaError> {
        match value {
            "filesystem" => Ok(Self::Filesystem),
            "plugin" => Ok(Self::Plugin),
            "sync" => Ok(Self::Sync),
            "snapshot_import" => Ok(Self::SnapshotImport),
            _ => Err(ReplicaError::CorruptStore(
                "operation record has an unknown source".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Create,
    Update,
    Move,
    Delete,
    Replace,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Move => "move",
            Self::Delete => "delete",
            Self::Replace => "replace",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ReplicaError> {
        match value {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "move" => Ok(Self::Move),
            "delete" => Ok(Self::Delete),
            "replace" => Ok(Self::Replace),
            _ => Err(ReplicaError::CorruptStore(
                "operation record has an unknown kind".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OperationRecord {
    pub timestamp: time::OffsetDateTime,
    pub operation_id: String,
    pub source: OperationSource,
    pub kind: OperationKind,
    pub catalog_node_id: Uuid,
    pub document_id: Uuid,
    pub path_before: Option<String>,
    pub path_after: Option<String>,
    pub correlation_id: String,
}

pub fn parse_uuid_v4(value: &str, field: &'static str) -> Result<Uuid, ReplicaError> {
    let id = Uuid::parse_str(value)
        .map_err(|_| ReplicaError::CorruptStore(format!("{field} is not a UUID v4")))?;
    if id.get_version_num() != 4 || id.to_string() != value {
        return Err(ReplicaError::CorruptStore(format!(
            "{field} is not a canonical UUID v4"
        )));
    }
    Ok(id)
}

pub fn portable_name_key(name: &str) -> String {
    name.nfc().case_fold().collect::<String>().nfc().collect()
}

fn projected_paths(
    root_id: Uuid,
    entries: &BTreeMap<Uuid, CatalogEntry>,
) -> Result<HashMap<Uuid, String>, ReplicaError> {
    let mut children: BTreeMap<Uuid, Vec<&CatalogEntry>> = BTreeMap::new();
    for entry in entries.values().filter(|entry| !entry.deleted) {
        children
            .entry(entry.parent_catalog_node_id)
            .or_default()
            .push(entry);
    }
    for siblings in children.values_mut() {
        siblings.sort_by_key(|entry| entry.catalog_node_id);
    }

    let mut paths = HashMap::new();
    let mut visiting = BTreeSet::new();
    project_children(
        root_id,
        "/",
        &children,
        &mut paths,
        &mut visiting,
        entries.len(),
    )?;
    if paths.len() != entries.values().filter(|entry| !entry.deleted).count() {
        return Err(ReplicaError::CorruptStore(
            "catalog contains an unreachable live entry".to_owned(),
        ));
    }
    Ok(paths)
}

fn project_children(
    parent_id: Uuid,
    parent_path: &str,
    children: &BTreeMap<Uuid, Vec<&CatalogEntry>>,
    paths: &mut HashMap<Uuid, String>,
    visiting: &mut BTreeSet<Uuid>,
    maximum_depth: usize,
) -> Result<(), ReplicaError> {
    if visiting.len() > maximum_depth {
        return Err(ReplicaError::CorruptStore(
            "catalog contains a parent cycle".to_owned(),
        ));
    }
    let Some(siblings) = children.get(&parent_id) else {
        return Ok(());
    };

    let names = projected_sibling_names(siblings);
    for entry in siblings {
        if !visiting.insert(entry.catalog_node_id) {
            return Err(ReplicaError::CorruptStore(
                "catalog contains a parent cycle".to_owned(),
            ));
        }
        let name = names
            .get(&entry.catalog_node_id)
            .ok_or_else(|| ReplicaError::Internal("missing projected sibling name".to_owned()))?;
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        paths.insert(entry.catalog_node_id, path.clone());
        project_children(
            entry.catalog_node_id,
            &path,
            children,
            paths,
            visiting,
            maximum_depth,
        )?;
        visiting.remove(&entry.catalog_node_id);
    }
    Ok(())
}

fn projected_sibling_names(siblings: &[&CatalogEntry]) -> HashMap<Uuid, String> {
    let mut groups: BTreeMap<String, Vec<&CatalogEntry>> = BTreeMap::new();
    for entry in siblings {
        groups
            .entry(portable_name_key(&entry.name))
            .or_default()
            .push(*entry);
    }

    let mut names = HashMap::new();
    let mut reserved = BTreeSet::new();
    let mut generated = Vec::new();
    for group in groups.values() {
        if group.len() == 1 {
            let entry = group[0];
            names.insert(entry.catalog_node_id, entry.name.clone());
            reserved.insert(portable_name_key(&entry.name));
        } else {
            generated.extend(group.iter().copied());
        }
    }
    generated.sort_by_key(|entry| entry.catalog_node_id);
    for entry in generated {
        let suffix = entry.catalog_node_id.to_string();
        let mut candidate = format!("{}.conflict-{suffix}", entry.name);
        while !reserved.insert(portable_name_key(&candidate)) {
            candidate.push_str(".conflict-");
            candidate.push_str(&suffix);
        }
        names.insert(entry.catalog_node_id, candidate);
    }
    names
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value);
}

impl fmt::Display for ReplicaStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => formatter.write_str("uninitialized"),
            Self::InitializedEmpty { .. } => formatter.write_str("initialized_empty"),
            Self::InitializedPopulated { .. } => formatter.write_str("initialized_populated"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory(parent: Uuid, name: &str) -> CatalogEntry {
        let mut entry = CatalogEntry {
            catalog_node_id: Uuid::new_v4(),
            parent_catalog_node_id: parent,
            loro_tree_id: "1@1".to_owned(),
            name: name.to_owned(),
            deleted: false,
            catalog_revision: [0; 32],
            data: EntryData::Directory,
        };
        entry.recompute_revision();
        entry
    }

    #[test]
    fn every_same_name_conflict_receives_a_full_uuid_suffix() {
        let root = Uuid::new_v4();
        let first = directory(root, "Todo.md");
        let second = directory(root, "todo.md");
        let first_id = first.catalog_node_id;
        let second_id = second.catalog_node_id;
        let entries = BTreeMap::from([(first_id, first), (second_id, second)]);
        let paths = projected_paths(root, &entries).unwrap();

        assert_eq!(paths[&first_id], format!("/Todo.md.conflict-{first_id}"));
        assert_eq!(paths[&second_id], format!("/todo.md.conflict-{second_id}"));
    }

    #[test]
    fn object_ids_accept_only_canonical_uuid_v4_values() {
        let id = Uuid::new_v4();
        assert_eq!(parse_uuid_v4(&id.to_string(), "id").unwrap(), id);
        assert!(parse_uuid_v4(&id.to_string().to_ascii_uppercase(), "id").is_err());
        assert!(parse_uuid_v4(&Uuid::nil().to_string(), "id").is_err());
        assert!(parse_uuid_v4("not-a-uuid", "id").is_err());
    }

    #[test]
    fn binary_lww_orders_by_lamport_then_writer_node_id() {
        let lower_writer = Uuid::from_u128(1);
        let higher_writer = Uuid::from_u128(2);
        let versions = BTreeMap::from([
            (
                BinaryStamp {
                    lamport_clock: 9,
                    writer_node_id: higher_writer,
                },
                BinaryVersion {
                    sha256: "older".to_owned(),
                    size_bytes: 1,
                    media_type: "application/octet-stream".to_owned(),
                },
            ),
            (
                BinaryStamp {
                    lamport_clock: 10,
                    writer_node_id: lower_writer,
                },
                BinaryVersion {
                    sha256: "newer-lamport".to_owned(),
                    size_bytes: 2,
                    media_type: "application/octet-stream".to_owned(),
                },
            ),
            (
                BinaryStamp {
                    lamport_clock: 10,
                    writer_node_id: higher_writer,
                },
                BinaryVersion {
                    sha256: "writer-tiebreak".to_owned(),
                    size_bytes: 3,
                    media_type: "application/octet-stream".to_owned(),
                },
            ),
        ]);
        let binary = BinaryEntry {
            binary_id: Uuid::new_v4(),
            media_type: "application/octet-stream".to_owned(),
            versions,
        };

        let (stamp, version) = binary.winning_version().unwrap();
        assert_eq!(stamp.lamport_clock, 10);
        assert_eq!(stamp.writer_node_id, higher_writer);
        assert_eq!(version.sha256, "writer-tiebreak");
    }
}
