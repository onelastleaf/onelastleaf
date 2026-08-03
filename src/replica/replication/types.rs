use std::collections::BTreeMap;

use loro::{Frontiers, VersionVector};
use tempfile::TempPath;
use uuid::Uuid;

use super::super::ReplicaError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ReplicaObject {
    Catalog,
    Document(Uuid),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaObjectSummary {
    pub object: ReplicaObject,
    pub version_vector: VersionVector,
    pub frontier: Frontiers,
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicaInventory {
    pub generation_id: Uuid,
    pub state_token: [u8; 32],
    pub replica_id: Uuid,
    pub objects: Vec<ReplicaObjectSummary>,
    pub blobs: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExportedReplicaObject {
    pub payload: Vec<u8>,
    pub resulting_version_vector: VersionVector,
    pub payload_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub(crate) struct BootstrapSource {
    pub inventory: ReplicaInventory,
    pub objects: BTreeMap<ReplicaObject, ExportedReplicaObject>,
}

#[derive(Debug)]
pub(crate) struct BootstrapCandidate {
    pub claim_id: Uuid,
    pub replica_id: Uuid,
    pub object_updates: BTreeMap<ReplicaObject, Vec<u8>>,
    pub blobs: BTreeMap<String, StagedBlob>,
}

#[derive(Debug)]
pub(crate) struct ReplicationCandidate {
    pub base_generation_id: Uuid,
    pub base_state_token: [u8; 32],
    pub object_updates: BTreeMap<ReplicaObject, Vec<u8>>,
    pub blobs: BTreeMap<String, StagedBlob>,
}

#[derive(Debug)]
pub(crate) struct StagedBlob {
    pub path: TempPath,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub(crate) enum ReplicaUpdateValidationError {
    Decode,
    Import,
    Invalid(ReplicaError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicationCommit {
    AlreadySatisfied,
    Committed {
        object_count: u64,
        blob_count: u64,
        transferred_bytes: u64,
    },
}
