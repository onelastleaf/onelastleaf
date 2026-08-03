use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotInspection {
    pub format: String,
    pub format_version: u32,
    pub snapshot_id: String,
    pub replica_id: String,
    pub created_at: String,
    pub live_documents: u64,
    pub tombstoned_documents: u64,
    pub blobs: u64,
    pub catalog_bytes: u64,
    pub document_bytes: u64,
    pub blob_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub format: String,
    pub format_version: u32,
    pub snapshot_id: String,
    pub replica_id: String,
    pub created_at: String,
    pub catalog: ManifestObject,
    pub documents: Vec<ManifestDocument>,
    pub blobs: Vec<ManifestBlob>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestObject {
    pub entry: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ManifestDocumentState {
    Live,
    Tombstoned,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestDocument {
    pub document_id: String,
    pub state: ManifestDocumentState,
    pub entry: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestBlob {
    pub entry: String,
    pub size_bytes: u64,
    pub sha256: String,
}

pub(super) struct VerifiedSnapshot {
    pub _staging: TempDir,
    pub manifest: Manifest,
    pub catalog_path: PathBuf,
    pub documents: BTreeMap<Uuid, PathBuf>,
    pub blobs: BTreeMap<String, PathBuf>,
}
