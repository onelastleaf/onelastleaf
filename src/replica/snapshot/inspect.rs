use std::{fs::File, io::Read, path::Path};

use tar::Archive;

use super::{
    super::ReplicaError,
    MANIFEST_ENTRY,
    archive_read::validate_regular_entry,
    manifest::{inspection, parse_manifest},
    types::SnapshotInspection,
    verify::stage_and_verify,
};

pub fn inspect_snapshot(path: &Path) -> Result<SnapshotInspection, ReplicaError> {
    let file =
        File::open(path).map_err(|error| ReplicaError::io("open replica snapshot", error))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(|error| ReplicaError::InvalidSnapshot(format!("invalid zstd frame: {error}")))?;
    let mut archive = Archive::new(decoder);
    let mut entries = archive
        .entries()
        .map_err(|error| ReplicaError::InvalidSnapshot(format!("invalid tar archive: {error}")))?;
    let mut first = entries
        .next()
        .ok_or_else(|| ReplicaError::InvalidSnapshot("snapshot archive is empty".to_owned()))?
        .map_err(|error| ReplicaError::InvalidSnapshot(format!("invalid tar entry: {error}")))?;
    validate_regular_entry(&first, MANIFEST_ENTRY)?;
    let mut source = Vec::new();
    first
        .read_to_end(&mut source)
        .map_err(|error| ReplicaError::io("read snapshot manifest", error))?;
    let manifest = parse_manifest(&source)?;
    inspection(&manifest)
}

pub fn verify_snapshot(path: &Path) -> Result<SnapshotInspection, ReplicaError> {
    let verified = stage_and_verify(path)?;
    inspection(&verified.manifest)
}
