use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::Path,
};

use tar::Archive;
use tempfile::Builder as TempBuilder;

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        model::{decode_catalog_snapshot, validate_document_snapshot},
    },
    CATALOG_ENTRY, MANIFEST_ENTRY,
    archive_read::{copy_entry, validate_regular_entry},
    manifest::{parse_manifest, validate_manifest},
    support::parse_manifest_uuid,
    types::{ManifestDocumentState, VerifiedSnapshot},
};

pub(super) fn stage_and_verify(path: &Path) -> Result<VerifiedSnapshot, ReplicaError> {
    let source =
        File::open(path).map_err(|error| ReplicaError::io("open replica snapshot", error))?;
    let buffered = BufReader::new(source);
    let mut decoder = zstd::stream::read::Decoder::with_buffer(buffered)
        .map_err(|error| ReplicaError::InvalidSnapshot(format!("invalid zstd frame: {error}")))?
        .single_frame();
    let staging = TempBuilder::new()
        .prefix("oll-snapshot-verify-")
        .tempdir()
        .map_err(|error| ReplicaError::io("create snapshot verification staging", error))?;
    let (manifest, mut staged_paths) = {
        let mut archive = Archive::new(&mut decoder);
        let mut entries = archive.entries().map_err(|error| {
            ReplicaError::InvalidSnapshot(format!("invalid tar archive: {error}"))
        })?;

        let mut manifest_entry = entries
            .next()
            .ok_or_else(|| ReplicaError::InvalidSnapshot("snapshot archive is empty".to_owned()))?
            .map_err(|error| {
                ReplicaError::InvalidSnapshot(format!("invalid tar entry: {error}"))
            })?;
        validate_regular_entry(&manifest_entry, MANIFEST_ENTRY)?;
        let mut manifest_source = Vec::new();
        manifest_entry
            .read_to_end(&mut manifest_source)
            .map_err(|error| ReplicaError::io("read snapshot manifest", error))?;
        let manifest = parse_manifest(&manifest_source)?;
        validate_manifest(&manifest)?;

        let mut expected = Vec::with_capacity(1 + manifest.documents.len() + manifest.blobs.len());
        expected.push((
            CATALOG_ENTRY.to_owned(),
            manifest.catalog.size_bytes,
            manifest.catalog.sha256.clone(),
        ));
        expected.extend(manifest.documents.iter().map(|document| {
            (
                document.entry.clone(),
                document.size_bytes,
                document.sha256.clone(),
            )
        }));
        expected.extend(
            manifest
                .blobs
                .iter()
                .map(|blob| (blob.entry.clone(), blob.size_bytes, blob.sha256.clone())),
        );

        let mut staged_paths = BTreeMap::new();
        for (expected_path, expected_size, expected_hash) in &expected {
            let mut entry = entries
                .next()
                .ok_or_else(|| {
                    ReplicaError::InvalidSnapshot(format!(
                        "snapshot entry {expected_path} is missing"
                    ))
                })?
                .map_err(|error| {
                    ReplicaError::InvalidSnapshot(format!("invalid tar entry: {error}"))
                })?;
            validate_regular_entry(&entry, expected_path)?;
            if entry.size() != *expected_size {
                return Err(ReplicaError::InvalidSnapshot(format!(
                    "snapshot entry {expected_path} has the wrong size"
                )));
            }
            let staged = staging.path().join(format!("entry-{}", staged_paths.len()));
            let actual_hash = copy_entry(&mut entry, &staged)?;
            if &actual_hash != expected_hash {
                return Err(ReplicaError::InvalidSnapshot(format!(
                    "snapshot entry {expected_path} has the wrong SHA-256"
                )));
            }
            staged_paths.insert(expected_path.clone(), staged);
        }
        if entries.next().is_some() {
            return Err(ReplicaError::InvalidSnapshot(
                "snapshot contains an undeclared trailing entry".to_owned(),
            ));
        }
        (manifest, staged_paths)
    };

    let mut trailing = Vec::new();
    decoder.read_to_end(&mut trailing).map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("invalid zstd checksum: {error}"))
    })?;
    if trailing.iter().any(|byte| *byte != 0) {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot tar contains trailing payload bytes".to_owned(),
        ));
    }
    let mut compressed = decoder.finish();
    if !compressed
        .fill_buf()
        .map_err(|error| ReplicaError::io("inspect snapshot trailing data", error))?
        .is_empty()
    {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot contains multiple zstd frames or trailing bytes".to_owned(),
        ));
    }

    let catalog_path = staged_paths
        .remove(CATALOG_ENTRY)
        .ok_or_else(|| ReplicaError::InvalidSnapshot("snapshot catalog is missing".to_owned()))?;
    let catalog_bytes = fs::read(&catalog_path)
        .map_err(|error| ReplicaError::io("read verified catalog snapshot", error))?;
    let (_, catalog_entries) = decode_catalog_snapshot(&catalog_bytes)?;
    let catalog_documents = catalog_entries
        .values()
        .filter_map(|entry| {
            entry.document().map(|document| {
                (
                    document.document_id,
                    (
                        !entry.deleted,
                        document.encoding.clone(),
                        document.has_byte_order_mark,
                        document.size_bytes,
                    ),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let referenced_blobs = catalog_entries
        .values()
        .filter_map(|entry| entry.binary())
        .flat_map(|binary| binary.versions.values())
        .map(|version| version.sha256.clone())
        .collect::<BTreeSet<_>>();

    let mut documents = BTreeMap::new();
    for declared in &manifest.documents {
        let document_id = parse_manifest_uuid(&declared.document_id, "document_id")?;
        let (expected_live, encoding, has_byte_order_mark, size_bytes) =
            catalog_documents.get(&document_id).ok_or_else(|| {
                ReplicaError::InvalidSnapshot(
                    "manifest document is not referenced by the catalog".to_owned(),
                )
            })?;
        if *expected_live != (declared.state == ManifestDocumentState::Live) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest document state contradicts the catalog".to_owned(),
            ));
        }
        let staged = staged_paths.remove(&declared.entry).ok_or_else(|| {
            ReplicaError::InvalidSnapshot("verified document entry is missing".to_owned())
        })?;
        let bytes = fs::read(&staged)
            .map_err(|error| ReplicaError::io("read verified document snapshot", error))?;
        let document = validate_document_snapshot(&bytes)?;
        let content = document.get_text("content").to_string();
        let (encoded, promoted) =
            encode_text(&content, encoding, *has_byte_order_mark).map_err(|_| {
                ReplicaError::InvalidSnapshot(
                    "catalog document encoding metadata is invalid".to_owned(),
                )
            })?;
        if promoted {
            return Err(ReplicaError::InvalidSnapshot(
                "catalog document content is not exactly representable in its declared encoding"
                    .to_owned(),
            ));
        }
        let encoded_size = u64::try_from(encoded.len())
            .map_err(|_| ReplicaError::InvalidSnapshot("document size overflow".to_owned()))?;
        if encoded_size != *size_bytes {
            return Err(ReplicaError::InvalidSnapshot(
                "catalog document size differs from its encoded content".to_owned(),
            ));
        }
        if documents.insert(document_id, staged).is_some() {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest repeats a document_id".to_owned(),
            ));
        }
    }
    if documents.len() != catalog_documents.len() {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog references an undeclared document object".to_owned(),
        ));
    }

    let declared_blobs = manifest
        .blobs
        .iter()
        .map(|blob| blob.sha256.clone())
        .collect::<BTreeSet<_>>();
    if declared_blobs != referenced_blobs {
        return Err(ReplicaError::InvalidSnapshot(
            "manifest blob set differs from catalog references".to_owned(),
        ));
    }
    let mut blobs = BTreeMap::new();
    let mut blob_sizes = BTreeMap::new();
    for declared in &manifest.blobs {
        let staged = staged_paths.remove(&declared.entry).ok_or_else(|| {
            ReplicaError::InvalidSnapshot("verified blob entry is missing".to_owned())
        })?;
        let actual_size = fs::metadata(&staged)
            .map_err(|error| ReplicaError::io("inspect verified blob", error))?
            .len();
        blob_sizes.insert(declared.sha256.clone(), actual_size);
        blobs.insert(declared.sha256.clone(), staged);
    }
    for version in catalog_entries
        .values()
        .filter_map(|entry| entry.binary())
        .flat_map(|binary| binary.versions.values())
    {
        let actual_size = blob_sizes.get(&version.sha256).ok_or_else(|| {
            ReplicaError::InvalidSnapshot("catalog references a missing blob".to_owned())
        })?;
        if *actual_size != version.size_bytes {
            return Err(ReplicaError::InvalidSnapshot(
                "catalog binary version size differs from its blob".to_owned(),
            ));
        }
    }
    if !staged_paths.is_empty() {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot staging contains undeclared entries".to_owned(),
        ));
    }
    Ok(VerifiedSnapshot {
        _staging: staging,
        manifest,
        catalog_path,
        documents,
        blobs,
    })
}
