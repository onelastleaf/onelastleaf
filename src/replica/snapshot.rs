use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, EntryType, Header};
use tempfile::{Builder as TempBuilder, TempDir};
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::node::logging::LogLevel;

use super::{
    ReplicaError,
    classification::encode_text,
    model::{decode_catalog_snapshot, generate_loro_peer_id, validate_document_snapshot},
    store::{NewBlob, NewBlobSource},
    types::{
        ActiveReplica, DocumentObject, OperationKind, OperationRecord, OperationSource,
        parse_uuid_v4,
    },
    watcher::ReplicaRuntime,
};

const SNAPSHOT_FORMAT: &str = "onelastleaf-replica-snapshot";
const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const MANIFEST_ENTRY: &str = "manifest.json";
const CATALOG_ENTRY: &str = "catalog.loro";

#[cfg(test)]
struct ExportArchiveTestHook {
    destination: PathBuf,
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static EXPORT_ARCHIVE_TEST_HOOK: std::sync::Mutex<Option<ExportArchiveTestHook>> =
    std::sync::Mutex::new(None);

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
struct Manifest {
    format: String,
    format_version: u32,
    snapshot_id: String,
    replica_id: String,
    created_at: String,
    catalog: ManifestObject,
    documents: Vec<ManifestDocument>,
    blobs: Vec<ManifestBlob>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestObject {
    entry: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ManifestDocumentState {
    Live,
    Tombstoned,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestDocument {
    document_id: String,
    state: ManifestDocumentState,
    entry: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestBlob {
    entry: String,
    size_bytes: u64,
    sha256: String,
}

struct VerifiedSnapshot {
    _staging: TempDir,
    manifest: Manifest,
    catalog_path: PathBuf,
    documents: BTreeMap<Uuid, PathBuf>,
    blobs: BTreeMap<String, PathBuf>,
}

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

pub(crate) async fn export_runtime(
    runtime: &ReplicaRuntime,
    destination: &Path,
    correlation_id: &str,
) -> Result<(Uuid, Uuid), ReplicaError> {
    let started = std::time::Instant::now();
    runtime
        .logger
        .emit(
            LogLevel::Info,
            "oll::replica",
            "snapshot_export_started",
            correlation_id,
            serde_json::json!({}),
        )
        .map_err(|error| ReplicaError::Internal(error.to_string()))?;
    let result = export_runtime_inner(runtime, destination).await;
    let log_result = match &result {
        Ok((snapshot_id, replica_id)) => runtime.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "snapshot_export_completed",
            correlation_id,
            serde_json::json!({
                "snapshot_id": snapshot_id.to_string(),
                "replica_id": replica_id.to_string(),
                "duration_ms": elapsed_ms(started),
            }),
        ),
        Err(error) => runtime.logger.emit(
            snapshot_failure_level(error),
            "oll::replica",
            "snapshot_export_failed",
            correlation_id,
            serde_json::json!({
                "error_code": error.code(),
                "duration_ms": elapsed_ms(started),
            }),
        ),
    };
    if result.is_ok() {
        log_result.map_err(|error| ReplicaError::Internal(error.to_string()))?;
    }
    result
}

async fn export_runtime_inner(
    runtime: &ReplicaRuntime,
    destination: &Path,
) -> Result<(Uuid, Uuid), ReplicaError> {
    if destination.exists() {
        return Err(ReplicaError::AlreadyExists(
            "snapshot destination already exists".to_owned(),
        ));
    }
    let parent = destination.parent().ok_or_else(|| {
        ReplicaError::InvalidArgument("snapshot destination has no parent".to_owned())
    })?;
    if !parent.is_dir() {
        return Err(ReplicaError::InvalidArgument(
            "snapshot destination parent is not a directory".to_owned(),
        ));
    }

    let coordinator = runtime.coordinator.lock().await;
    let replica = runtime
        .state
        .read()
        .await
        .clone()
        .ok_or(ReplicaError::Uninitialized)?;
    let staging = TempBuilder::new()
        .prefix(".oll-export-")
        .tempdir_in(parent)
        .map_err(|error| ReplicaError::io("create snapshot staging directory", error))?;
    let catalog_path = staging.path().join(CATALOG_ENTRY);
    fs::write(&catalog_path, &replica.catalog_loro)
        .map_err(|error| ReplicaError::io("stage catalog snapshot", error))?;
    let catalog_object = manifest_object(CATALOG_ENTRY, &replica.catalog_loro)?;

    let documents_dir = staging.path().join("documents");
    let blobs_dir = staging.path().join("blobs");
    fs::create_dir(&documents_dir)
        .map_err(|error| ReplicaError::io("create document staging directory", error))?;
    fs::create_dir(&blobs_dir)
        .map_err(|error| ReplicaError::io("create blob staging directory", error))?;

    let live_document_ids = replica
        .entries
        .values()
        .filter_map(|entry| {
            (!entry.deleted)
                .then(|| entry.document().map(|document| document.document_id))
                .flatten()
        })
        .collect::<BTreeSet<_>>();
    let mut documents = Vec::new();
    for (document_id, document) in &replica.documents {
        let entry = format!("documents/{document_id}.loro");
        let path = documents_dir.join(format!("{document_id}.loro"));
        fs::write(&path, &document.loro)
            .map_err(|error| ReplicaError::io("stage document snapshot", error))?;
        documents.push(ManifestDocument {
            document_id: document_id.to_string(),
            state: if live_document_ids.contains(document_id) {
                ManifestDocumentState::Live
            } else {
                ManifestDocumentState::Tombstoned
            },
            entry,
            size_bytes: u64::try_from(document.loro.len())
                .map_err(|_| ReplicaError::Internal("document size overflow".to_owned()))?,
            sha256: hex_sha256(&document.loro),
        });
    }

    let blob_hashes = replica
        .entries
        .values()
        .filter_map(|entry| entry.binary())
        .flat_map(|binary| binary.versions.values())
        .map(|version| version.sha256.clone())
        .collect::<BTreeSet<_>>();
    let mut blobs = Vec::new();
    for sha256 in blob_hashes {
        let path = blobs_dir.join(&sha256);
        runtime.store.write_blob_to_path(&sha256, &path).await?;
        let size_bytes = runtime.store.blob_size(&sha256).await?;
        blobs.push(ManifestBlob {
            entry: format!("blobs/{sha256}"),
            size_bytes,
            sha256,
        });
    }

    let snapshot_id = Uuid::new_v4();
    let manifest = Manifest {
        format: SNAPSHOT_FORMAT.to_owned(),
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id: snapshot_id.to_string(),
        replica_id: replica.replica_id.to_string(),
        created_at: time::OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| ReplicaError::Internal("cannot format snapshot time".to_owned()))?,
        catalog: catalog_object,
        documents,
        blobs,
    };
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|_| ReplicaError::Internal("cannot serialize snapshot manifest".to_owned()))?;
    manifest_bytes.push(b'\n');
    let manifest_path = staging.path().join(MANIFEST_ENTRY);
    fs::write(&manifest_path, &manifest_bytes)
        .map_err(|error| ReplicaError::io("stage snapshot manifest", error))?;

    let mut archive_temporary = TempBuilder::new()
        .prefix(".oll-snapshot-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| ReplicaError::io("create snapshot temporary file", error))?;
    let archive_manifest_path = manifest_path.clone();
    let archive_catalog_path = catalog_path.clone();
    let archive_manifest = manifest.clone();
    let archive_staging_root = staging.path().to_owned();
    #[cfg(test)]
    let archive_test_hook = {
        let mut hook = EXPORT_ARCHIVE_TEST_HOOK.lock().unwrap();
        if hook
            .as_ref()
            .is_some_and(|hook| hook.destination == destination)
        {
            hook.take()
        } else {
            None
        }
    };
    let task = tokio::task::spawn_blocking(move || -> Result<_, ReplicaError> {
        #[cfg(test)]
        if let Some(hook) = archive_test_hook {
            let _ = hook.started.send(());
            let _ = hook.release.recv();
        }
        build_archive(
            archive_temporary.as_file_mut(),
            &archive_manifest_path,
            &archive_catalog_path,
            &archive_manifest,
            &archive_staging_root,
        )?;
        Ok(archive_temporary)
    })
    .await;
    let temporary = match task {
        Ok(result) => result?,
        Err(error) => {
            return Err(ReplicaError::Internal(format!(
                "snapshot archive task failed: {error}"
            )));
        }
    };
    drop(coordinator);
    match fs::hard_link(temporary.path(), destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(ReplicaError::AlreadyExists(
                "snapshot destination already exists".to_owned(),
            ));
        }
        Err(error) => {
            return Err(ReplicaError::io("publish replica snapshot", error));
        }
    }
    temporary
        .close()
        .map_err(|error| ReplicaError::io("remove snapshot temporary link", error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io("sync snapshot destination directory", error))?;
    Ok((snapshot_id, replica.replica_id))
}

pub(crate) async fn import_runtime(
    runtime: &ReplicaRuntime,
    source: &Path,
    correlation_id: &str,
) -> Result<(Uuid, Uuid), ReplicaError> {
    let started = std::time::Instant::now();
    runtime
        .logger
        .emit(
            LogLevel::Info,
            "oll::replica",
            "snapshot_import_started",
            correlation_id,
            serde_json::json!({}),
        )
        .map_err(|error| ReplicaError::Internal(error.to_string()))?;
    let result = import_runtime_inner(runtime, source, correlation_id).await;
    let log_result = match &result {
        Ok((snapshot_id, replica_id)) => runtime.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "snapshot_import_completed",
            correlation_id,
            serde_json::json!({
                "snapshot_id": snapshot_id.to_string(),
                "replica_id": replica_id.to_string(),
                "duration_ms": elapsed_ms(started),
            }),
        ),
        Err(error) => runtime.logger.emit(
            snapshot_failure_level(error),
            "oll::replica",
            "snapshot_import_failed",
            correlation_id,
            serde_json::json!({
                "error_code": error.code(),
                "duration_ms": elapsed_ms(started),
            }),
        ),
    };
    if result.is_ok() {
        log_result.map_err(|error| ReplicaError::Internal(error.to_string()))?;
    }
    result
}

async fn import_runtime_inner(
    runtime: &ReplicaRuntime,
    source: &Path,
    correlation_id: &str,
) -> Result<(Uuid, Uuid), ReplicaError> {
    let source = source.to_owned();
    let verified = tokio::task::spawn_blocking(move || stage_and_verify(&source))
        .await
        .map_err(|error| {
            ReplicaError::Internal(format!("snapshot verification task failed: {error}"))
        })??;
    let snapshot_id = parse_manifest_uuid(&verified.manifest.snapshot_id, "snapshot_id")?;
    let replica_id = parse_manifest_uuid(&verified.manifest.replica_id, "replica_id")?;
    let catalog_bytes = fs::read(&verified.catalog_path)
        .map_err(|error| ReplicaError::io("read staged catalog", error))?;
    let (root_catalog_node_id, entries) = decode_catalog_snapshot(&catalog_bytes)?;
    let mut excluded_peers = BTreeSet::new();
    let catalog_doc = validate_loro_and_collect_peers(&catalog_bytes, &mut excluded_peers)?;
    drop(catalog_doc);

    let mut documents = BTreeMap::new();
    for (document_id, path) in &verified.documents {
        let bytes = fs::read(path)
            .map_err(|error| ReplicaError::io("read staged document snapshot", error))?;
        let doc = validate_loro_and_collect_peers(&bytes, &mut excluded_peers)?;
        let _content = doc.get_text("content");
        let _data = doc.get_map("data");
        documents.insert(*document_id, DocumentObject::new(*document_id, bytes));
    }
    let peer = generate_loro_peer_id(&excluded_peers)?;
    let lamport_clock = entries
        .values()
        .filter_map(|entry| entry.binary())
        .flat_map(|binary| binary.versions.keys())
        .map(|stamp| stamp.lamport_clock)
        .max()
        .unwrap_or(0);
    let mut candidate = ActiveReplica {
        generation_id: Uuid::new_v4(),
        replica_id,
        loro_peer_id: peer,
        root_catalog_node_id,
        catalog_loro: catalog_bytes,
        lamport_clock,
        projection_generation: 1,
        entries,
        documents,
    };
    let paths = candidate.projected_paths()?;
    for entry in candidate.entries.values_mut() {
        if let Some(path) = paths.get(&entry.catalog_node_id) {
            entry.recompute_revision_at_path(path);
        } else {
            entry.recompute_revision();
        }
    }
    let blobs = verified
        .blobs
        .iter()
        .map(|(sha256, path)| {
            let size_bytes = verified
                .manifest
                .blobs
                .iter()
                .find(|blob| &blob.sha256 == sha256)
                .map(|blob| blob.size_bytes)
                .ok_or_else(|| {
                    ReplicaError::InvalidSnapshot("staged blob is undeclared".to_owned())
                })?;
            Ok(NewBlob {
                sha256: sha256.clone(),
                source: NewBlobSource::File {
                    path: path.clone(),
                    size_bytes,
                },
            })
        })
        .collect::<Result<Vec<_>, ReplicaError>>()?;
    let operations = candidate
        .entries
        .values()
        .filter_map(|entry| {
            entry.document().map(|document| OperationRecord {
                timestamp: time::OffsetDateTime::now_utc(),
                operation_id: Uuid::new_v4().to_string(),
                source: OperationSource::SnapshotImport,
                kind: OperationKind::Replace,
                catalog_node_id: entry.catalog_node_id,
                document_id: document.document_id,
                path_before: None,
                path_after: paths.get(&entry.catalog_node_id).cloned(),
                correlation_id: correlation_id.to_owned(),
            })
        })
        .collect::<Vec<_>>();
    runtime
        .store
        .build_inactive_generation(&candidate, &blobs, &operations)
        .await?;

    let _coordinator = runtime.coordinator.lock().await;
    let expected_active = runtime
        .state
        .read()
        .await
        .as_ref()
        .map(|replica| replica.generation_id);
    runtime
        .store
        .activate_generation(expected_active, candidate.generation_id)
        .await?;
    runtime.replace_state(candidate.clone()).await;
    runtime.project_complete(&candidate).await?;
    runtime
        .store
        .clear_projection_pending(candidate.generation_id)
        .await?;
    Ok((snapshot_id, replica_id))
}

fn stage_and_verify(path: &Path) -> Result<VerifiedSnapshot, ReplicaError> {
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

fn validate_manifest(manifest: &Manifest) -> Result<(), ReplicaError> {
    if manifest.format != SNAPSHOT_FORMAT {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot format marker is not recognized".to_owned(),
        ));
    }
    if manifest.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot format_version is not supported".to_owned(),
        ));
    }
    parse_manifest_uuid(&manifest.snapshot_id, "snapshot_id")?;
    parse_manifest_uuid(&manifest.replica_id, "replica_id")?;
    let created = time::OffsetDateTime::parse(&manifest.created_at, &Rfc3339).map_err(|_| {
        ReplicaError::InvalidSnapshot("created_at is not an RFC 3339 timestamp".to_owned())
    })?;
    if created.offset() != time::UtcOffset::UTC {
        return Err(ReplicaError::InvalidSnapshot(
            "created_at must use UTC".to_owned(),
        ));
    }
    if manifest.catalog.entry != CATALOG_ENTRY {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog entry must be catalog.loro".to_owned(),
        ));
    }
    validate_sha256(&manifest.catalog.sha256)?;

    let mut previous_document = None;
    let mut document_ids = BTreeSet::new();
    for document in &manifest.documents {
        let id = parse_manifest_uuid(&document.document_id, "document_id")?;
        if !document_ids.insert(id) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest repeats a document_id".to_owned(),
            ));
        }
        if previous_document.is_some_and(|previous| previous >= id) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest documents are not in canonical order".to_owned(),
            ));
        }
        previous_document = Some(id);
        if document.entry != format!("documents/{id}.loro") {
            return Err(ReplicaError::InvalidSnapshot(
                "document archive entry does not match its DocumentId".to_owned(),
            ));
        }
        validate_sha256(&document.sha256)?;
    }

    let mut previous_blob: Option<&str> = None;
    let mut hashes = BTreeSet::new();
    for blob in &manifest.blobs {
        validate_sha256(&blob.sha256)?;
        if !hashes.insert(blob.sha256.as_str()) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest repeats a blob hash".to_owned(),
            ));
        }
        if previous_blob.is_some_and(|previous| previous >= blob.sha256.as_str()) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest blobs are not in canonical order".to_owned(),
            ));
        }
        previous_blob = Some(&blob.sha256);
        if blob.entry != format!("blobs/{}", blob.sha256) {
            return Err(ReplicaError::InvalidSnapshot(
                "blob archive entry does not match its SHA-256".to_owned(),
            ));
        }
    }
    Ok(())
}

fn inspection(manifest: &Manifest) -> Result<SnapshotInspection, ReplicaError> {
    validate_manifest(manifest)?;
    let mut live_documents = 0_u64;
    let mut tombstoned_documents = 0_u64;
    let mut document_bytes = 0_u64;
    for document in &manifest.documents {
        match document.state {
            ManifestDocumentState::Live => {
                live_documents = live_documents.checked_add(1).ok_or_else(size_overflow)?;
            }
            ManifestDocumentState::Tombstoned => {
                tombstoned_documents = tombstoned_documents
                    .checked_add(1)
                    .ok_or_else(size_overflow)?;
            }
        }
        document_bytes = document_bytes
            .checked_add(document.size_bytes)
            .ok_or_else(size_overflow)?;
    }
    let mut blob_bytes = 0_u64;
    for blob in &manifest.blobs {
        blob_bytes = blob_bytes
            .checked_add(blob.size_bytes)
            .ok_or_else(size_overflow)?;
    }
    Ok(SnapshotInspection {
        format: manifest.format.clone(),
        format_version: manifest.format_version,
        snapshot_id: manifest.snapshot_id.clone(),
        replica_id: manifest.replica_id.clone(),
        created_at: manifest.created_at.clone(),
        live_documents,
        tombstoned_documents,
        blobs: u64::try_from(manifest.blobs.len()).map_err(|_| size_overflow())?,
        catalog_bytes: manifest.catalog.size_bytes,
        document_bytes,
        blob_bytes,
    })
}

fn build_archive(
    output: &mut File,
    manifest_path: &Path,
    catalog_path: &Path,
    manifest: &Manifest,
    staging_root: &Path,
) -> Result<(), ReplicaError> {
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)
        .map_err(|error| ReplicaError::io("initialize zstd encoder", error))?;
    encoder
        .include_checksum(true)
        .map_err(|error| ReplicaError::io("enable zstd checksum", error))?;
    {
        let mut archive = Builder::new(&mut encoder);
        append_archive_file(&mut archive, MANIFEST_ENTRY, manifest_path)?;
        append_archive_file(&mut archive, CATALOG_ENTRY, catalog_path)?;
        for document in &manifest.documents {
            append_archive_file(
                &mut archive,
                &document.entry,
                &staging_root.join(&document.entry),
            )?;
        }
        for blob in &manifest.blobs {
            append_archive_file(&mut archive, &blob.entry, &staging_root.join(&blob.entry))?;
        }
        archive
            .finish()
            .map_err(|error| ReplicaError::io("finish tar archive", error))?;
    }
    let output = encoder
        .finish()
        .map_err(|error| ReplicaError::io("finish zstd frame", error))?;
    output
        .sync_all()
        .map_err(|error| ReplicaError::io("sync snapshot temporary file", error))
}

fn append_archive_file<W: Write>(
    archive: &mut Builder<W>,
    entry_path: &str,
    source_path: &Path,
) -> Result<(), ReplicaError> {
    let mut source = File::open(source_path)
        .map_err(|error| ReplicaError::io("open staged snapshot entry", error))?;
    let size = source
        .metadata()
        .map_err(|error| ReplicaError::io("inspect staged snapshot entry", error))?
        .len();
    let mut header = Header::new_ustar();
    header.set_entry_type(EntryType::Regular);
    header.set_size(size);
    header.set_mode(0o600);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header
        .set_username("")
        .map_err(|error| ReplicaError::Internal(format!("cannot normalize tar owner: {error}")))?;
    header
        .set_groupname("")
        .map_err(|error| ReplicaError::Internal(format!("cannot normalize tar group: {error}")))?;
    header.set_cksum();
    archive
        .append_data(&mut header, entry_path, &mut source)
        .map_err(|error| ReplicaError::io("append snapshot archive entry", error))
}

fn validate_regular_entry<R: Read>(
    entry: &tar::Entry<'_, R>,
    expected_path: &str,
) -> Result<(), ReplicaError> {
    if !entry.header().entry_type().is_file() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "snapshot entry {expected_path} is not a regular file"
        )));
    }
    let path = entry.path_bytes();
    if path.as_ref() != expected_path.as_bytes() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "expected snapshot entry {expected_path}"
        )));
    }
    Ok(())
}

fn copy_entry<R: Read>(
    entry: &mut tar::Entry<'_, R>,
    destination: &Path,
) -> Result<String, ReplicaError> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .map_err(|error| ReplicaError::io("create staged snapshot entry", error))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = entry
            .read(&mut buffer)
            .map_err(|error| ReplicaError::io("read snapshot archive entry", error))?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        output
            .write_all(&buffer[..count])
            .map_err(|error| ReplicaError::io("write staged snapshot entry", error))?;
    }
    output
        .sync_all()
        .map_err(|error| ReplicaError::io("sync staged snapshot entry", error))?;
    Ok(super::lower_hex(&hash.finalize()))
}

fn parse_manifest(source: &[u8]) -> Result<Manifest, ReplicaError> {
    serde_json::from_slice(source).map_err(|_| {
        ReplicaError::InvalidSnapshot("manifest.json is not strict versioned JSON".to_owned())
    })
}

fn manifest_object(entry: &str, bytes: &[u8]) -> Result<ManifestObject, ReplicaError> {
    Ok(ManifestObject {
        entry: entry.to_owned(),
        size_bytes: u64::try_from(bytes.len())
            .map_err(|_| ReplicaError::Internal("snapshot entry size overflow".to_owned()))?,
        sha256: hex_sha256(bytes),
    })
}

fn parse_manifest_uuid(value: &str, field: &'static str) -> Result<Uuid, ReplicaError> {
    parse_uuid_v4(value, field)
        .map_err(|_| ReplicaError::InvalidSnapshot(format!("{field} is not a canonical UUID v4")))
}

fn validate_sha256(value: &str) -> Result<(), ReplicaError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplicaError::InvalidSnapshot(
            "SHA-256 must be lower-case 64-character hex".to_owned(),
        ));
    }
    Ok(())
}

fn validate_loro_and_collect_peers(
    bytes: &[u8],
    peers: &mut BTreeSet<u64>,
) -> Result<loro::LoroDoc, ReplicaError> {
    let doc = loro::LoroDoc::new();
    doc.import(bytes).map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("Loro snapshot cannot be decoded: {error}"))
    })?;
    peers.extend(doc.oplog_vv().keys().copied());
    Ok(doc)
}

fn hex_sha256(bytes: &[u8]) -> String {
    super::lower_hex(&Sha256::digest(bytes))
}

fn size_overflow() -> ReplicaError {
    ReplicaError::InvalidSnapshot("snapshot manifest size total overflows u64".to_owned())
}

fn snapshot_failure_level(error: &ReplicaError) -> LogLevel {
    if matches!(
        error,
        ReplicaError::Uninitialized
            | ReplicaError::InvalidArgument(_)
            | ReplicaError::NotFound(_)
            | ReplicaError::AlreadyExists(_)
            | ReplicaError::RevisionConflict(_)
            | ReplicaError::InvalidSnapshot(_)
    ) {
        LogLevel::Warn
    } else {
        LogLevel::Error
    }
}

fn elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use super::*;
    use crate::{
        configuration::ReplicaStoreConfig,
        node::{NodeIdentity, logging::NodeLogger},
        replica::{
            model::{
                get_entry_record, import_loro_doc, initialize_from_disk, scan_working_tree,
                write_entry_record,
            },
            types::EntryData,
        },
    };

    #[derive(Clone)]
    struct TestArchiveEntry {
        path: Vec<u8>,
        entry_type: EntryType,
        body: Vec<u8>,
        link_name: Option<String>,
    }

    impl TestArchiveEntry {
        fn regular(path: impl Into<String>, body: Vec<u8>) -> Self {
            Self {
                path: path.into().into_bytes(),
                entry_type: EntryType::Regular,
                body,
                link_name: None,
            }
        }

        fn typed(path: impl Into<String>, entry_type: EntryType) -> Self {
            Self {
                path: path.into().into_bytes(),
                entry_type,
                body: Vec::new(),
                link_name: (entry_type == EntryType::Symlink).then(|| "target".to_owned()),
            }
        }
    }

    #[derive(Clone)]
    struct TestSnapshot {
        manifest: Manifest,
        payloads: BTreeMap<String, Vec<u8>>,
    }

    impl TestSnapshot {
        fn entries(&self) -> Vec<TestArchiveEntry> {
            let mut entries = Vec::new();
            entries.push(TestArchiveEntry::regular(
                CATALOG_ENTRY,
                self.payloads[CATALOG_ENTRY].clone(),
            ));
            entries.extend(self.manifest.documents.iter().map(|document| {
                TestArchiveEntry::regular(
                    document.entry.clone(),
                    self.payloads[&document.entry].clone(),
                )
            }));
            entries.extend(self.manifest.blobs.iter().map(|blob| {
                TestArchiveEntry::regular(blob.entry.clone(), self.payloads[&blob.entry].clone())
            }));
            entries
        }

        fn mutate_catalog(
            &mut self,
            mutate: impl FnOnce(&mut BTreeMap<Uuid, super::super::types::CatalogEntry>),
        ) {
            let source = self.payloads[CATALOG_ENTRY].clone();
            let (_, mut entries) = decode_catalog_snapshot(&source).unwrap();
            mutate(&mut entries);
            let catalog = import_loro_doc(&source, 17).unwrap();
            catalog.set_next_commit_origin("snapshot_test");
            let records = catalog.get_map("entries");
            for entry in entries.values() {
                write_entry_record(
                    &get_entry_record(&records, entry.catalog_node_id).unwrap(),
                    entry,
                )
                .unwrap();
            }
            catalog.commit();
            let encoded = catalog.export(loro::ExportMode::Snapshot).unwrap();
            self.manifest.catalog.size_bytes = encoded.len() as u64;
            self.manifest.catalog.sha256 = hex_sha256(&encoded);
            self.payloads.insert(CATALOG_ENTRY.to_owned(), encoded);
        }

        fn replace_first_document_text(&mut self, text: &str) {
            let declared = self.manifest.documents.first_mut().unwrap();
            let source = self.payloads[&declared.entry].clone();
            let document = import_loro_doc(&source, 19).unwrap();
            document.set_next_commit_origin("snapshot_test");
            document
                .get_text("content")
                .update(text, loro::UpdateOptions::default())
                .unwrap();
            document.commit();
            let encoded = document.export(loro::ExportMode::Snapshot).unwrap();
            declared.size_bytes = encoded.len() as u64;
            declared.sha256 = hex_sha256(&encoded);
            self.payloads.insert(declared.entry.clone(), encoded);
        }
    }

    fn test_snapshot() -> TestSnapshot {
        let working = TempDir::new().unwrap();
        fs::write(working.path().join("a.md"), "snapshot document").unwrap();
        let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff";
        fs::write(working.path().join("image.gif"), binary).unwrap();
        let disk = scan_working_tree(working.path()).unwrap();
        let change =
            initialize_from_disk(&disk, Uuid::new_v4(), "snapshot-fixture-correlation").unwrap();
        let replica = change.replica;

        let documents = replica
            .documents
            .iter()
            .map(|(document_id, document)| ManifestDocument {
                document_id: document_id.to_string(),
                state: ManifestDocumentState::Live,
                entry: format!("documents/{document_id}.loro"),
                size_bytes: document.loro.len() as u64,
                sha256: hex_sha256(&document.loro),
            })
            .collect::<Vec<_>>();
        let mut payloads = BTreeMap::from([(CATALOG_ENTRY.to_owned(), replica.catalog_loro)]);
        for (document, object) in documents.iter().zip(replica.documents.values()) {
            payloads.insert(document.entry.clone(), object.loro.clone());
        }

        let mut blobs = Vec::new();
        for blob in change.blobs {
            let NewBlobSource::Bytes(bytes) = blob.source else {
                panic!("initial scan fixture must retain blob bytes");
            };
            let entry = format!("blobs/{}", blob.sha256);
            blobs.push(ManifestBlob {
                entry: entry.clone(),
                size_bytes: bytes.len() as u64,
                sha256: blob.sha256,
            });
            payloads.insert(entry, bytes);
        }
        blobs.sort_by(|left, right| left.sha256.cmp(&right.sha256));

        let catalog = &payloads[CATALOG_ENTRY];
        TestSnapshot {
            manifest: Manifest {
                format: SNAPSHOT_FORMAT.to_owned(),
                format_version: SNAPSHOT_FORMAT_VERSION,
                snapshot_id: Uuid::new_v4().to_string(),
                replica_id: replica.replica_id.to_string(),
                created_at: "2026-01-01T00:00:00Z".to_owned(),
                catalog: ManifestObject {
                    entry: CATALOG_ENTRY.to_owned(),
                    size_bytes: catalog.len() as u64,
                    sha256: hex_sha256(catalog),
                },
                documents,
                blobs,
            },
            payloads,
        }
    }

    fn manifest_source(manifest: &Manifest) -> Vec<u8> {
        let mut source = serde_json::to_vec_pretty(manifest).unwrap();
        source.push(b'\n');
        source
    }

    fn write_test_archive(path: &Path, manifest: Vec<u8>, entries: &[TestArchiveEntry]) {
        let output = File::create(path).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(output, 1).unwrap();
        encoder.include_checksum(true).unwrap();
        {
            let mut archive = Builder::new(&mut encoder);
            append_test_entry(
                &mut archive,
                &TestArchiveEntry::regular(MANIFEST_ENTRY, manifest),
            );
            for entry in entries {
                append_test_entry(&mut archive, entry);
            }
            archive.finish().unwrap();
        }
        encoder.finish().unwrap();
    }

    fn append_test_entry<W: Write>(archive: &mut Builder<W>, entry: &TestArchiveEntry) {
        assert!(entry.path.len() < 100);
        let mut header = Header::new_ustar();
        header.as_mut_bytes()[..100].fill(0);
        header.as_mut_bytes()[..entry.path.len()].copy_from_slice(&entry.path);
        header.set_entry_type(entry.entry_type);
        header.set_size(if entry.entry_type.is_file() {
            entry.body.len() as u64
        } else {
            0
        });
        header.set_mode(0o600);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        if let Some(link_name) = &entry.link_name {
            header.set_link_name(link_name).unwrap();
        }
        header.set_cksum();
        archive
            .append(&header, Cursor::new(entry.body.clone()))
            .unwrap();
    }

    fn assert_invalid(
        directory: &TempDir,
        name: &str,
        manifest: Vec<u8>,
        entries: Vec<TestArchiveEntry>,
    ) {
        let path = directory.path().join(name);
        write_test_archive(&path, manifest, &entries);
        assert!(
            matches!(
                verify_snapshot(&path),
                Err(ReplicaError::InvalidSnapshot(_))
            ),
            "{name} unexpectedly verified"
        );
    }

    #[test]
    fn strict_manifest_rejects_unknown_and_duplicate_fields() {
        let duplicate = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format":"onelastleaf-replica-snapshot"
        }"#;
        assert!(parse_manifest(duplicate).is_err());

        let unknown = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":1,
          "snapshot_id":"00000000-0000-4000-8000-000000000001",
          "replica_id":"00000000-0000-4000-8000-000000000002",
          "created_at":"2026-01-01T00:00:00Z",
          "catalog":{"entry":"catalog.loro","size_bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"},
          "documents":[],
          "blobs":[],
          "unknown":true
        }"#;
        assert!(parse_manifest(unknown).is_err());

        let wrong_type = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":"1"
        }"#;
        assert!(parse_manifest(wrong_type).is_err());
    }

    #[test]
    fn archive_contract_rejects_order_set_type_size_and_hash_violations() {
        let directory = TempDir::new().unwrap();
        let fixture = test_snapshot();
        let canonical = fixture.entries();
        let source = manifest_source(&fixture.manifest);
        let valid = directory.path().join("valid.ollsnap");
        write_test_archive(&valid, source.clone(), &canonical);
        verify_snapshot(&valid).unwrap();

        let mut wrong_order = canonical.clone();
        wrong_order.swap(0, 1);
        assert_invalid(
            &directory,
            "wrong-order.ollsnap",
            source.clone(),
            wrong_order,
        );

        let mut duplicate = canonical.clone();
        duplicate.insert(1, canonical[0].clone());
        assert_invalid(
            &directory,
            "duplicate-entry.ollsnap",
            source.clone(),
            duplicate,
        );

        let mut undeclared = canonical.clone();
        undeclared.push(TestArchiveEntry::regular("extra", b"extra".to_vec()));
        assert_invalid(
            &directory,
            "undeclared-entry.ollsnap",
            source.clone(),
            undeclared,
        );

        for (name, entry_type) in [
            ("link-entry.ollsnap", EntryType::Symlink),
            ("special-entry.ollsnap", EntryType::Fifo),
        ] {
            let mut entries = canonical.clone();
            entries[0] = TestArchiveEntry::typed(CATALOG_ENTRY, entry_type);
            assert_invalid(&directory, name, source.clone(), entries);
        }

        let mut wrong_size = fixture.manifest.clone();
        wrong_size.catalog.size_bytes += 1;
        assert_invalid(
            &directory,
            "wrong-size.ollsnap",
            manifest_source(&wrong_size),
            canonical.clone(),
        );

        let mut wrong_hash = fixture.manifest.clone();
        wrong_hash.catalog.sha256 = hex_sha256(b"wrong catalog");
        assert_invalid(
            &directory,
            "wrong-hash.ollsnap",
            manifest_source(&wrong_hash),
            canonical,
        );
    }

    #[test]
    fn archive_contract_rejects_reference_schema_and_loro_violations() {
        let directory = TempDir::new().unwrap();
        let fixture = test_snapshot();

        let mut missing_document = fixture.clone();
        missing_document.manifest.documents.clear();
        let entries = missing_document.entries();
        assert_invalid(
            &directory,
            "missing-document-reference.ollsnap",
            manifest_source(&missing_document.manifest),
            entries,
        );

        let mut missing_blob = fixture.clone();
        missing_blob.manifest.blobs.clear();
        let entries = missing_blob.entries();
        assert_invalid(
            &directory,
            "missing-blob-reference.ollsnap",
            manifest_source(&missing_blob.manifest),
            entries,
        );

        let mut extra_blob = fixture.clone();
        let bytes = b"unreferenced blob".to_vec();
        let sha256 = hex_sha256(&bytes);
        let entry = format!("blobs/{sha256}");
        extra_blob.manifest.blobs.push(ManifestBlob {
            entry: entry.clone(),
            size_bytes: bytes.len() as u64,
            sha256,
        });
        extra_blob
            .manifest
            .blobs
            .sort_by(|left, right| left.sha256.cmp(&right.sha256));
        extra_blob.payloads.insert(entry, bytes);
        let entries = extra_blob.entries();
        assert_invalid(
            &directory,
            "extra-blob.ollsnap",
            manifest_source(&extra_blob.manifest),
            entries,
        );

        let wrong_catalog = {
            let doc = loro::LoroDoc::new();
            doc.set_peer_id(7).unwrap();
            doc.get_map("wrong").insert("field", 1).unwrap();
            doc.commit();
            doc.export(loro::ExportMode::Snapshot).unwrap()
        };
        let mut invalid_catalog = fixture.clone();
        invalid_catalog.manifest.catalog.size_bytes = wrong_catalog.len() as u64;
        invalid_catalog.manifest.catalog.sha256 = hex_sha256(&wrong_catalog);
        invalid_catalog
            .payloads
            .insert(CATALOG_ENTRY.to_owned(), wrong_catalog);
        let entries = invalid_catalog.entries();
        assert_invalid(
            &directory,
            "invalid-catalog-schema.ollsnap",
            manifest_source(&invalid_catalog.manifest),
            entries,
        );

        let wrong_document = {
            let doc = loro::LoroDoc::new();
            doc.set_peer_id(8).unwrap();
            doc.get_map("content").insert("wrong", true).unwrap();
            doc.get_map("data");
            doc.commit();
            doc.export(loro::ExportMode::Snapshot).unwrap()
        };
        let mut invalid_document = fixture.clone();
        let declared = invalid_document.manifest.documents.first_mut().unwrap();
        declared.size_bytes = wrong_document.len() as u64;
        declared.sha256 = hex_sha256(&wrong_document);
        invalid_document
            .payloads
            .insert(declared.entry.clone(), wrong_document);
        let entries = invalid_document.entries();
        assert_invalid(
            &directory,
            "invalid-document-schema.ollsnap",
            manifest_source(&invalid_document.manifest),
            entries,
        );

        let undecodable = b"not a Loro snapshot".to_vec();
        let mut invalid_loro = fixture;
        invalid_loro.manifest.catalog.size_bytes = undecodable.len() as u64;
        invalid_loro.manifest.catalog.sha256 = hex_sha256(&undecodable);
        invalid_loro
            .payloads
            .insert(CATALOG_ENTRY.to_owned(), undecodable);
        let entries = invalid_loro.entries();
        assert_invalid(
            &directory,
            "undecodable-loro.ollsnap",
            manifest_source(&invalid_loro.manifest),
            entries,
        );
    }

    #[test]
    fn archive_contract_validates_catalog_document_and_binary_payload_sizes() {
        let directory = TempDir::new().unwrap();
        let fixture = test_snapshot();

        let mut wrong_document_size = fixture.clone();
        wrong_document_size.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.size_bytes += 1;
        });
        assert_invalid(
            &directory,
            "catalog-document-size.ollsnap",
            manifest_source(&wrong_document_size.manifest),
            wrong_document_size.entries(),
        );

        let mut wrong_binary_size = fixture.clone();
        wrong_binary_size.mutate_catalog(|entries| {
            let version = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Binary(binary) => binary.versions.values_mut().next(),
                    _ => None,
                })
                .unwrap();
            version.size_bytes += 1;
        });
        assert_invalid(
            &directory,
            "catalog-binary-size.ollsnap",
            manifest_source(&wrong_binary_size.manifest),
            wrong_binary_size.entries(),
        );

        let mut utf16 = fixture.clone();
        utf16.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.encoding = "UTF-16LE".to_owned();
            document.has_byte_order_mark = true;
            document.size_bytes = encode_text("snapshot document", "UTF-16LE", true)
                .unwrap()
                .0
                .len() as u64;
        });
        let valid_utf16 = directory.path().join("valid-utf16-bom.ollsnap");
        write_test_archive(
            &valid_utf16,
            manifest_source(&utf16.manifest),
            &utf16.entries(),
        );
        verify_snapshot(&valid_utf16).unwrap();

        let mut wrong_utf16_size = utf16;
        wrong_utf16_size.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.size_bytes += 1;
        });
        assert_invalid(
            &directory,
            "wrong-utf16-bom-size.ollsnap",
            manifest_source(&wrong_utf16_size.manifest),
            wrong_utf16_size.entries(),
        );

        let mut invalid_bom = fixture.clone();
        invalid_bom.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.encoding = "windows-1252".to_owned();
            document.has_byte_order_mark = true;
        });
        assert_invalid(
            &directory,
            "invalid-bom-encoding.ollsnap",
            manifest_source(&invalid_bom.manifest),
            invalid_bom.entries(),
        );

        let mut unrepresentable = fixture;
        unrepresentable.replace_first_document_text("snapshot 🍃");
        unrepresentable.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.encoding = "windows-1252".to_owned();
            document.has_byte_order_mark = false;
            document.size_bytes = "snapshot 🍃".len() as u64;
        });
        assert_invalid(
            &directory,
            "unrepresentable-document.ollsnap",
            manifest_source(&unrepresentable.manifest),
            unrepresentable.entries(),
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_catalog_payload_metadata_cannot_replace_the_active_replica() {
        let directory = TempDir::new().unwrap();
        let mut fixture = test_snapshot();
        fixture.mutate_catalog(|entries| {
            let document = entries
                .values_mut()
                .find_map(|entry| match &mut entry.data {
                    EntryData::Document(document) => Some(document),
                    _ => None,
                })
                .unwrap();
            document.size_bytes += 1;
        });
        let snapshot = directory.path().join("invalid-metadata.ollsnap");
        write_test_archive(
            &snapshot,
            manifest_source(&fixture.manifest),
            &fixture.entries(),
        );

        let root = directory.path().join("working");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("original.md"), "authoritative").unwrap();
        let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
        let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
        let runtime = ReplicaRuntime::start(
            root.clone(),
            &ReplicaStoreConfig::Sqlite {
                path: directory.path().join("store/replica.sqlite3"),
            },
            identity.node_id(),
            logger,
        )
        .await
        .unwrap();
        let before = runtime.status().await;

        assert!(matches!(
            runtime
                .import_snapshot(&snapshot, "invalid-import-correlation")
                .await,
            Err(ReplicaError::InvalidSnapshot(_))
        ));
        assert_eq!(runtime.status().await, before);
        assert_eq!(
            fs::read_to_string(root.join("original.md")).unwrap(),
            "authoritative"
        );
        runtime
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_export_cleans_its_owned_temporary_archive() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("working");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("document.md"), "snapshot content").unwrap();
        let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
        let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
        let runtime = ReplicaRuntime::start(
            root,
            &ReplicaStoreConfig::Sqlite {
                path: directory.path().join("store/replica.sqlite3"),
            },
            identity.node_id(),
            logger,
        )
        .await
        .unwrap();
        let destination = directory.path().join("cancelled.ollsnap");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *EXPORT_ARCHIVE_TEST_HOOK.lock().unwrap() = Some(ExportArchiveTestHook {
            destination: destination.clone(),
            started: started_tx,
            release: release_rx,
        });

        let export_runtime = Arc::clone(&runtime);
        let export_destination = destination.clone();
        let export = tokio::spawn(async move {
            export_runtime
                .export_snapshot(&export_destination, "cancelled-export-correlation")
                .await
        });
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
            .await
            .unwrap();
        assert!(directory.path().read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".oll-snapshot-")
        }));

        export.abort();
        assert!(export.await.unwrap_err().is_cancelled());
        assert!(!destination.exists());
        release_tx.send(()).unwrap();
        for _ in 0..100 {
            let has_temporary = directory.path().read_dir().unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".oll-snapshot-")
            });
            if !has_temporary {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!directory.path().read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".oll-snapshot-")
        }));

        runtime
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[test]
    fn archive_contract_rejects_zstd_corruption_and_trailing_data() {
        let directory = TempDir::new().unwrap();
        let fixture = test_snapshot();
        let valid = directory.path().join("valid.ollsnap");
        write_test_archive(
            &valid,
            manifest_source(&fixture.manifest),
            &fixture.entries(),
        );
        verify_snapshot(&valid).unwrap();

        let mut corrupted = fs::read(&valid).unwrap();
        let final_byte = corrupted.last_mut().unwrap();
        *final_byte ^= 0xff;
        let corrupt_path = directory.path().join("corrupt.ollsnap");
        fs::write(&corrupt_path, corrupted).unwrap();
        assert!(matches!(
            verify_snapshot(&corrupt_path),
            Err(ReplicaError::InvalidSnapshot(_))
        ));

        let trailing_path = directory.path().join("trailing.ollsnap");
        fs::copy(&valid, &trailing_path).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&trailing_path)
            .unwrap()
            .write_all(b"trailing payload")
            .unwrap();
        assert!(matches!(
            verify_snapshot(&trailing_path),
            Err(ReplicaError::InvalidSnapshot(_))
        ));
    }

    #[test]
    fn malformed_archive_path_cannot_escape_verification_staging() {
        let directory = TempDir::new().unwrap();
        let snapshot = directory.path().join("malicious.ollsnap");
        let escaped = directory.path().join("escaped");
        let manifest = Manifest {
            format: SNAPSHOT_FORMAT.to_owned(),
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id: Uuid::new_v4().to_string(),
            replica_id: Uuid::new_v4().to_string(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            catalog: ManifestObject {
                entry: CATALOG_ENTRY.to_owned(),
                size_bytes: 0,
                sha256: hex_sha256(&[]),
            },
            documents: Vec::new(),
            blobs: Vec::new(),
        };
        let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        manifest_bytes.push(b'\n');

        let output = File::create(&snapshot).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(output, 1).unwrap();
        {
            let mut archive = Builder::new(&mut encoder);
            let mut header = Header::new_ustar();
            header.set_entry_type(EntryType::Regular);
            header.set_size(manifest_bytes.len() as u64);
            header.set_mode(0o600);
            header.set_path(MANIFEST_ENTRY).unwrap();
            header.set_cksum();
            archive
                .append(&header, Cursor::new(manifest_bytes))
                .unwrap();

            let malicious_path = escaped.as_os_str().as_encoded_bytes();
            assert!(malicious_path.len() < 100);
            let mut header = Header::new_old();
            header.set_entry_type(EntryType::Regular);
            header.set_size(0);
            header.set_mode(0o600);
            header.as_mut_bytes()[..100].fill(0);
            header.as_mut_bytes()[..malicious_path.len()].copy_from_slice(malicious_path);
            header.set_cksum();
            archive
                .append(&header, Cursor::new(Vec::<u8>::new()))
                .unwrap();
            archive.finish().unwrap();
        }
        encoder.finish().unwrap();

        assert!(matches!(
            verify_snapshot(&snapshot),
            Err(ReplicaError::InvalidSnapshot(_))
        ));
        assert!(!escaped.exists());

        let fixture = test_snapshot();
        let mut entries = fixture.entries();
        entries[0].path = b"../catalog.loro".to_vec();
        assert_invalid(
            &directory,
            "traversal.ollsnap",
            manifest_source(&fixture.manifest),
            entries,
        );
    }
}
