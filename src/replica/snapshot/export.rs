use std::{
    collections::BTreeSet,
    fs::{self, File},
    io,
    path::Path,
};

use tempfile::Builder as TempBuilder;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::node::logging::LogLevel;

use super::{
    super::{ReplicaError, watcher::ReplicaRuntime},
    CATALOG_ENTRY, MANIFEST_ENTRY, SNAPSHOT_FORMAT, SNAPSHOT_FORMAT_VERSION,
    archive_write::build_archive,
    manifest::manifest_object,
    support::{elapsed_ms, hex_sha256, snapshot_failure_level},
    types::{Manifest, ManifestBlob, ManifestDocument, ManifestDocumentState},
};

#[cfg(test)]
use super::EXPORT_ARCHIVE_TEST_HOOK;

pub(crate) async fn export_runtime(
    runtime: &ReplicaRuntime,
    destination: &Path,
    correlation_id: &str,
) -> Result<(Uuid, Uuid), ReplicaError> {
    let started = std::time::Instant::now();
    runtime.logger.emit(
        LogLevel::Info,
        "oll::replica",
        "snapshot_export_started",
        correlation_id,
        serde_json::json!({}),
    );
    let result = export_runtime_inner(runtime, destination).await;
    match &result {
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

    let coordinator = runtime.identities.commit_guard().await;
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
