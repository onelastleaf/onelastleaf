use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use uuid::Uuid;

use crate::node::logging::LogLevel;

use super::{
    super::{
        ReplicaError, identity,
        model::{decode_catalog_snapshot, generate_loro_peer_id},
        store::{IdentityTransitionKind, NewBlob, NewBlobSource},
        types::{ActiveReplica, DocumentObject, OperationKind, OperationRecord, OperationSource},
        watcher::ReplicaRuntime,
    },
    support::{
        elapsed_ms, parse_manifest_uuid, snapshot_failure_level, validate_loro_and_collect_peers,
    },
    verify::stage_and_verify,
};

pub(crate) async fn import_runtime(
    runtime: &ReplicaRuntime,
    source: &Path,
    correlation_id: &str,
) -> Result<(Uuid, Uuid), ReplicaError> {
    let started = std::time::Instant::now();
    runtime.logger.emit(
        LogLevel::Info,
        "oll::replica",
        "snapshot_import_started",
        correlation_id,
        serde_json::json!({}),
    );
    let result = import_runtime_inner(runtime, source, correlation_id).await;
    match &result {
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
        .build_inactive_generation(&candidate, &blobs, &operations, &[])
        .await?;

    let _coordinator = runtime.identities.commit_guard().await;
    let expected_active = runtime
        .state
        .read()
        .await
        .as_ref()
        .map(|replica| (replica.generation_id, replica.replica_id));
    identity::activate_candidate(
        &runtime.store,
        &runtime.config_root,
        expected_active,
        &candidate,
        IdentityTransitionKind::SnapshotImport,
        true,
    )
    .await?;
    runtime.replace_state(candidate.clone()).await;
    runtime.project_complete(&candidate).await?;
    runtime
        .store
        .clear_projection_pending(candidate.generation_id)
        .await?;
    Ok((snapshot_id, replica_id))
}
