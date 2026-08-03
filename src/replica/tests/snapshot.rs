use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_round_trip_preserves_documents_blobs_and_replaces_one_replica() {
    let source = Deployment::new();
    fs::create_dir(source.native("/notes")).unwrap();
    fs::write(source.native("/notes/a.md"), "snapshot text").unwrap();
    fs::write(source.native("/removed.md"), "retained tombstone").unwrap();
    let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
    fs::write(source.native("/image.gif"), &binary).unwrap();
    let source_runtime = source.start().await;
    let source_document = source_runtime
        .inspect_document(&source.native("/notes/a.md"))
        .await
        .unwrap();
    let removed_document = source_runtime
        .inspect_document(&source.native("/removed.md"))
        .await
        .unwrap();
    source_runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "delete-before-snapshot".to_owned(),
                preconditions: vec![catalog_revision_precondition(&removed_document)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::DeleteNode(
                        oll::DeleteNode {
                            path: document_path("/removed.md"),
                            recursive: false,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "snapshot-test-correlation",
        )
        .await
        .unwrap();
    let mut second_binary = binary.clone();
    second_binary[6] = 2;
    second_binary.resize(1024 * 1024 + 17, 0x5a);
    fs::write(source.native("/image.gif"), &second_binary).unwrap();
    for _ in 0..50 {
        let state = source_runtime.state.read().await;
        let versions = state
            .as_ref()
            .and_then(|replica| replica.entry_at_path("/image.gif").ok().flatten())
            .and_then(|entry| entry.binary())
            .map_or(0, |binary| binary.versions.len());
        if versions == 2 {
            break;
        }
        drop(state);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        source_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap()
            .versions
            .len(),
        2
    );
    fs::write(source.native("/image-copy.gif"), &second_binary).unwrap();
    wait_for_path(&source_runtime, "/image-copy.gif").await;
    let (source_replica_id, source_peer) = {
        let state = source_runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        (replica.replica_id, replica.loro_peer_id)
    };
    let snapshot = source._directory.path().join("backup.ollsnap");
    let (_, exported_replica_id) = source_runtime
        .export_snapshot(&snapshot, "snapshot-export-correlation")
        .await
        .unwrap();
    assert_eq!(exported_replica_id, source_replica_id);
    let inspection = super::super::verify_snapshot(&snapshot).unwrap();
    assert_eq!(inspection.live_documents, 1);
    assert_eq!(inspection.tombstoned_documents, 1);
    assert_eq!(inspection.blobs, 2);
    assert!(matches!(
        source_runtime
            .export_snapshot(&snapshot, "snapshot-existing-export-correlation")
            .await,
        Err(ReplicaError::AlreadyExists(_))
    ));
    let racing_destination = source._directory.path().join("racing-backup.ollsnap");
    let (left, right) = tokio::join!(
        source_runtime.export_snapshot(&racing_destination, "snapshot-race-left-correlation"),
        source_runtime.export_snapshot(&racing_destination, "snapshot-race-right-correlation")
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(left, Err(ReplicaError::AlreadyExists(_))))
            + usize::from(matches!(right, Err(ReplicaError::AlreadyExists(_)))),
        1
    );

    let target = Deployment::new();
    fs::write(target.native("/old.md"), "old replica").unwrap();
    let target_runtime = target.start().await;
    let old_replica_id = match target_runtime.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    assert_ne!(old_replica_id, source_replica_id);
    let (_, imported_replica_id) = target_runtime
        .import_snapshot(&snapshot, "snapshot-target-import-correlation")
        .await
        .unwrap();
    assert_eq!(imported_replica_id, source_replica_id);
    assert!(!target.native("/old.md").exists());
    assert_eq!(
        fs::read_to_string(target.native("/notes/a.md")).unwrap(),
        "snapshot text"
    );
    assert_eq!(
        fs::read(target.native("/image.gif")).unwrap(),
        second_binary
    );
    assert_eq!(
        fs::read(target.native("/image-copy.gif")).unwrap(),
        second_binary
    );
    let imported_document = target_runtime
        .inspect_document(&target.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(imported_document.document_id, source_document.document_id);
    let imported_peer = target_runtime
        .state
        .read()
        .await
        .as_ref()
        .unwrap()
        .loro_peer_id;
    assert_ne!(imported_peer, source_peer);
    {
        let state = target_runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        assert_eq!(replica.documents.len(), 2);
        assert!(
            replica
                .documents
                .contains_key(&removed_document.document_id)
        );
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        assert_eq!(binary.versions.len(), 2);
        for version in binary.versions.values() {
            assert_eq!(
                target_runtime
                    .store
                    .read_blob(&version.sha256)
                    .await
                    .unwrap()
                    .len() as u64,
                version.size_bytes
            );
        }
    }
    assert!(!target.native("/removed.md").exists());

    shutdown_runtime(&target_runtime).await;
    drop(target_runtime);
    let restarted = target.start().await;
    assert_eq!(
        restarted.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_eq!(
        restarted
            .inspect_document(&target.native("/notes/a.md"))
            .await
            .unwrap()
            .document_id,
        source_document.document_id
    );
    shutdown_runtime(&restarted).await;

    let (_, same_replica_id) = source_runtime
        .import_snapshot(&snapshot, "snapshot-source-import-correlation")
        .await
        .unwrap();
    assert_eq!(same_replica_id, source_replica_id);
    assert_eq!(
        source_runtime.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_ne!(
        source_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .loro_peer_id,
        source_peer
    );
    shutdown_runtime(&source_runtime).await;
    let source_log = fs::read_to_string(source.log_dir.join("oll.log")).unwrap();
    let source_events = source_log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    for (event, correlation_id) in [
        ("snapshot_export_started", "snapshot-export-correlation"),
        ("snapshot_export_completed", "snapshot-export-correlation"),
        (
            "snapshot_export_failed",
            "snapshot-existing-export-correlation",
        ),
        (
            "snapshot_import_started",
            "snapshot-source-import-correlation",
        ),
        (
            "snapshot_import_completed",
            "snapshot-source-import-correlation",
        ),
    ] {
        assert!(source_events.iter().any(|record| {
            record["event"] == event && record["correlation_id"] == correlation_id
        }));
    }
    let target_log = fs::read_to_string(target.log_dir.join("oll.log")).unwrap();
    let target_events = target_log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    for event in ["snapshot_import_started", "snapshot_import_completed"] {
        assert!(target_events.iter().any(|record| {
            record["event"] == event
                && record["correlation_id"] == "snapshot-target-import-correlation"
        }));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_empty_snapshot_round_trips_into_an_uninitialized_slot() {
    let source = Deployment::new();
    fs::write(source.native("/temporary.md"), "retained history").unwrap();
    let source_runtime = source.start().await;
    let document = source_runtime
        .inspect_document(&source.native("/temporary.md"))
        .await
        .unwrap();
    source_runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "empty-snapshot-delete".to_owned(),
                preconditions: vec![catalog_revision_precondition(&document)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::DeleteNode(
                        oll::DeleteNode {
                            path: document_path("/temporary.md"),
                            recursive: false,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "empty-snapshot-correlation",
        )
        .await
        .unwrap();
    let replica_id = match source_runtime.status().await {
        ReplicaStatus::InitializedEmpty { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    let snapshot = source._directory.path().join("empty.ollsnap");
    source_runtime
        .export_snapshot(&snapshot, "empty-snapshot-export-correlation")
        .await
        .unwrap();
    let inspection = super::super::verify_snapshot(&snapshot).unwrap();
    assert_eq!(inspection.live_documents, 0);
    assert_eq!(inspection.tombstoned_documents, 1);

    let target = Deployment::new();
    let target_runtime = target.start().await;
    assert_eq!(target_runtime.status().await, ReplicaStatus::Uninitialized);
    let (_, imported_replica_id) = target_runtime
        .import_snapshot(&snapshot, "empty-snapshot-import-correlation")
        .await
        .unwrap();
    assert_eq!(imported_replica_id, replica_id);
    assert_eq!(
        target_runtime.status().await,
        ReplicaStatus::InitializedEmpty { replica_id }
    );
    assert!(fs::read_dir(&target.root).unwrap().next().is_none());
    assert!(
        target_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .documents
            .contains_key(&document.document_id)
    );

    shutdown_runtime(&target_runtime).await;
    shutdown_runtime(&source_runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filesystem_kind_replacement_allocates_new_stable_identity() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/changing"), "text").unwrap();
    let runtime = deployment.start().await;
    let text = runtime
        .inspect_document(&deployment.native("/changing"))
        .await
        .unwrap();

    fs::write(
        deployment.native("/changing"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();
    for _ in 0..50 {
        let state = runtime.state.read().await;
        if state
            .as_ref()
            .and_then(|replica| replica.entry_at_path("/changing").ok().flatten())
            .is_some_and(|entry| matches!(entry.data, EntryData::Binary(_)))
        {
            break;
        }
        drop(state);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let binary_identity = {
        let state = runtime.state.read().await;
        let entry = state
            .as_ref()
            .unwrap()
            .entry_at_path("/changing")
            .unwrap()
            .unwrap();
        assert_ne!(entry.catalog_node_id, text.catalog_node_id);
        entry.binary().unwrap().binary_id
    };

    fs::write(deployment.native("/changing"), "text again").unwrap();
    let replacement = wait_for_document(&runtime, &deployment.native("/changing")).await;
    assert_ne!(replacement.catalog_node_id, text.catalog_node_id);
    assert_ne!(replacement.document_id, text.document_id);
    let state = runtime.state.read().await;
    assert!(state.as_ref().unwrap().entries.values().any(|entry| {
        entry
            .binary()
            .is_some_and(|binary| binary.binary_id == binary_identity)
    }));
    drop(state);
    shutdown_runtime(&runtime).await;
}
