use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_claim_atomically_imports_remote_state_and_merges_only_local_paths() {
    let source = Deployment::new();
    fs::write(source.native("/remote.md"), "remote only").unwrap();
    fs::write(source.native("/collision.md"), "remote wins").unwrap();
    fs::write(
        source.native("/image.gif"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();
    let source_runtime = source.start().await;
    let source_replica_id = match source_runtime.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected source state: {state:?}"),
    };
    let source_peer_id = source_runtime
        .state
        .read()
        .await
        .as_ref()
        .unwrap()
        .loro_peer_id;
    let source_data = source_runtime.capture_bootstrap_source().await.unwrap();
    let mut blobs = std::collections::BTreeMap::new();
    for (sha256, size_bytes) in &source_data.inventory.blobs {
        let staged = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            staged.path(),
            source_runtime.store.read_blob(sha256).await.unwrap(),
        )
        .unwrap();
        blobs.insert(
            sha256.clone(),
            StagedBlob {
                path: staged.into_temp_path(),
                size_bytes: *size_bytes,
            },
        );
    }

    let target = Deployment::new();
    let target_runtime = target.start().await;
    assert_eq!(target_runtime.status().await, ReplicaStatus::Uninitialized);
    let claim = BootstrapClaim {
        claim_id: Uuid::new_v4(),
        source_node_id: source.identity.node_id(),
        correlation_id: "bootstrap-regression-correlation".to_owned(),
    };
    assert!(
        target_runtime
            .acquire_bootstrap_claim(&claim)
            .await
            .unwrap()
    );
    assert!(
        !target_runtime
            .acquire_bootstrap_claim(&BootstrapClaim {
                claim_id: Uuid::new_v4(),
                source_node_id: Uuid::new_v4(),
                correlation_id: "losing-bootstrap-correlation".to_owned(),
            })
            .await
            .unwrap()
    );
    let guard = target_runtime.identities.commit_guard_owned().await;
    fs::write(target.native("/local.md"), "local only").unwrap();
    fs::write(target.native("/collision.md"), "local loses").unwrap();

    let commit = target_runtime
        .commit_bootstrap_candidate(
            BootstrapCandidate {
                claim_id: claim.claim_id,
                replica_id: source_replica_id,
                object_updates: source_data
                    .objects
                    .into_iter()
                    .map(|(object, exported)| (object, exported.payload))
                    .collect(),
                blobs,
            },
            &guard,
            target.identity.node_id(),
            &claim.correlation_id,
        )
        .await
        .unwrap();
    assert!(matches!(commit, ReplicationCommit::Committed { .. }));
    target_runtime
        .release_bootstrap_claim(claim.claim_id)
        .await
        .unwrap();
    drop(guard);

    assert_eq!(
        target_runtime.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_eq!(
        fs::read_to_string(target.native("/remote.md")).unwrap(),
        "remote only"
    );
    assert_eq!(
        fs::read_to_string(target.native("/collision.md")).unwrap(),
        "remote wins"
    );
    assert_eq!(
        fs::read_to_string(target.native("/local.md")).unwrap(),
        "local only"
    );
    assert!(target.native("/image.gif").is_file());
    let target_peer_id = target_runtime
        .state
        .read()
        .await
        .as_ref()
        .unwrap()
        .loro_peer_id;
    assert_ne!(target_peer_id, source_peer_id);
    assert_eq!(
        identity::ReplicaIdentity::load(&target.config_root)
            .unwrap()
            .replica_id(),
        source_replica_id
    );

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
        fs::read_to_string(target.native("/collision.md")).unwrap(),
        "remote wins"
    );
    assert_eq!(
        fs::read_to_string(target.native("/local.md")).unwrap(),
        "local only"
    );
    shutdown_runtime(&restarted).await;
    shutdown_runtime(&source_runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_candidate_rejects_a_concurrent_local_commit_before_publication() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/race.md"), "before").unwrap();
    let runtime = deployment.start().await;
    let base = runtime.capture_replica_inventory().await.unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "concurrent-local-sync-race".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/race.md", "after")],
            },
            OperationSource::Plugin,
            "concurrent-local-sync-correlation",
        )
        .await
        .unwrap();

    assert!(matches!(
        runtime
            .commit_replication_candidate(
                ReplicationCandidate {
                    base_generation_id: base.generation_id,
                    base_state_token: base.state_token,
                    object_updates: Default::default(),
                    blobs: Default::default(),
                },
                "stale-sync-correlation",
            )
            .await,
        Err(ReplicaError::RevisionConflict(_))
    ));
    assert_eq!(
        fs::read_to_string(deployment.native("/race.md")).unwrap(),
        "after"
    );
    shutdown_runtime(&runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_discards_a_pre_activation_normal_sync_generation_and_its_orphan_blob() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/active.md"), "active").unwrap();
    let runtime = deployment.start().await;
    let active = runtime.state.read().await.clone().unwrap();
    let mut candidate = active.clone();
    candidate.generation_id = Uuid::new_v4();
    let orphan_bytes = b"orphaned sync blob".to_vec();
    let orphan_sha256 = super::super::lower_hex(&Sha256::digest(&orphan_bytes));
    runtime
        .store
        .build_sync_generation(
            active.generation_id,
            &candidate,
            &[NewBlob {
                sha256: orphan_sha256.clone(),
                source: NewBlobSource::Bytes(orphan_bytes),
            }],
            &[],
        )
        .await
        .unwrap();
    assert!(
        runtime
            .store
            .generation_exists(candidate.generation_id)
            .await
            .unwrap()
    );
    assert!(runtime.store.blob_exists(&orphan_sha256).await.unwrap());

    shutdown_runtime(&runtime).await;
    drop(runtime);
    let restarted = deployment.start().await;
    assert_eq!(
        restarted.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: active.replica_id,
        }
    );
    assert!(
        !restarted
            .store
            .generation_exists(candidate.generation_id)
            .await
            .unwrap()
    );
    assert!(!restarted.store.blob_exists(&orphan_sha256).await.unwrap());
    shutdown_runtime(&restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_discards_the_inactive_generation_owned_by_a_stale_bootstrap_claim() {
    let source = Deployment::new();
    fs::write(source.native("/source.md"), "source").unwrap();
    let source_runtime = source.start().await;
    let mut stale_candidate = source_runtime.state.read().await.clone().unwrap();

    let target = Deployment::new();
    let target_runtime = target.start().await;
    let claim = BootstrapClaim {
        claim_id: Uuid::new_v4(),
        source_node_id: source.identity.node_id(),
        correlation_id: "stale-bootstrap-claim-correlation".to_owned(),
    };
    stale_candidate.generation_id = claim.claim_id;
    target_runtime
        .store
        .build_inactive_generation(&stale_candidate, &[], &[], &[])
        .await
        .unwrap();
    assert!(
        target_runtime
            .store
            .generation_exists(claim.claim_id)
            .await
            .unwrap()
    );
    assert!(
        target_runtime
            .acquire_bootstrap_claim(&claim)
            .await
            .unwrap()
    );

    shutdown_runtime(&target_runtime).await;
    drop(target_runtime);
    let restarted = target.start().await;
    assert_eq!(restarted.status().await, ReplicaStatus::Uninitialized);
    assert!(
        !restarted
            .store
            .generation_exists(claim.claim_id)
            .await
            .unwrap()
    );
    assert!(
        restarted
            .acquire_bootstrap_claim(&BootstrapClaim {
                claim_id: Uuid::new_v4(),
                source_node_id: Uuid::new_v4(),
                correlation_id: "post-recovery-bootstrap-correlation".to_owned(),
            })
            .await
            .unwrap()
    );

    shutdown_runtime(&restarted).await;
    shutdown_runtime(&source_runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_peer_bindings_reject_node_name_id_and_target_collisions() {
    let deployment = Deployment::new();
    let runtime = deployment.start().await;
    let first_id = Uuid::new_v4();
    let first = NodeIdentity::new(first_id, "peer-one".parse().unwrap());
    runtime
        .bind_sync_peer(&first, Some("oll://127.0.0.1:17384"))
        .await
        .unwrap();

    let renamed = NodeIdentity::new(first_id, "peer-renamed".parse().unwrap());
    assert!(matches!(
        runtime.bind_sync_peer(&renamed, None).await,
        Err(ReplicaError::RevisionConflict(_))
    ));
    let reused_name = NodeIdentity::new(Uuid::new_v4(), "peer-one".parse().unwrap());
    assert!(matches!(
        runtime.bind_sync_peer(&reused_name, None).await,
        Err(ReplicaError::RevisionConflict(_))
    ));
    let reused_target = NodeIdentity::new(Uuid::new_v4(), "peer-two".parse().unwrap());
    assert!(matches!(
        runtime
            .bind_sync_peer(&reused_target, Some("oll://127.0.0.1:17384"))
            .await,
        Err(ReplicaError::RevisionConflict(_))
    ));

    let bindings = runtime.sync_peer_bindings().await.unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].identity, first);
    assert_eq!(
        bindings[0].connect_targets,
        vec!["oll://127.0.0.1:17384".to_owned()]
    );
    shutdown_runtime(&runtime).await;
}
