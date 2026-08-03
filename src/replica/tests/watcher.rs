use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_finishes_in_flight_watcher_work_without_starting_queued_work() {
    let deployment = Deployment::new();
    let runtime = deployment.start().await;
    let log_path = deployment.log_dir.join("oll.log");
    let initial_starts = reconciliation_start_count(&log_path);
    let coordinator = runtime.identities.commit_guard().await;

    fs::write(deployment.native("/first.md"), "first").unwrap();
    wait_for_reconciliation_start_count(&log_path, initial_starts + 1).await;
    fs::write(deployment.native("/second.md"), "second").unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;

    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = tokio::spawn(async move {
        shutdown_runtime
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(coordinator);
    shutdown.await.unwrap().unwrap();

    assert_eq!(reconciliation_start_count(&log_path), initial_starts + 1);
    assert!(
        runtime
            .inspect_document(&deployment.native("/first.md"))
            .await
            .is_ok()
    );
    assert!(
        runtime
            .inspect_document(&deployment.native("/second.md"))
            .await
            .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_shutdown_aborts_in_flight_work_at_its_absolute_deadline() {
    let deployment = Deployment::new();
    let runtime = deployment.start().await;
    let log_path = deployment.log_dir.join("oll.log");
    let initial_starts = reconciliation_start_count(&log_path);
    let coordinator = runtime.identities.commit_guard().await;

    fs::write(deployment.native("/blocked.md"), "blocked").unwrap();
    wait_for_reconciliation_start_count(&log_path, initial_starts + 1).await;
    let started = tokio::time::Instant::now();
    let result = runtime
        .shutdown(tokio::time::Instant::now() + Duration::from_millis(100))
        .await;

    assert!(matches!(result, Err(ReplicaError::Internal(_))));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(coordinator);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_initializes_once_deduplicates_final_state_and_survives_restart() {
    let deployment = Deployment::new();
    let runtime = deployment.start().await;
    assert_eq!(runtime.status().await, ReplicaStatus::Uninitialized);

    fs::create_dir(deployment.native("/notes")).unwrap();
    fs::write(deployment.native("/notes/a.md"), "hello\n").unwrap();
    fs::write(
        deployment.native("/image.gif"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();

    let first = wait_for_document(&runtime, &deployment.native("/notes/a.md")).await;
    wait_for_path(&runtime, "/image.gif").await;
    let replica_id = match runtime.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let operations_before = runtime
        .list_operations(&deployment.native("/notes/a.md"), 100)
        .await
        .unwrap();
    let (binary_versions_before, lamport_before) = {
        let state = runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        (binary.versions.len(), replica.lamport_clock)
    };

    fs::create_dir_all(deployment.native("/notes")).unwrap();
    fs::write(deployment.native("/notes/a.md"), "hello\n").unwrap();
    fs::write(
        deployment.native("/image.gif"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    let duplicate = runtime
        .inspect_document(&deployment.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(duplicate.catalog_node_id, first.catalog_node_id);
    assert_eq!(duplicate.document_id, first.document_id);
    assert_eq!(duplicate.catalog_revision, first.catalog_revision);
    assert_eq!(duplicate.document_revision, first.document_revision);
    assert_eq!(
        runtime
            .list_operations(&deployment.native("/notes/a.md"), 100)
            .await
            .unwrap()
            .len(),
        operations_before.len()
    );
    {
        let state = runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        assert_eq!(binary.versions.len(), binary_versions_before);
        assert_eq!(replica.lamport_clock, lamport_before);
    }

    shutdown_runtime(&runtime).await;
    drop(runtime);
    let restarted = deployment.start().await;
    assert_eq!(
        restarted.status().await,
        ReplicaStatus::InitializedPopulated { replica_id }
    );
    let after_restart = restarted
        .inspect_document(&deployment.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(after_restart.catalog_node_id, first.catalog_node_id);
    assert_eq!(after_restart.document_id, first.document_id);
    assert_eq!(after_restart.document_revision, first.document_revision);
    shutdown_runtime(&restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_registration_closes_the_startup_scan_race() {
    let deployment = Deployment::new();
    for index in 0..300 {
        fs::write(
            deployment.native(&format!("/seed-{index:03}.md")),
            format!("seed {index}"),
        )
        .unwrap();
    }
    let logger = NodeLogger::open(&deployment.log_dir, deployment.identity.clone()).unwrap();
    let root = deployment.root.clone();
    let config_root = deployment.config_root.clone();
    let config = ReplicaStoreConfig::Sqlite {
        path: deployment.store_path.clone(),
    };
    let identities = IdentityCoordinator::new(deployment.identity.clone());
    let starting = tokio::spawn(async move {
        ReplicaRuntime::start(config_root, root, &config, identities, logger).await
    });
    tokio::task::yield_now().await;
    fs::write(deployment.native("/arrived-during-startup.md"), "not lost").unwrap();

    let runtime = starting.await.unwrap().unwrap();
    let late = wait_for_document(&runtime, &deployment.native("/arrived-during-startup.md")).await;
    assert_eq!(late.path, "/arrived-during-startup.md");
    assert_eq!(
        fs::read_to_string(deployment.native("/arrived-during-startup.md")).unwrap(),
        "not lost"
    );
    shutdown_runtime(&runtime).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_rename_and_editor_replacement_preserve_identity_but_offline_move_does_not() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "first").unwrap();
    let runtime = deployment.start().await;
    let original = runtime
        .inspect_document(&deployment.native("/a.md"))
        .await
        .unwrap();

    fs::rename(deployment.native("/a.md"), deployment.native("/renamed.md")).unwrap();
    let renamed = wait_for_document(&runtime, &deployment.native("/renamed.md")).await;
    assert_eq!(renamed.catalog_node_id, original.catalog_node_id);
    assert_eq!(renamed.document_id, original.document_id);

    fs::write(deployment.native("/.editor-save.tmp"), "editor replacement").unwrap();
    fs::rename(
        deployment.native("/.editor-save.tmp"),
        deployment.native("/renamed.md"),
    )
    .unwrap();
    let mut replaced = None;
    for _ in 0..50 {
        let inspection = runtime
            .inspect_document(&deployment.native("/renamed.md"))
            .await
            .unwrap();
        if inspection.document_revision != renamed.document_revision {
            replaced = Some(inspection);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let replaced = replaced.expect("editor replacement was not reconciled");
    assert_eq!(replaced.catalog_node_id, original.catalog_node_id);
    assert_eq!(replaced.document_id, original.document_id);
    assert_eq!(
        fs::read_to_string(deployment.native("/renamed.md")).unwrap(),
        "editor replacement"
    );

    shutdown_runtime(&runtime).await;
    drop(runtime);
    fs::rename(
        deployment.native("/renamed.md"),
        deployment.native("/offline.md"),
    )
    .unwrap();
    let restarted = deployment.start().await;
    let offline = restarted
        .inspect_document(&deployment.native("/offline.md"))
        .await
        .unwrap();
    assert_ne!(offline.catalog_node_id, original.catalog_node_id);
    assert_ne!(offline.document_id, original.document_id);
    shutdown_runtime(&restarted).await;
}
