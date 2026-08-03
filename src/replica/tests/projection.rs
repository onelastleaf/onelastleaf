use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_markers_win_over_stale_working_tree_after_restart() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "store-wins").unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;

    let active = runtime.state.read().await.clone().unwrap();
    fs::write(deployment.native("/a.md"), "stale-disk").unwrap();
    runtime
        .store
        .save_active(&active, &[], &[], &["/a.md".to_owned()])
        .await
        .unwrap();
    drop(runtime);

    let restarted = deployment.start().await;
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "store-wins"
    );
    assert!(
        restarted
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
    shutdown_runtime(&restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targeted_projection_retries_a_transient_failure_before_acknowledging_the_path() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "before").unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;

    let displaced_root = deployment._directory.path().join("working-displaced");
    fs::rename(&deployment.root, &displaced_root).unwrap();
    symlink(&displaced_root, &deployment.root).unwrap();
    let root = deployment.root.clone();
    let repair = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        fs::remove_file(&root).unwrap();
        fs::rename(&displaced_root, &root).unwrap();
    });

    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "transient-projection-retry".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/a.md", "after")],
            },
            OperationSource::Plugin,
            "transient-projection-correlation",
        )
        .await
        .unwrap();
    repair.await.unwrap();

    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "after"
    );
    let active = runtime.state.read().await.clone().unwrap();
    assert!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
    runtime
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let records = fs::read_to_string(deployment.log_dir.join("oll.log")).unwrap();
    assert!(records.lines().any(|line| {
        let event: serde_json::Value = serde_json::from_str(line).unwrap();
        event["event"] == "working_tree_projection_retrying"
            && event["correlation_id"] == "transient-projection-correlation"
            && event["attempt"] == 1
            && event["backoff_ms"] == 100
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_commit_cannot_forget_an_exhausted_projection_marker() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "old A").unwrap();
    fs::write(deployment.native("/b.md"), "old B").unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;

    let displaced_root = deployment._directory.path().join("working-displaced");
    fs::rename(&deployment.root, &displaced_root).unwrap();
    symlink(&displaced_root, &deployment.root).unwrap();
    let error = runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "failed-a-projection".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/a.md", "new A")],
            },
            OperationSource::Plugin,
            "failed-a-projection-correlation",
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ReplicaError::InvalidArgument(_)));
    let active = runtime.state.read().await.clone().unwrap();
    assert_eq!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap(),
        ["/a.md"]
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "old A"
    );

    fs::remove_file(&deployment.root).unwrap();
    fs::rename(&displaced_root, &deployment.root).unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "successful-b-projection".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/b.md", "new B")],
            },
            OperationSource::Plugin,
            "successful-b-projection-correlation",
        )
        .await
        .unwrap();

    let active = runtime.state.read().await.clone().unwrap();
    assert_eq!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap(),
        ["/a.md"]
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "old A"
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/b.md")).unwrap(),
        "new B"
    );

    runtime
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let records = fs::read_to_string(deployment.log_dir.join("oll.log")).unwrap();
    let retry_attempts = records
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| {
            event["event"] == "working_tree_projection_retrying"
                && event["correlation_id"] == "failed-a-projection-correlation"
        })
        .map(|event| event["attempt"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(retry_attempts, [1, 2]);

    drop(runtime);
    let restarted = deployment.start().await;
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "new A"
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/b.md")).unwrap(),
        "new B"
    );
    let active = restarted.state.read().await.clone().unwrap();
    assert!(
        restarted
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
    shutdown_runtime(&restarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_commit_retry_completes_its_pending_projection() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "before").unwrap();
    let runtime = deployment.start().await;
    let request = oll::CommitDocumentsRequest {
        operation_id: "retained-projection-result".to_owned(),
        preconditions: Vec::new(),
        mutations: vec![replace_mutation("/a.md", "committed")],
    };
    let response = runtime
        .commit_documents(
            request.clone(),
            OperationSource::Plugin,
            "original-correlation",
        )
        .await
        .unwrap();
    shutdown_runtime(&runtime).await;
    let active = runtime.state.read().await.clone().unwrap();
    fs::write(deployment.native("/a.md"), "stale").unwrap();
    runtime
        .store
        .save_active(&active, &[], &[], &["/a.md".to_owned()])
        .await
        .unwrap();

    let retry = runtime
        .commit_documents(request, OperationSource::Plugin, "retry-correlation")
        .await
        .unwrap();
    assert_eq!(retry, response);
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "committed"
    );
    assert!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targeted_projection_never_follows_a_parent_symlink_outside_replica_root() {
    let deployment = Deployment::new();
    fs::create_dir(deployment.native("/dir")).unwrap();
    fs::write(deployment.native("/dir/a.md"), "before").unwrap();
    let outside = deployment._directory.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;

    fs::remove_dir_all(deployment.native("/dir")).unwrap();
    symlink(&outside, deployment.native("/dir")).unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "safe-parent-projection".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/dir/a.md", "inside-only")],
            },
            OperationSource::Plugin,
            "safe-projection-correlation",
        )
        .await
        .unwrap();

    assert!(
        fs::symlink_metadata(deployment.native("/dir"))
            .unwrap()
            .is_dir()
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/dir/a.md")).unwrap(),
        "inside-only"
    );
    assert!(!outside.join("a.md").exists());
}
