use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finite_bidirectional_round_converges_offline_catalog_changes() {
    let first = SyncDeployment::new("sync-first");
    fs::create_dir(first.root.join("folder")).unwrap();
    fs::write(first.root.join("shared.md"), "base").unwrap();
    fs::write(first.root.join("move.md"), "move me").unwrap();
    fs::write(first.root.join("rename.md"), "rename me").unwrap();
    fs::write(first.root.join("delete.md"), "delete me").unwrap();
    let (first_replica, first_logger) = first.start_replica().await;
    let snapshot = first._directory.path().join("seed.ollsnap");
    first_replica
        .export_snapshot(&snapshot, "sync-test-snapshot")
        .await
        .unwrap();

    let second = SyncDeployment::new("sync-second");
    let (second_replica, second_logger) = second.start_replica().await;
    second_replica
        .import_snapshot(&snapshot, "sync-test-import")
        .await
        .unwrap();
    create_text(&first_replica, "first-create", "/first.md", "first").await;
    create_text(&second_replica, "second-create", "/second.md", "second").await;
    replace_text(
        &first_replica,
        "first-offline-edit",
        "/shared.md",
        "first offline edit",
    )
    .await;
    replace_text(
        &second_replica,
        "second-offline-edit",
        "/shared.md",
        "second offline edit",
    )
    .await;
    move_node(
        &first_replica,
        "first-offline-move",
        "/move.md",
        "/folder/move.md",
    )
    .await;
    move_node(
        &second_replica,
        "second-offline-rename",
        "/rename.md",
        "/renamed.md",
    )
    .await;
    delete_node(&second_replica, "second-offline-delete", "/delete.md").await;
    let mut binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
    binary.resize(200_000, 0x5a);
    fs::write(first.root.join("image.gif"), &binary).unwrap();
    let binary_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let present = first_replica
            .state
            .read()
            .await
            .as_ref()
            .and_then(|replica| replica.entry_at_path("/image.gif").ok().flatten())
            .is_some();
        if present {
            break;
        }
        assert!(
            Instant::now() < binary_deadline,
            "binary was not reconciled"
        );
        sleep(Duration::from_millis(25)).await;
    }

    let first_listen = unused_loopback_address();
    let second_listen = unused_loopback_address();
    let first_sync = SyncRuntime::start(
        &first.sync_config(
            Some(first_listen),
            vec![ConnectUrl::from_str(&format!("oll://{second_listen}")).unwrap()],
        ),
        Arc::clone(&first.identities),
        Arc::clone(&first_replica),
        first_logger,
    )
    .await
    .unwrap();
    let second_sync = SyncRuntime::start(
        &second.sync_config(
            Some(second_listen),
            vec![ConnectUrl::from_str(&format!("oll://{first_listen}")).unwrap()],
        ),
        Arc::clone(&second.identities),
        Arc::clone(&second_replica),
        second_logger,
    )
    .await
    .unwrap();

    let ready_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if first_sync
            .status()
            .await
            .iter()
            .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            && second_sync
                .status()
                .await
                .iter()
                .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "normal session was not ready"
        );
        sleep(Duration::from_millis(25)).await;
    }
    let (expected_first_direction, expected_second_direction) =
        if first.identity.node_id() < second.identity.node_id() {
            (
                PeerConnectionDirection::Outbound as i32,
                PeerConnectionDirection::Inbound as i32,
            )
        } else {
            (
                PeerConnectionDirection::Inbound as i32,
                PeerConnectionDirection::Outbound as i32,
            )
        };
    loop {
        let first_direction = first_sync.status().await[0].direction;
        let second_direction = second_sync.status().await[0].direction;
        if first_direction == expected_first_direction
            && second_direction == expected_second_direction
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "duplicate-session arbitration did not stabilize"
        );
        sleep(Duration::from_millis(25)).await;
    }
    let second_name = second.identity.node_name().clone();
    let (first_result, result) = tokio::join!(
        first_sync.synchronize(Some(&second_name), 3, "simultaneous-first-correlation"),
        second_sync.synchronize(None, 3, "simultaneous-second-correlation"),
    );
    let first_result = first_result.unwrap();
    let result = result.unwrap();
    assert_eq!(first_result.len(), 1);
    assert_ne!(
        first_result[0].outcome,
        PeerSyncOutcome::Failed as i32,
        "{first_result:?}"
    );
    assert_eq!(result.len(), 1);
    assert_ne!(
        result[0].outcome,
        PeerSyncOutcome::Failed as i32,
        "{result:?}"
    );
    assert_eq!(read_text(&first_replica, "/first.md").await, "first");
    assert_eq!(read_text(&first_replica, "/second.md").await, "second");
    assert_eq!(read_text(&second_replica, "/first.md").await, "first");
    assert_eq!(read_text(&second_replica, "/second.md").await, "second");
    assert_eq!(
        read_text(&first_replica, "/shared.md").await,
        read_text(&second_replica, "/shared.md").await
    );
    for replica in [&first_replica, &second_replica] {
        assert_eq!(read_text(replica, "/folder/move.md").await, "move me");
        assert_eq!(read_text(replica, "/renamed.md").await, "rename me");
    }
    for root in [&first.root, &second.root] {
        assert!(!root.join("move.md").exists());
        assert!(!root.join("rename.md").exists());
        assert!(!root.join("delete.md").exists());
    }
    assert_eq!(fs::read(second.root.join("image.gif")).unwrap(), binary);
    first_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    second_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let logs = [&first.log_dir, &second.log_dir].map(|log_dir| {
        fs::read_to_string(log_dir.join("sync.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>()
    });
    let round_correlation = [
        "simultaneous-first-correlation",
        "simultaneous-second-correlation",
    ]
    .into_iter()
    .find(|correlation_id| {
        logs.iter().all(|events| {
            events.iter().any(|event| {
                event["event"] == "sync_replica_transfer_staged"
                    && event["correlation_id"] == *correlation_id
            }) && events.iter().any(|event| {
                event["event"] == "sync_candidate_committed"
                    && event["correlation_id"] == *correlation_id
            })
        })
    })
    .expect("one inherited correlation must span both directions of the finite round");
    assert!(!round_correlation.is_empty());

    let second_result = second_sync
        .synchronize(None, 1, "already-converged-correlation")
        .await
        .unwrap();
    assert_eq!(
        second_result[0].outcome,
        PeerSyncOutcome::AlreadySatisfied as i32
    );

    let shutdown_deadline = Instant::now() + Duration::from_secs(5);
    second_sync.shutdown(shutdown_deadline).await.unwrap();
    first_sync.shutdown(shutdown_deadline).await.unwrap();
    second_replica.shutdown(shutdown_deadline).await.unwrap();
    first_replica.shutdown(shutdown_deadline).await.unwrap();
}
