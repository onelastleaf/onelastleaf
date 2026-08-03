use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noise_session_bootstraps_an_uninitialized_receiver_and_reconnects_normally() {
    let source = SyncDeployment::new("sync-source");
    fs::write(source.root.join("source.md"), "from source").unwrap();
    let source_binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff";
    fs::write(source.root.join("source.gif"), source_binary).unwrap();
    let (source_replica, source_logger) = source.start_replica().await;
    let source_replica_id = match source_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected source state: {state:?}"),
    };
    let receiver = SyncDeployment::new("sync-receiver");
    let (receiver_replica, receiver_logger) = receiver.start_replica().await;
    assert_eq!(
        receiver_replica.status().await,
        ReplicaStatus::Uninitialized
    );

    let listen = unused_loopback_address();
    let source_sync = SyncRuntime::start(
        &source.sync_config(Some(listen), Vec::new()),
        Arc::clone(&source.identities),
        Arc::clone(&source_replica),
        source_logger,
    )
    .await
    .unwrap();
    let receiver_sync = SyncRuntime::start(
        &receiver.sync_config(
            None,
            vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
        ),
        Arc::clone(&receiver.identities),
        Arc::clone(&receiver_replica),
        receiver_logger,
    )
    .await
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if receiver_replica.status().await
            == (ReplicaStatus::InitializedPopulated {
                replica_id: source_replica_id,
            })
            && receiver_sync
                .status()
                .await
                .iter()
                .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bootstrap did not complete and reconnect"
        );
        sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        read_text(&receiver_replica, "/source.md").await,
        "from source"
    );
    assert_eq!(
        fs::read(receiver.root.join("source.gif")).unwrap(),
        source_binary
    );
    source_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    receiver_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let source_events = fs::read_to_string(source.log_dir.join("sync.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let receiver_events = fs::read_to_string(receiver.log_dir.join("sync.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let bootstrap_correlation = source_events
        .iter()
        .find(|event| event["event"] == "sync_bootstrap_started")
        .and_then(|event| event["correlation_id"].as_str())
        .unwrap();
    assert!(receiver_events.iter().any(|event| {
        event["event"] == "sync_bootstrap_started"
            && event["correlation_id"] == bootstrap_correlation
    }));
    assert!(source_events.iter().any(|event| {
        event["event"] == "sync_replica_transfer_completed"
            && event["correlation_id"] == bootstrap_correlation
    }));
    assert!(receiver_events.iter().any(|event| {
        event["event"] == "sync_candidate_committed"
            && event["correlation_id"] == bootstrap_correlation
    }));

    let shutdown_deadline = Instant::now() + Duration::from_secs(5);
    receiver_sync.shutdown(shutdown_deadline).await.unwrap();
    source_sync.shutdown(shutdown_deadline).await.unwrap();
    receiver_replica.shutdown(shutdown_deadline).await.unwrap();
    source_replica.shutdown(shutdown_deadline).await.unwrap();
}
