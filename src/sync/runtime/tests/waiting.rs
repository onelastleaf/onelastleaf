use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uninitialized_peers_wait_on_one_session_then_bootstrap_without_backoff() {
    let source = SyncDeployment::new("waiting-source");
    let receiver = SyncDeployment::new("waiting-receiver");
    let (source_replica, source_logger) = source.start_replica().await;
    let (receiver_replica, receiver_logger) = receiver.start_replica().await;
    assert_eq!(source_replica.status().await, ReplicaStatus::Uninitialized);
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

    let waiting_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let source_waiting = source_sync
            .status()
            .await
            .iter()
            .any(|peer| peer.connection_state == PeerConnectionState::WaitingForReplica as i32);
        let receiver_waiting = receiver_sync
            .status()
            .await
            .iter()
            .any(|peer| peer.connection_state == PeerConnectionState::WaitingForReplica as i32);
        if source_waiting && receiver_waiting {
            break;
        }
        assert!(
            Instant::now() < waiting_deadline,
            "uninitialized peers did not enter the waiting state"
        );
        sleep(Duration::from_millis(10)).await;
    }

    let waiting_session_id = receiver_sync
        .sessions
        .lock()
        .await
        .values()
        .next()
        .expect("waiting session is registered")
        .session_id;
    sleep(Duration::from_millis(700)).await;
    assert_eq!(
        receiver_sync
            .sessions
            .lock()
            .await
            .values()
            .next()
            .expect("waiting session remains registered")
            .session_id,
        waiting_session_id,
        "waiting peers must not close and reconnect while neither has a replica"
    );

    receiver_sync
        .ping(
            source.identity.node_name(),
            "waiting-session-ping-correlation",
        )
        .await
        .unwrap();
    let manual = receiver_sync
        .synchronize(
            Some(source.identity.node_name()),
            3,
            "waiting-session-sync-correlation",
        )
        .await
        .unwrap();
    assert_eq!(manual.len(), 1);
    assert_eq!(manual[0].outcome, PeerSyncOutcome::Failed as i32);
    assert_eq!(manual[0].attempts_used, 1);
    assert_eq!(manual[0].error_code, ErrorCode::FailedPrecondition as i32);
    assert!(
        manual[0]
            .error_message
            .contains("waiting for a local replica")
    );
    assert_eq!(
        receiver_sync
            .sessions
            .lock()
            .await
            .values()
            .next()
            .expect("manual sync does not close the waiting session")
            .session_id,
        waiting_session_id
    );

    fs::write(source.root.join("created.md"), "created locally").unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(10);
    let source_replica_id = loop {
        if let ReplicaStatus::InitializedPopulated { replica_id } = source_replica.status().await {
            if receiver_replica.status().await
                == (ReplicaStatus::InitializedPopulated { replica_id })
                && receiver_sync
                    .status()
                    .await
                    .iter()
                    .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            {
                break replica_id;
            }
        }
        assert!(
            Instant::now() < ready_deadline,
            "replica availability did not renegotiate into bootstrap and ready"
        );
        sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        receiver_replica.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_eq!(
        read_text(&receiver_replica, "/created.md").await,
        "created locally"
    );

    for replica in [&source_replica, &receiver_replica] {
        replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
    let receiver_events = fs::read_to_string(receiver.log_dir.join("sync.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let waiting_index = receiver_events
        .iter()
        .position(|event| event["event"] == "sync_session_waiting_for_replica")
        .expect("waiting transition is observable");
    let bootstrap_index = receiver_events
        .iter()
        .position(|event| event["event"] == "sync_bootstrap_started")
        .expect("bootstrap starts after replica availability");
    assert!(waiting_index < bootstrap_index);
    assert!(
        receiver_events[waiting_index..bootstrap_index]
            .iter()
            .any(|event| event["event"] == "sync_replica_renegotiation_started")
    );
    assert!(
        !receiver_events[waiting_index..bootstrap_index]
            .iter()
            .any(|event| event["event"] == "sync_reconnect_scheduled")
    );
    assert!(!receiver_events.iter().any(|event| {
        event["event"] == "sync_session_failed" && event["error_code"] == "no_replica_available"
    }));

    let shutdown_deadline = Instant::now() + Duration::from_secs(5);
    receiver_sync.shutdown(shutdown_deadline).await.unwrap();
    source_sync.shutdown(shutdown_deadline).await.unwrap();
    receiver_replica.shutdown(shutdown_deadline).await.unwrap();
    source_replica.shutdown(shutdown_deadline).await.unwrap();
}
