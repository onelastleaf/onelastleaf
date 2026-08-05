use super::*;
use std::sync::Arc;

use crate::protocol::oll::{SyncPing, SyncPong, SyncRoundRequest};

#[tokio::test]
async fn long_local_commit_keeps_queued_round_pongs_matchable() {
    let (mut channel, mut peer) = test_channels().await;
    let peer = tokio::spawn(async move {
        for _ in 0..6 {
            let envelope = peer.receive(None).await.unwrap();
            let Some(sync_envelope::Payload::Ping(SyncPing { nonce, .. })) = envelope.payload
            else {
                panic!("expected a round keepalive Ping");
            };
            peer.send(
                sync_envelope::Payload::Pong(SyncPong { nonce }),
                &envelope.correlation_id,
                Some(envelope.message_id),
                None,
            )
            .await
            .unwrap();
        }
        peer.send(
            sync_envelope::Payload::RoundRequest(SyncRoundRequest {}),
            "long-commit-correlation",
            None,
            None,
        )
        .await
        .unwrap();
    });
    let commit_duration = ROUND_PROGRESS_DEADLINE
        .saturating_add(ROUND_KEEPALIVE_INTERVAL.saturating_mul(2))
        .saturating_add(Duration::from_millis(100));

    let (result, liveness_error) = await_with_round_keepalive(
        &mut channel,
        "long-commit-correlation",
        "long_commit_keepalive",
        async {
            tokio::time::sleep(commit_duration).await;
            42
        },
    )
    .await;
    assert_eq!(result, 42);
    assert_eq!(liveness_error, None);

    let envelope = channel
        .receive_progress("post_commit_message")
        .await
        .unwrap();
    assert!(matches!(
        envelope.payload,
        Some(sync_envelope::Payload::RoundRequest(_))
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn stalled_inventory_capture_times_out_without_changing_active_replica_state() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("working");
    let config_root = directory.path().join("config");
    let log_dir = directory.path().join("logs");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&config_root).unwrap();
    fs::write(root.join("ready.md"), "ready").unwrap();
    let identity = NodeIdentity::generate("inventory-timeout".parse().unwrap());
    let identities = IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&log_dir, identity, None).unwrap();
    let replica = ReplicaRuntime::start(
        config_root,
        root,
        &ReplicaStoreConfig::Sqlite {
            path: directory.path().join("store/replica.sqlite3"),
        },
        Arc::clone(&identities),
        Arc::clone(&logger),
    )
    .await
    .unwrap();
    let before = replica.capture_replica_inventory().await.unwrap();
    let commit_guard = identities.commit_guard().await;
    let (mut channel, _peer) = test_channels().await;

    let result = send_round(
        &mut channel,
        &replica,
        SyncObservation {
            connection_id: Uuid::new_v4(),
            peer_node_id: Uuid::new_v4(),
            direction: "outbound",
            correlation_id: "inventory-timeout-correlation",
        },
        None,
        crate::sync::transport::MAX_CHUNK_BYTES,
    )
    .await;
    assert!(matches!(
        result,
        Err(RoundError::Session(
            SessionError::ProgressDeadlineExceeded {
                failure_stage: "inventory_capture"
            }
        ))
    ));

    drop(commit_guard);
    let after = replica.capture_replica_inventory().await.unwrap();
    assert_eq!(after.generation_id, before.generation_id);
    assert_eq!(after.state_token, before.state_token);
    assert_eq!(after.objects, before.objects);
    assert_eq!(after.blobs, before.blobs);

    replica
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}
