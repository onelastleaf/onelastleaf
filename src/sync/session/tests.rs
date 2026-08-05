use tokio::io::duplex;

use crate::{cli::NodeName, configuration::NetworkKey, sync::derive_noise_psk};

use super::*;
use crate::sync::{HANDSHAKE_DEADLINE, NoiseTransport};

#[tokio::test]
async fn hello_and_ready_select_normal_and_bootstrap_roles_without_compression_or_nonce() {
    let left_identity = NodeIdentity::generate("left-node".parse::<NodeName>().unwrap());
    let right_identity = NodeIdentity::generate("right-node".parse::<NodeName>().unwrap());
    let replica_id = Uuid::new_v4();
    let key = derive_noise_psk(&NetworkKey::new_for_test(vec![9; 32]));
    let (left_stream, right_stream) = duplex(16 * 1024);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (left_transport, right_transport) = tokio::join!(
        NoiseTransport::connect(left_stream, &key, deadline),
        NoiseTransport::accept(right_stream, &key, deadline),
    );
    let (left, right) = tokio::join!(
        PendingSession::begin(
            left_transport.unwrap(),
            &left_identity,
            ReplicaStatus::InitializedEmpty { replica_id },
            "left-handshake",
            deadline,
        ),
        PendingSession::begin(
            right_transport.unwrap(),
            &right_identity,
            ReplicaStatus::InitializedPopulated { replica_id },
            "right-handshake",
            deadline,
        ),
    );
    let mut left = left.unwrap();
    let mut right = right.unwrap();
    assert_eq!(left.remote, right_identity);
    assert_eq!(right.remote, left_identity);
    assert_eq!(left.mode, SessionReplicaMode::Normal);
    assert_eq!(right.mode, SessionReplicaMode::Normal);
    let (left_ready, right_ready) = tokio::join!(
        left.exchange_ready("left-handshake", deadline),
        right.exchange_ready("right-handshake", deadline),
    );
    left_ready.unwrap();
    right_ready.unwrap();
}

#[tokio::test]
async fn two_uninitialized_nodes_enter_waiting_and_keep_the_channel_usable() {
    let left_identity = NodeIdentity::generate("left-empty".parse().unwrap());
    let right_identity = NodeIdentity::generate("right-empty".parse().unwrap());
    let key = derive_noise_psk(&NetworkKey::new_for_test(vec![4; 32]));
    let (left_stream, right_stream) = duplex(16 * 1024);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (left_transport, right_transport) = tokio::join!(
        NoiseTransport::connect(left_stream, &key, deadline),
        NoiseTransport::accept(right_stream, &key, deadline),
    );
    let (left, right) = tokio::join!(
        PendingSession::begin(
            left_transport.unwrap(),
            &left_identity,
            ReplicaStatus::Uninitialized,
            "left-empty",
            deadline,
        ),
        PendingSession::begin(
            right_transport.unwrap(),
            &right_identity,
            ReplicaStatus::Uninitialized,
            "right-empty",
            deadline,
        ),
    );
    let mut left = left.unwrap();
    let mut right = right.unwrap();
    assert_eq!(left.mode, SessionReplicaMode::Waiting);
    assert_eq!(right.mode, SessionReplicaMode::Waiting);
    assert_eq!(left.replica_id, None);
    assert_eq!(right.replica_id, None);
    let (left_ready, right_ready) = tokio::join!(
        left.exchange_ready("left-empty", deadline),
        right.exchange_ready("right-empty", deadline),
    );
    left_ready.unwrap();
    right_ready.unwrap();

    let ping_id = left
        .channel
        .send(
            sync_envelope::Payload::Ping(crate::protocol::oll::SyncPing {
                nonce: 42,
                sent_at: None,
            }),
            "waiting-ping",
            None,
            Some(deadline),
        )
        .await
        .unwrap();
    let ping = right.channel.receive(Some(deadline)).await.unwrap();
    assert_eq!(ping.message_id, ping_id);
    assert!(matches!(
        ping.payload,
        Some(sync_envelope::Payload::Ping(_))
    ));
    right
        .channel
        .send(
            sync_envelope::Payload::Pong(crate::protocol::oll::SyncPong { nonce: 42 }),
            "waiting-ping",
            Some(ping_id),
            Some(deadline),
        )
        .await
        .unwrap();
    assert!(matches!(
        left.channel.receive(Some(deadline)).await.unwrap().payload,
        Some(sync_envelope::Payload::Pong(_))
    ));
}

#[tokio::test]
async fn schema_self_and_replica_mismatches_are_authenticated_close_reasons() {
    let key = derive_noise_psk(&NetworkKey::new_for_test(vec![5; 32]));
    let left_identity = NodeIdentity::generate("schema-left".parse().unwrap());
    let right_identity = NodeIdentity::generate("schema-right".parse().unwrap());
    let replica_id = Uuid::new_v4();
    let (left_stream, right_stream) = duplex(16 * 1024);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (left_transport, right_transport) = tokio::join!(
        NoiseTransport::connect(left_stream, &key, deadline),
        NoiseTransport::accept(right_stream, &key, deadline),
    );
    let local = PendingSession::begin(
        left_transport.unwrap(),
        &left_identity,
        ReplicaStatus::InitializedEmpty { replica_id },
        "schema-left-correlation",
        deadline,
    );
    let remote = async {
        let mut channel = SessionChannel::new(right_transport.unwrap());
        let local_hello = channel.receive(Some(deadline)).await.unwrap();
        assert!(matches!(
            local_hello.payload,
            Some(sync_envelope::Payload::Hello(_))
        ));
        channel
            .send(
                sync_envelope::Payload::Hello(SyncHello {
                    node: Some(right_identity.to_proto()),
                    replica_state: Some(sync_hello::ReplicaState::ReplicaId(ReplicaId {
                        value: replica_id.to_string(),
                    })),
                    protocol_schema_sha256: vec![0; 32],
                    max_chunk_bytes: MAX_CHUNK_BYTES,
                }),
                "schema-right-correlation",
                None,
                Some(deadline),
            )
            .await
            .unwrap();
        channel.receive(Some(deadline)).await.unwrap_err()
    };
    let (local, remote) = tokio::join!(local, remote);
    assert!(matches!(
        local,
        Err(SessionError::LocalProtocol {
            code: SyncCloseCode::SchemaMismatch,
            error_code: "schema_mismatch",
            ..
        })
    ));
    assert!(matches!(
        remote,
        SessionError::RemoteClosed {
            code: SyncCloseCode::SchemaMismatch,
            ..
        }
    ));

    let same_identity = NodeIdentity::generate("same-node".parse().unwrap());
    let (left_stream, right_stream) = duplex(16 * 1024);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (left_transport, right_transport) = tokio::join!(
        NoiseTransport::connect(left_stream, &key, deadline),
        NoiseTransport::accept(right_stream, &key, deadline),
    );
    let (left, right) = tokio::join!(
        PendingSession::begin(
            left_transport.unwrap(),
            &same_identity,
            ReplicaStatus::InitializedEmpty { replica_id },
            "self-left",
            deadline,
        ),
        PendingSession::begin(
            right_transport.unwrap(),
            &same_identity,
            ReplicaStatus::InitializedEmpty { replica_id },
            "self-right",
            deadline,
        ),
    );
    for result in [left, right] {
        assert!(matches!(
            result,
            Err(SessionError::LocalProtocol {
                code: SyncCloseCode::SelfConnection,
                error_code: "self_connection",
                ..
            })
        ));
    }

    let left_identity = NodeIdentity::generate("mismatch-left".parse().unwrap());
    let right_identity = NodeIdentity::generate("mismatch-right".parse().unwrap());
    let (left_stream, right_stream) = duplex(16 * 1024);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (left_transport, right_transport) = tokio::join!(
        NoiseTransport::connect(left_stream, &key, deadline),
        NoiseTransport::accept(right_stream, &key, deadline),
    );
    let (left, right) = tokio::join!(
        PendingSession::begin(
            left_transport.unwrap(),
            &left_identity,
            ReplicaStatus::InitializedEmpty {
                replica_id: Uuid::new_v4(),
            },
            "mismatch-left",
            deadline,
        ),
        PendingSession::begin(
            right_transport.unwrap(),
            &right_identity,
            ReplicaStatus::InitializedEmpty {
                replica_id: Uuid::new_v4(),
            },
            "mismatch-right",
            deadline,
        ),
    );
    for result in [left, right] {
        assert!(matches!(
            result,
            Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ReplicaMismatch,
                error_code: "replica_mismatch",
                ..
            })
        ));
    }
}

#[tokio::test]
async fn each_received_envelope_refreshes_the_round_progress_deadline() {
    let key = derive_noise_psk(&NetworkKey::new_for_test(vec![6; 32]));
    let (sender_stream, receiver_stream) = duplex(16 * 1024);
    let handshake_deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let (sender, receiver) = tokio::join!(
        NoiseTransport::connect(sender_stream, &key, handshake_deadline),
        NoiseTransport::accept(receiver_stream, &key, handshake_deadline),
    );
    let mut sender = SessionChannel::new(sender.unwrap());
    let mut receiver = SessionChannel::new(receiver.unwrap());
    let sending = tokio::spawn(async move {
        for sequence in 0..3 {
            tokio::time::sleep(Duration::from_millis(450)).await;
            sender
                .send_progress(
                    sync_envelope::Payload::RoundRequest(crate::protocol::oll::SyncRoundRequest {}),
                    &format!("progress-{sequence}"),
                    None,
                    "progress_test_send",
                )
                .await
                .unwrap();
        }
    });

    let started = Instant::now();
    for sequence in 0..3 {
        let envelope = receiver
            .receive_progress("progress_test_receive")
            .await
            .unwrap();
        assert_eq!(envelope.correlation_id, format!("progress-{sequence}"));
    }
    assert!(started.elapsed() > ROUND_PROGRESS_DEADLINE);
    sending.await.unwrap();
}
