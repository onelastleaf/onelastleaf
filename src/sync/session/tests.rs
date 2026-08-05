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
async fn two_uninitialized_nodes_close_without_entering_ready_state() {
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
    assert!(matches!(
        left,
        Err(SessionError::LocalProtocol {
            code: SyncCloseCode::NoReplicaAvailable,
            error_code: "no_replica_available",
            ..
        })
    ));
    assert!(matches!(
        right,
        Err(SessionError::LocalProtocol {
            code: SyncCloseCode::NoReplicaAvailable,
            error_code: "no_replica_available",
            ..
        })
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
