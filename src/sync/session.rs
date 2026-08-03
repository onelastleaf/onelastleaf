use std::fmt;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    node::identity::NodeIdentity,
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{
            NoLocalReplica, ReplicaId, SyncClose, SyncCloseCode, SyncEnvelope, SyncHello,
            SyncReady, sync_envelope, sync_hello,
        },
    },
    replica::ReplicaStatus,
};

use super::transport::{MAX_CHUNK_BYTES, NoiseTransport, TransportError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionReplicaMode {
    Normal,
    BootstrapSource,
    BootstrapReceiver,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionError {
    Transport(TransportError),
    LocalProtocol {
        code: SyncCloseCode,
        message: &'static str,
    },
    RemoteClosed {
        code: SyncCloseCode,
        message: String,
    },
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::LocalProtocol { message, .. } => formatter.write_str(message),
            Self::RemoteClosed { message, .. } if !message.is_empty() => {
                formatter.write_str(message)
            }
            Self::RemoteClosed { .. } => formatter.write_str("remote sync session closed"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<TransportError> for SessionError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

pub(crate) struct SessionChannel<S> {
    transport: NoiseTransport<S>,
    next_message_id: u64,
    last_received_message_id: u64,
}

impl<S> SessionChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn new(transport: NoiseTransport<S>) -> Self {
        Self {
            transport,
            next_message_id: 1,
            last_received_message_id: 0,
        }
    }

    pub(crate) fn handshake_hash(&self) -> &[u8; 32] {
        self.transport.handshake_hash()
    }

    pub(crate) async fn send(
        &mut self,
        payload: sync_envelope::Payload,
        correlation_id: &str,
        reply_to: Option<u64>,
        deadline: Option<Instant>,
    ) -> Result<u64, SessionError> {
        if correlation_id.is_empty() {
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                message: "local sync correlation_id is empty",
            });
        }
        let message_id = self.next_message_id;
        self.next_message_id =
            self.next_message_id
                .checked_add(1)
                .ok_or(SessionError::LocalProtocol {
                    code: SyncCloseCode::ResourceExhausted,
                    message: "sync message ID space is exhausted",
                })?;
        self.transport
            .write_envelope(
                &SyncEnvelope {
                    message_id,
                    reply_to,
                    correlation_id: correlation_id.to_owned(),
                    payload: Some(payload),
                },
                deadline,
            )
            .await?;
        Ok(message_id)
    }

    pub(crate) async fn receive(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<SyncEnvelope, SessionError> {
        let envelope = self.transport.read_envelope(deadline).await?;
        if envelope.message_id <= self.last_received_message_id
            || envelope.correlation_id.is_empty()
            || envelope.payload.is_none()
        {
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                message: "received sync envelope has invalid metadata",
            });
        }
        self.last_received_message_id = envelope.message_id;
        if let Some(sync_envelope::Payload::Close(close)) = envelope.payload.as_ref() {
            let code =
                SyncCloseCode::try_from(close.code).unwrap_or(SyncCloseCode::ProtocolViolation);
            return Err(SessionError::RemoteClosed {
                code,
                message: close.message.clone(),
            });
        }
        Ok(envelope)
    }

    pub(crate) async fn close(
        &mut self,
        code: SyncCloseCode,
        message: &'static str,
        correlation_id: &str,
        deadline: Option<Instant>,
    ) {
        let _ = self
            .send(
                sync_envelope::Payload::Close(SyncClose {
                    code: code as i32,
                    message: message.to_owned(),
                }),
                correlation_id,
                None,
                deadline,
            )
            .await;
        let _ = self.transport.shutdown().await;
    }
}

pub(crate) struct PendingSession<S> {
    pub channel: SessionChannel<S>,
    pub remote: NodeIdentity,
    pub replica_id: Uuid,
    pub mode: SessionReplicaMode,
    pub max_chunk_bytes: u32,
    pub bootstrap_correlation_id: Option<String>,
}

impl<S> PendingSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) async fn begin(
        transport: NoiseTransport<S>,
        local_identity: &NodeIdentity,
        local_replica: ReplicaStatus,
        correlation_id: &str,
        deadline: Instant,
    ) -> Result<Self, SessionError> {
        let mut channel = SessionChannel::new(transport);
        channel
            .send(
                sync_envelope::Payload::Hello(SyncHello {
                    node: Some(local_identity.to_proto()),
                    replica_state: Some(match local_replica {
                        ReplicaStatus::Uninitialized => {
                            sync_hello::ReplicaState::NoLocalReplica(NoLocalReplica {})
                        }
                        ReplicaStatus::InitializedEmpty { replica_id }
                        | ReplicaStatus::InitializedPopulated { replica_id } => {
                            sync_hello::ReplicaState::ReplicaId(ReplicaId {
                                value: replica_id.to_string(),
                            })
                        }
                    }),
                    protocol_schema_sha256: PROTOCOL_SCHEMA_SHA256.to_vec(),
                    max_chunk_bytes: MAX_CHUNK_BYTES,
                }),
                correlation_id,
                None,
                Some(deadline),
            )
            .await?;
        let envelope = channel.receive(Some(deadline)).await?;
        let remote_correlation_id = envelope.correlation_id.clone();
        if envelope.reply_to.is_some() {
            return fail_handshake(
                channel,
                SyncCloseCode::ProtocolViolation,
                "SyncHello must not reply to another message",
                correlation_id,
                deadline,
            )
            .await;
        }
        let Some(sync_envelope::Payload::Hello(hello)) = envelope.payload else {
            return fail_handshake(
                channel,
                SyncCloseCode::ProtocolViolation,
                "first encrypted sync message must be SyncHello",
                correlation_id,
                deadline,
            )
            .await;
        };
        if hello.protocol_schema_sha256.as_slice() != PROTOCOL_SCHEMA_SHA256 {
            return fail_handshake(
                channel,
                SyncCloseCode::SchemaMismatch,
                "protocol schema fingerprint differs",
                correlation_id,
                deadline,
            )
            .await;
        }
        if !(1..=MAX_CHUNK_BYTES).contains(&hello.max_chunk_bytes) {
            return fail_handshake(
                channel,
                SyncCloseCode::NegotiationFailed,
                "peer max_chunk_bytes is invalid",
                correlation_id,
                deadline,
            )
            .await;
        }
        let remote = match hello
            .node
            .and_then(|node| NodeIdentity::from_proto(node).ok())
        {
            Some(remote) => remote,
            None => {
                return fail_handshake(
                    channel,
                    SyncCloseCode::ProtocolViolation,
                    "peer NodeIdentity is invalid",
                    correlation_id,
                    deadline,
                )
                .await;
            }
        };
        if remote.node_id() == local_identity.node_id() {
            return fail_handshake(
                channel,
                SyncCloseCode::SelfConnection,
                "peer presented the local NodeId",
                correlation_id,
                deadline,
            )
            .await;
        }
        let remote_replica = match hello.replica_state {
            Some(sync_hello::ReplicaState::NoLocalReplica(_)) => None,
            Some(sync_hello::ReplicaState::ReplicaId(replica_id)) => {
                match parse_replica_id(&replica_id.value) {
                    Some(replica_id) => Some(replica_id),
                    None => {
                        return fail_handshake(
                            channel,
                            SyncCloseCode::ProtocolViolation,
                            "peer ReplicaId is invalid",
                            correlation_id,
                            deadline,
                        )
                        .await;
                    }
                }
            }
            None => {
                return fail_handshake(
                    channel,
                    SyncCloseCode::ProtocolViolation,
                    "SyncHello is missing replica state",
                    correlation_id,
                    deadline,
                )
                .await;
            }
        };
        let local_replica = match local_replica {
            ReplicaStatus::Uninitialized => None,
            ReplicaStatus::InitializedEmpty { replica_id }
            | ReplicaStatus::InitializedPopulated { replica_id } => Some(replica_id),
        };
        let (replica_id, mode) = match (local_replica, remote_replica) {
            (Some(local), Some(remote)) if local == remote => (local, SessionReplicaMode::Normal),
            (Some(_), Some(_)) => {
                return fail_handshake(
                    channel,
                    SyncCloseCode::ReplicaMismatch,
                    "peer ReplicaId differs from the local replica",
                    correlation_id,
                    deadline,
                )
                .await;
            }
            (Some(local), None) => (local, SessionReplicaMode::BootstrapSource),
            (None, Some(remote)) => (remote, SessionReplicaMode::BootstrapReceiver),
            (None, None) => {
                return fail_handshake(
                    channel,
                    SyncCloseCode::NoReplicaAvailable,
                    "neither peer has a local replica",
                    correlation_id,
                    deadline,
                )
                .await;
            }
        };
        Ok(Self {
            channel,
            remote,
            replica_id,
            mode,
            max_chunk_bytes: hello.max_chunk_bytes.min(MAX_CHUNK_BYTES),
            bootstrap_correlation_id: match mode {
                SessionReplicaMode::Normal => None,
                SessionReplicaMode::BootstrapSource => Some(correlation_id.to_owned()),
                SessionReplicaMode::BootstrapReceiver => Some(remote_correlation_id),
            },
        })
    }

    pub(crate) async fn exchange_ready(
        &mut self,
        correlation_id: &str,
        deadline: Instant,
    ) -> Result<(), SessionError> {
        let correlation_id = self
            .bootstrap_correlation_id
            .as_deref()
            .unwrap_or(correlation_id);
        self.channel
            .send(
                sync_envelope::Payload::Ready(SyncReady {
                    max_chunk_bytes: self.max_chunk_bytes,
                    session_replica_id: Some(ReplicaId {
                        value: self.replica_id.to_string(),
                    }),
                }),
                correlation_id,
                None,
                Some(deadline),
            )
            .await?;
        let envelope = self.channel.receive(Some(deadline)).await?;
        if envelope.reply_to.is_some() {
            self.channel
                .close(
                    SyncCloseCode::ProtocolViolation,
                    "SyncReady must not reply to another message",
                    correlation_id,
                    Some(deadline),
                )
                .await;
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                message: "SyncReady must not reply to another message",
            });
        }
        if self.bootstrap_correlation_id.is_some() && envelope.correlation_id != correlation_id {
            self.channel
                .close(
                    SyncCloseCode::ProtocolViolation,
                    "bootstrap SyncReady correlation differs from SyncHello",
                    correlation_id,
                    Some(deadline),
                )
                .await;
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                message: "bootstrap SyncReady correlation differs from SyncHello",
            });
        }
        let Some(sync_envelope::Payload::Ready(ready)) = envelope.payload else {
            self.channel
                .close(
                    SyncCloseCode::ProtocolViolation,
                    "expected SyncReady after SyncHello",
                    correlation_id,
                    Some(deadline),
                )
                .await;
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                message: "expected SyncReady after SyncHello",
            });
        };
        let ready_replica = ready
            .session_replica_id
            .and_then(|replica| parse_replica_id(&replica.value));
        if ready.max_chunk_bytes != self.max_chunk_bytes || ready_replica != Some(self.replica_id) {
            self.channel
                .close(
                    SyncCloseCode::NegotiationFailed,
                    "peer SyncReady differs from negotiated values",
                    correlation_id,
                    Some(deadline),
                )
                .await;
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::NegotiationFailed,
                message: "peer SyncReady differs from negotiated values",
            });
        }
        Ok(())
    }
}

async fn fail_handshake<S, T>(
    mut channel: SessionChannel<S>,
    code: SyncCloseCode,
    message: &'static str,
    correlation_id: &str,
    deadline: Instant,
) -> Result<T, SessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .close(code, message, correlation_id, Some(deadline))
        .await;
    Err(SessionError::LocalProtocol { code, message })
}

fn parse_replica_id(value: &str) -> Option<Uuid> {
    let parsed = Uuid::parse_str(value).ok()?;
    (parsed.get_version_num() == 4 && parsed.to_string() == value).then_some(parsed)
}

#[cfg(test)]
mod tests {
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
                ..
            })
        ));
        assert!(matches!(
            right,
            Err(SessionError::LocalProtocol {
                code: SyncCloseCode::NoReplicaAvailable,
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
                    ..
                })
            ));
        }
    }
}
