use super::*;

pub(crate) struct PendingSession<S> {
    pub channel: SessionChannel<S>,
    pub remote: NodeIdentity,
    pub replica_id: Option<Uuid>,
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
                "hello_reply_to_present",
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
                "expected_sync_hello",
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
                "schema_mismatch",
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
                "invalid_max_chunk_bytes",
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
                    "invalid_node_identity",
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
                "self_connection",
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
                            "invalid_replica_id",
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
                    "missing_replica_state",
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
            (Some(local), Some(remote)) if local == remote => {
                (Some(local), SessionReplicaMode::Normal)
            }
            (Some(_), Some(_)) => {
                return fail_handshake(
                    channel,
                    SyncCloseCode::ReplicaMismatch,
                    "replica_mismatch",
                    "peer ReplicaId differs from the local replica",
                    correlation_id,
                    deadline,
                )
                .await;
            }
            (Some(local), None) => (Some(local), SessionReplicaMode::BootstrapSource),
            (None, Some(remote)) => (Some(remote), SessionReplicaMode::BootstrapReceiver),
            (None, None) => (None, SessionReplicaMode::Waiting),
        };
        Ok(Self {
            channel,
            remote,
            replica_id,
            mode,
            max_chunk_bytes: hello.max_chunk_bytes.min(MAX_CHUNK_BYTES),
            bootstrap_correlation_id: match mode {
                SessionReplicaMode::Waiting | SessionReplicaMode::Normal => None,
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
                    session_replica_id: self.replica_id.map(|replica_id| ReplicaId {
                        value: replica_id.to_string(),
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
                error_code: "ready_reply_to_present",
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
                error_code: "bootstrap_correlation_mismatch",
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
                error_code: "expected_sync_ready",
                message: "expected SyncReady after SyncHello",
            });
        };
        let ready_replica = match ready.session_replica_id {
            Some(replica) => match parse_replica_id(&replica.value) {
                Some(replica_id) => Some(replica_id),
                None => {
                    self.channel
                        .close(
                            SyncCloseCode::NegotiationFailed,
                            "peer SyncReady contains an invalid ReplicaId",
                            correlation_id,
                            Some(deadline),
                        )
                        .await;
                    return Err(SessionError::LocalProtocol {
                        code: SyncCloseCode::NegotiationFailed,
                        error_code: "ready_negotiation_mismatch",
                        message: "peer SyncReady contains an invalid ReplicaId",
                    });
                }
            },
            None => None,
        };
        if ready.max_chunk_bytes != self.max_chunk_bytes || ready_replica != self.replica_id {
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
                error_code: "ready_negotiation_mismatch",
                message: "peer SyncReady differs from negotiated values",
            });
        }
        Ok(())
    }
}

async fn fail_handshake<S, T>(
    mut channel: SessionChannel<S>,
    code: SyncCloseCode,
    error_code: &'static str,
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
    Err(SessionError::LocalProtocol {
        code,
        error_code,
        message,
    })
}

fn parse_replica_id(value: &str) -> Option<Uuid> {
    let parsed = Uuid::parse_str(value).ok()?;
    (parsed.get_version_num() == 4 && parsed.to_string() == value).then_some(parsed)
}
