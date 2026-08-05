use super::*;

pub(crate) struct SessionChannel<S> {
    transport: NoiseTransport<S>,
    next_message_id: u64,
    last_received_message_id: u64,
    last_activity: Instant,
    transparent_pings: HashMap<u64, TransparentPing>,
}

struct TransparentPing {
    message_id: u64,
    expires_at: Option<Instant>,
}

impl<S> SessionChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(in crate::sync) fn new(transport: NoiseTransport<S>) -> Self {
        Self {
            transport,
            next_message_id: 1,
            last_received_message_id: 0,
            last_activity: Instant::now(),
            transparent_pings: HashMap::new(),
        }
    }

    pub(crate) fn handshake_hash(&self) -> &[u8; 32] {
        self.transport.handshake_hash()
    }

    pub(crate) fn last_activity(&self) -> Instant {
        self.last_activity
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
                error_code: "empty_local_correlation_id",
                message: "local sync correlation_id is empty",
            });
        }
        let message_id = self.next_message_id;
        self.next_message_id =
            self.next_message_id
                .checked_add(1)
                .ok_or(SessionError::LocalProtocol {
                    code: SyncCloseCode::ResourceExhausted,
                    error_code: "message_id_exhausted",
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
        self.last_activity = Instant::now();
        Ok(message_id)
    }

    pub(crate) async fn send_progress(
        &mut self,
        payload: sync_envelope::Payload,
        correlation_id: &str,
        reply_to: Option<u64>,
        failure_stage: &'static str,
    ) -> Result<u64, SessionError> {
        self.send(
            payload,
            correlation_id,
            reply_to,
            Some(Instant::now() + ROUND_PROGRESS_DEADLINE),
        )
        .await
        .map_err(|error| match error {
            SessionError::Transport(TransportError::DeadlineExceeded) => {
                SessionError::ProgressDeadlineExceeded { failure_stage }
            }
            other => other,
        })
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
                error_code: "invalid_envelope_metadata",
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
        self.last_activity = Instant::now();
        Ok(envelope)
    }

    pub(crate) async fn receive_progress(
        &mut self,
        failure_stage: &'static str,
    ) -> Result<SyncEnvelope, SessionError> {
        loop {
            let envelope = self
                .receive(Some(Instant::now() + ROUND_PROGRESS_DEADLINE))
                .await
                .map_err(|error| match error {
                    SessionError::Transport(TransportError::DeadlineExceeded) => {
                        SessionError::ProgressDeadlineExceeded { failure_stage }
                    }
                    other => other,
                })?;
            match envelope.payload.as_ref() {
                Some(sync_envelope::Payload::Ping(ping)) => {
                    self.send_progress(
                        sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                        &envelope.correlation_id,
                        Some(envelope.message_id),
                        "round_pong_send",
                    )
                    .await?;
                }
                Some(sync_envelope::Payload::Pong(_)) => {
                    if !self.consume_transparent_pong(&envelope)? {
                        return Ok(envelope);
                    }
                }
                _ => return Ok(envelope),
            }
        }
    }

    pub(crate) async fn send_round_keepalive(
        &mut self,
        correlation_id: &str,
        failure_stage: &'static str,
    ) -> Result<(), SessionError> {
        let nonce = self.next_message_id;
        let message_id = self
            .send_progress(
                sync_envelope::Payload::Ping(SyncPing {
                    nonce,
                    sent_at: None,
                }),
                correlation_id,
                None,
                failure_stage,
            )
            .await?;
        self.track_transparent_ping(nonce, message_id, None);
        Ok(())
    }

    pub(crate) fn track_transparent_ping(
        &mut self,
        nonce: u64,
        message_id: u64,
        expires_at: Option<Instant>,
    ) {
        let now = Instant::now();
        self.transparent_pings
            .retain(|_, ping| ping.expires_at.is_none_or(|expires_at| expires_at > now));
        self.transparent_pings.insert(
            nonce,
            TransparentPing {
                message_id,
                expires_at,
            },
        );
    }

    pub(crate) fn consume_transparent_pong(
        &mut self,
        envelope: &SyncEnvelope,
    ) -> Result<bool, SessionError> {
        let Some(sync_envelope::Payload::Pong(pong)) = envelope.payload.as_ref() else {
            return Ok(false);
        };
        let now = Instant::now();
        self.transparent_pings
            .retain(|_, ping| ping.expires_at.is_none_or(|expires_at| expires_at > now));
        let Some(ping) = self.transparent_pings.remove(&pong.nonce) else {
            return Ok(false);
        };
        if envelope.reply_to != Some(ping.message_id) {
            return Err(SessionError::LocalProtocol {
                code: SyncCloseCode::ProtocolViolation,
                error_code: "invalid_transparent_pong_reply",
                message: "sync keepalive reply does not name its request",
            });
        }
        Ok(true)
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
