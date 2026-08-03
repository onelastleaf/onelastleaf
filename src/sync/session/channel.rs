use super::*;

pub(crate) struct SessionChannel<S> {
    transport: NoiseTransport<S>,
    next_message_id: u64,
    last_received_message_id: u64,
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
