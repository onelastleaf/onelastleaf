use super::*;

pub(super) async fn request_bidirectional_round(
    channel: &mut SessionChannel<TcpStream>,
    replica: &ReplicaRuntime,
    local_node_id: Uuid,
    remote_node_id: Uuid,
    correlation_id: &str,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let request_message_id = channel
        .send(
            sync_envelope::Payload::RoundRequest(SyncRoundRequest {}),
            correlation_id,
            None,
            None,
        )
        .await
        .map_err(|error| SyncError::Unavailable(error.to_string()))?;
    loop {
        let envelope = channel.receive(None).await.map_err(|error| {
            SyncError::Protocol(format!("cannot receive requested sync round: {error}"))
        })?;
        match envelope.payload {
            Some(sync_envelope::Payload::RoundStart(start)) => {
                if envelope.reply_to != Some(request_message_id)
                    || envelope.correlation_id != correlation_id
                {
                    return Err(SyncError::Protocol(
                        "requested sync round does not name its request".to_owned(),
                    ));
                }
                let received = receive_round(
                    channel,
                    replica,
                    envelope.message_id,
                    start,
                    correlation_id,
                    max_chunk_bytes,
                )
                .await
                .map_err(round_error_to_sync)?;
                let (_, sent) = send_round(
                    channel,
                    replica,
                    correlation_id,
                    Some(envelope.message_id),
                    max_chunk_bytes,
                )
                .await
                .map_err(round_error_to_sync)?;
                return Ok(received.combine(sent));
            }
            Some(sync_envelope::Payload::RoundRequest(_)) => {
                if envelope.reply_to.is_some() {
                    return Err(SyncError::Protocol(
                        "sync round request must not reply to another message".to_owned(),
                    ));
                }
                if local_node_id < remote_node_id {
                    continue;
                }
                return source_bidirectional_round(
                    channel,
                    replica,
                    &envelope.correlation_id,
                    envelope.message_id,
                    max_chunk_bytes,
                )
                .await;
            }
            Some(sync_envelope::Payload::Ping(ping)) => {
                channel
                    .send(
                        sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                        &envelope.correlation_id,
                        Some(envelope.message_id),
                        None,
                    )
                    .await
                    .map_err(|error| SyncError::Unavailable(error.to_string()))?;
            }
            _ => {
                return Err(SyncError::Protocol(
                    "message is invalid while waiting for a requested sync round".to_owned(),
                ));
            }
        }
    }
}

pub(super) async fn source_bidirectional_round(
    channel: &mut SessionChannel<TcpStream>,
    replica: &ReplicaRuntime,
    correlation_id: &str,
    request_message_id: u64,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let (start_message_id, sent) = send_round(
        channel,
        replica,
        correlation_id,
        Some(request_message_id),
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    let envelope = channel.receive(None).await.map_err(|error| {
        SyncError::Protocol(format!("cannot receive reverse sync round: {error}"))
    })?;
    let Some(sync_envelope::Payload::RoundStart(start)) = envelope.payload else {
        return Err(SyncError::Protocol(
            "peer did not begin the reverse sync round".to_owned(),
        ));
    };
    if envelope.reply_to != Some(start_message_id) || envelope.correlation_id != correlation_id {
        return Err(SyncError::Protocol(
            "reverse sync round does not match its initiating round".to_owned(),
        ));
    }
    let received = receive_round(
        channel,
        replica,
        envelope.message_id,
        start,
        correlation_id,
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    Ok(sent.combine(received))
}
