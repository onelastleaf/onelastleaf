use super::*;

pub(super) async fn request_bidirectional_round(
    channel: &mut SessionChannel<TcpStream>,
    replica: &ReplicaRuntime,
    local_node_id: Uuid,
    observation: SyncObservation<'_>,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let request_message_id = channel
        .send_progress(
            sync_envelope::Payload::RoundRequest(SyncRoundRequest {}),
            observation.correlation_id,
            None,
            "round_request_send",
        )
        .await
        .map_err(|error| round_error_to_sync(RoundError::Session(error)))?;
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_round_request_sent",
        observation.correlation_id,
        json!({
            "connection_id": observation.connection_id.to_string(),
            "peer_node_id": observation.peer_node_id.to_string(),
            "message_id": request_message_id,
        }),
    );
    loop {
        let envelope = channel
            .receive_progress("round_start_receive")
            .await
            .map_err(|error| round_error_to_sync(RoundError::Session(error)))?;
        match envelope.payload {
            Some(sync_envelope::Payload::RoundStart(start)) => {
                if envelope.reply_to != Some(request_message_id)
                    || envelope.correlation_id != observation.correlation_id
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
                    observation,
                    max_chunk_bytes,
                )
                .await
                .map_err(round_error_to_sync)?;
                let (_, sent) = send_round(
                    channel,
                    replica,
                    observation,
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
                replica.logger.emit(
                    LogLevel::Info,
                    "oll::sync",
                    "sync_round_request_received",
                    &envelope.correlation_id,
                    json!({
                        "connection_id": observation.connection_id.to_string(),
                        "peer_node_id": observation.peer_node_id.to_string(),
                        "message_id": envelope.message_id,
                    }),
                );
                if local_node_id < observation.peer_node_id {
                    continue;
                }
                let source_observation = SyncObservation {
                    correlation_id: &envelope.correlation_id,
                    ..observation
                };
                return source_bidirectional_round(
                    channel,
                    replica,
                    source_observation,
                    envelope.message_id,
                    max_chunk_bytes,
                )
                .await;
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
    observation: SyncObservation<'_>,
    request_message_id: u64,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let (start_message_id, sent) = send_round(
        channel,
        replica,
        observation,
        Some(request_message_id),
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    let envelope = channel
        .receive_progress("reverse_round_start_receive")
        .await
        .map_err(|error| round_error_to_sync(RoundError::Session(error)))?;
    let Some(sync_envelope::Payload::RoundStart(start)) = envelope.payload else {
        return Err(SyncError::Protocol(
            "peer did not begin the reverse sync round".to_owned(),
        ));
    };
    if envelope.reply_to != Some(start_message_id)
        || envelope.correlation_id != observation.correlation_id
    {
        return Err(SyncError::Protocol(
            "reverse sync round does not match its initiating round".to_owned(),
        ));
    }
    let received = receive_round(
        channel,
        replica,
        envelope.message_id,
        start,
        observation,
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    Ok(sent.combine(received))
}
