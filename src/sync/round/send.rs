use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn send_replica_transfer<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    exported: &ExportedReplicaObject,
    object: ReplicaObject,
    round_id: &str,
    max_chunk_bytes: u32,
    correlation_id: &str,
    request_message_id: u64,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let transfer_id = Uuid::new_v4().to_string();
    let chunk_count = chunk_count(exported.payload.len(), max_chunk_bytes)?;
    let payload_size = u64::try_from(exported.payload.len())
        .map_err(|_| RoundError::Protocol("replica payload is too large"))?;
    let started = Instant::now();
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_replica_transfer_started",
        correlation_id,
        json!({
            "round_id": round_id,
            "transfer_id": &transfer_id,
            "object_kind": match object {
                ReplicaObject::Catalog => "catalog",
                ReplicaObject::Document(_) => "document",
            },
            "document_id": match object {
                ReplicaObject::Catalog => None,
                ReplicaObject::Document(document_id) => Some(document_id.to_string()),
            },
            "bytes": payload_size,
            "chunk_count": chunk_count,
        }),
    );
    let start_message_id = channel
        .send(
            sync_envelope::Payload::ReplicaTransferStart(ReplicaTransferStart {
                transfer_id: transfer_id.clone(),
                round_id: round_id.to_owned(),
                object: Some(replica_object_to_proto(object)),
                payload_size,
                chunk_count,
                resulting_loro_version_vector: Some(version_vector_to_proto(
                    &exported.resulting_version_vector,
                )),
                payload_sha256: exported.payload_sha256.to_vec(),
            }),
            correlation_id,
            Some(request_message_id),
            None,
        )
        .await?;
    for (index, chunk) in exported
        .payload
        .chunks(max_chunk_bytes as usize)
        .enumerate()
    {
        channel
            .send(
                sync_envelope::Payload::ReplicaTransferChunk(ReplicaTransferChunk {
                    transfer_id: transfer_id.clone(),
                    chunk_index: u32::try_from(index)
                        .map_err(|_| RoundError::Protocol("too many replica chunks"))?,
                    data: chunk.to_vec(),
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
    }
    channel
        .send(
            sync_envelope::Payload::ReplicaTransferComplete(ReplicaTransferComplete {
                transfer_id: transfer_id.clone(),
            }),
            correlation_id,
            Some(start_message_id),
            None,
        )
        .await?;
    let response = channel.receive(None).await?;
    if response.correlation_id != correlation_id || response.reply_to != Some(start_message_id) {
        return Err(RoundError::Protocol(
            "replica transfer acknowledgement metadata is invalid",
        ));
    }
    match response.payload {
        Some(sync_envelope::Payload::ReplicaTransferAck(ack))
            if ack.transfer_id == transfer_id
                && ack
                    .object
                    .as_ref()
                    .and_then(|object| replica_object_from_proto(object).ok())
                    == Some(object) =>
        {
            let _ = frontier_from_proto(ack.staged_loro_frontier.as_ref())?;
            replica.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_replica_transfer_completed",
                correlation_id,
                json!({
                    "round_id": round_id,
                    "transfer_id": &transfer_id,
                    "bytes": payload_size,
                    "chunk_count": chunk_count,
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            );
            Ok(())
        }
        Some(sync_envelope::Payload::ReplicaTransferReject(reject))
            if reject.transfer_id == transfer_id =>
        {
            replica.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_replica_transfer_rejected",
                correlation_id,
                json!({
                    "round_id": round_id,
                    "transfer_id": &transfer_id,
                    "error_code": reject.code,
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            );
            Err(RoundError::Rejected(reject.message))
        }
        _ => Err(RoundError::Protocol(
            "replica transfer acknowledgement is invalid",
        )),
    }
}

pub(super) async fn send_blob_transfer<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    sha256: &str,
    round_id: &str,
    max_chunk_bytes: u32,
    correlation_id: &str,
    request_message_id: u64,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let staging = tempdir()
        .map_err(|error| ReplicaError::io("create replication blob staging directory", error))?;
    let staged_path = staging.path().join("blob");
    let size_bytes = replica.stage_replication_blob(sha256, &staged_path).await?;
    let mut staged = tokio::fs::File::open(&staged_path)
        .await
        .map_err(|error| ReplicaError::io("open staged replication blob", error))?;
    let transfer_id = Uuid::new_v4().to_string();
    let chunk_count = expected_chunk_count(size_bytes, max_chunk_bytes)?;
    let started = Instant::now();
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_blob_transfer_started",
        correlation_id,
        json!({
            "round_id": round_id,
            "transfer_id": &transfer_id,
            "sha256": sha256,
            "bytes": size_bytes,
            "chunk_count": chunk_count,
        }),
    );
    let start_message_id = channel
        .send(
            sync_envelope::Payload::BlobTransferStart(BlobTransferStart {
                transfer_id: transfer_id.clone(),
                round_id: round_id.to_owned(),
                sha256: decode_sha256(sha256)?,
                size_bytes,
                chunk_count,
            }),
            correlation_id,
            Some(request_message_id),
            None,
        )
        .await?;
    let mut remaining = size_bytes;
    let mut buffer = vec![0_u8; max_chunk_bytes as usize];
    for index in 0..chunk_count {
        let count = usize::try_from(remaining.min(u64::from(max_chunk_bytes)))
            .map_err(|_| RoundError::Protocol("blob chunk size overflows usize"))?;
        staged
            .read_exact(&mut buffer[..count])
            .await
            .map_err(|error| ReplicaError::io("read staged replication blob", error))?;
        channel
            .send(
                sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                    transfer_id: transfer_id.clone(),
                    chunk_index: index,
                    data: buffer[..count].to_vec(),
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
        remaining -= u64::try_from(count)
            .map_err(|_| RoundError::Protocol("blob chunk size overflows u64"))?;
    }
    if remaining != 0 {
        return Err(RoundError::Protocol(
            "staged blob ended before its declared size",
        ));
    }
    channel
        .send(
            sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                transfer_id: transfer_id.clone(),
            }),
            correlation_id,
            Some(start_message_id),
            None,
        )
        .await?;
    let response = channel.receive(None).await?;
    if response.correlation_id != correlation_id || response.reply_to != Some(start_message_id) {
        return Err(RoundError::Protocol(
            "blob transfer acknowledgement metadata is invalid",
        ));
    }
    match response.payload {
        Some(sync_envelope::Payload::BlobTransferAck(ack))
            if ack.transfer_id == transfer_id && ack.sha256 == decode_sha256(sha256)? =>
        {
            replica.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_blob_transfer_completed",
                correlation_id,
                json!({
                    "round_id": round_id,
                    "transfer_id": &transfer_id,
                    "sha256": sha256,
                    "bytes": size_bytes,
                    "chunk_count": chunk_count,
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            );
            Ok(())
        }
        Some(sync_envelope::Payload::BlobTransferReject(reject))
            if reject.transfer_id == transfer_id =>
        {
            replica.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_blob_transfer_rejected",
                correlation_id,
                json!({
                    "round_id": round_id,
                    "transfer_id": &transfer_id,
                    "sha256": sha256,
                    "error_code": reject.code,
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            );
            Err(RoundError::Rejected(reject.message))
        }
        _ => Err(RoundError::Protocol(
            "blob transfer acknowledgement is invalid",
        )),
    }
}
