use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive_replica_transfer<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    expected_object: ReplicaObject,
    round_id: &str,
    max_chunk_bytes: u32,
    correlation_id: &str,
    request_message_id: u64,
    exact_resulting_version: bool,
) -> Result<(Vec<u8>, String, ReplicaObjectSummary, u64), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let envelope = channel.receive(None).await?;
    let Some(sync_envelope::Payload::ReplicaTransferStart(start)) = envelope.payload else {
        return Err(RoundError::Protocol("expected ReplicaTransferStart"));
    };
    let object = start
        .object
        .as_ref()
        .ok_or(RoundError::Protocol(
            "replica transfer is missing its object",
        ))
        .and_then(replica_object_from_proto)?;
    if start.round_id != round_id
        || object != expected_object
        || parse_round_id(&start.transfer_id).is_none()
        || start.payload_sha256.len() != 32
        || start.chunk_count != expected_chunk_count(start.payload_size, max_chunk_bytes)?
        || envelope.reply_to != Some(request_message_id)
        || envelope.correlation_id != correlation_id
    {
        send_replica_reject(
            channel,
            &start.transfer_id,
            ReplicaTransferRejectCode::InvalidRequest,
            "replica transfer start is invalid",
            correlation_id,
            Some(envelope.message_id),
        )
        .await?;
        return Err(RoundError::Protocol("replica transfer start is invalid"));
    }
    let (path, digest) = match receive_chunks(
        channel,
        &start.transfer_id,
        start.payload_size,
        start.chunk_count,
        max_chunk_bytes,
        true,
        correlation_id,
        envelope.message_id,
    )
    .await
    {
        Ok(received) => received,
        Err(ChunkError::Session(error)) => return Err(RoundError::Session(error)),
        Err(error) => {
            let code = error
                .replica_reject_code()
                .expect("session errors returned before typed rejection");
            send_replica_reject(
                channel,
                &start.transfer_id,
                code,
                "replica transfer chunks could not be staged",
                correlation_id,
                Some(envelope.message_id),
            )
            .await?;
            return Err(error.into_round_error());
        }
    };
    if digest.as_slice() != start.payload_sha256 {
        send_replica_reject(
            channel,
            &start.transfer_id,
            ReplicaTransferRejectCode::HashMismatch,
            "replica transfer SHA-256 mismatch",
            correlation_id,
            Some(envelope.message_id),
        )
        .await?;
        return Err(RoundError::Protocol("replica transfer SHA-256 mismatch"));
    }
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|error| ReplicaError::io("read staged Loro update", error))?;
    let summary = match replica
        .validate_replica_update(expected_object, &bytes)
        .await
    {
        Ok(summary) => summary,
        Err(error) => {
            let (code, message) = match &error {
                ReplicaUpdateValidationError::Decode => (
                    ReplicaTransferRejectCode::LoroDecodeFailed,
                    "replica transfer Loro payload could not be decoded",
                ),
                ReplicaUpdateValidationError::Import => (
                    ReplicaTransferRejectCode::LoroImportFailed,
                    "replica transfer Loro payload could not be imported",
                ),
                ReplicaUpdateValidationError::Invalid(_) => (
                    ReplicaTransferRejectCode::InvalidRequest,
                    "replica transfer produced an invalid object",
                ),
            };
            send_replica_reject(
                channel,
                &start.transfer_id,
                code,
                message,
                correlation_id,
                Some(envelope.message_id),
            )
            .await?;
            return Err(match error {
                ReplicaUpdateValidationError::Decode => {
                    RoundError::Protocol("replica transfer Loro payload could not be decoded")
                }
                ReplicaUpdateValidationError::Import => {
                    RoundError::Protocol("replica transfer Loro payload could not be imported")
                }
                ReplicaUpdateValidationError::Invalid(error) => RoundError::Replica(error),
            });
        }
    };
    let declared_version = version_vector_from_proto(start.resulting_loro_version_vector.as_ref())?;
    if (exact_resulting_version && declared_version != summary.version_vector)
        || (!exact_resulting_version
            && !version_vector_covers(&summary.version_vector, &declared_version))
    {
        send_replica_reject(
            channel,
            &start.transfer_id,
            ReplicaTransferRejectCode::InvalidRequest,
            "replica transfer resulting version is invalid",
            correlation_id,
            Some(envelope.message_id),
        )
        .await?;
        return Err(RoundError::Protocol(
            "replica transfer resulting version is invalid",
        ));
    }
    Ok((bytes, start.transfer_id, summary, envelope.message_id))
}

pub(super) async fn receive_blob_transfer<S>(
    channel: &mut SessionChannel<S>,
    expected_sha256: &str,
    expected_size: u64,
    round_id: &str,
    max_chunk_bytes: u32,
    correlation_id: &str,
    request_message_id: u64,
) -> Result<(String, u64, StagedBlob), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let envelope = channel.receive(None).await?;
    let Some(sync_envelope::Payload::BlobTransferStart(start)) = envelope.payload else {
        return Err(RoundError::Protocol("expected BlobTransferStart"));
    };
    if start.round_id != round_id
        || parse_round_id(&start.transfer_id).is_none()
        || start.sha256 != decode_sha256(expected_sha256)?
        || start.size_bytes != expected_size
        || start.chunk_count != expected_chunk_count(start.size_bytes, max_chunk_bytes)?
        || envelope.reply_to != Some(request_message_id)
        || envelope.correlation_id != correlation_id
    {
        send_blob_reject(
            channel,
            &start.transfer_id,
            BlobTransferRejectCode::InvalidRequest,
            "blob transfer start is invalid",
            correlation_id,
            Some(envelope.message_id),
        )
        .await?;
        return Err(RoundError::Protocol("blob transfer start is invalid"));
    }
    let (path, digest) = match receive_chunks(
        channel,
        &start.transfer_id,
        start.size_bytes,
        start.chunk_count,
        max_chunk_bytes,
        false,
        correlation_id,
        envelope.message_id,
    )
    .await
    {
        Ok(received) => received,
        Err(ChunkError::Session(error)) => return Err(RoundError::Session(error)),
        Err(error) => {
            let code = error
                .blob_reject_code()
                .expect("session errors returned before typed rejection");
            send_blob_reject(
                channel,
                &start.transfer_id,
                code,
                "blob transfer chunks could not be staged",
                correlation_id,
                Some(envelope.message_id),
            )
            .await?;
            return Err(error.into_round_error());
        }
    };
    if crate::replica::lower_hex(&digest) != expected_sha256 {
        send_blob_reject(
            channel,
            &start.transfer_id,
            BlobTransferRejectCode::HashMismatch,
            "blob transfer SHA-256 mismatch",
            correlation_id,
            Some(envelope.message_id),
        )
        .await?;
        return Err(RoundError::Protocol("blob transfer SHA-256 mismatch"));
    }
    Ok((
        start.transfer_id,
        envelope.message_id,
        StagedBlob {
            path,
            size_bytes: start.size_bytes,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn receive_chunks<S>(
    channel: &mut SessionChannel<S>,
    transfer_id: &str,
    size_bytes: u64,
    chunk_count: u32,
    max_chunk_bytes: u32,
    replica_chunks: bool,
    correlation_id: &str,
    start_message_id: u64,
) -> Result<(TempPath, [u8; 32]), ChunkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let staged = NamedTempFile::new().map_err(|_| ChunkError::Store)?;
    let (file, path) = staged.into_parts();
    let mut file = tokio::fs::File::from_std(file);
    let mut hash = Sha256::new();
    let mut received = 0_u64;
    for expected_index in 0..chunk_count {
        let envelope = channel.receive(None).await.map_err(ChunkError::Session)?;
        if envelope.correlation_id != correlation_id || envelope.reply_to != Some(start_message_id)
        {
            return Err(ChunkError::Sequence);
        }
        let (id, index, data) = match envelope.payload {
            Some(sync_envelope::Payload::ReplicaTransferChunk(chunk)) if replica_chunks => {
                (chunk.transfer_id, chunk.chunk_index, chunk.data)
            }
            Some(sync_envelope::Payload::BlobTransferChunk(chunk)) if !replica_chunks => {
                (chunk.transfer_id, chunk.chunk_index, chunk.data)
            }
            _ => return Err(ChunkError::Sequence),
        };
        if id != transfer_id || index != expected_index || data.len() > max_chunk_bytes as usize {
            return Err(ChunkError::Sequence);
        }
        received = received
            .checked_add(u64::try_from(data.len()).map_err(|_| ChunkError::Size)?)
            .ok_or(ChunkError::Size)?;
        if received > size_bytes {
            return Err(ChunkError::Size);
        }
        hash.update(&data);
        file.write_all(&data).await.map_err(|_| ChunkError::Store)?;
    }
    let envelope = channel.receive(None).await.map_err(ChunkError::Session)?;
    if envelope.correlation_id != correlation_id || envelope.reply_to != Some(start_message_id) {
        return Err(ChunkError::Sequence);
    }
    let complete_id = match envelope.payload {
        Some(sync_envelope::Payload::ReplicaTransferComplete(complete)) if replica_chunks => {
            complete.transfer_id
        }
        Some(sync_envelope::Payload::BlobTransferComplete(complete)) if !replica_chunks => {
            complete.transfer_id
        }
        _ => return Err(ChunkError::Sequence),
    };
    if complete_id != transfer_id || received != size_bytes {
        return Err(ChunkError::Size);
    }
    file.flush().await.map_err(|_| ChunkError::Store)?;
    drop(file);
    Ok((path, hash.finalize().into()))
}
