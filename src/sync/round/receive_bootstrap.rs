use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn receive_bootstrap_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    start_envelope_message_id: u64,
    start: SyncRoundStart,
    correlation_id: &str,
    max_chunk_bytes: u32,
    claim_id: Uuid,
    replica_id: Uuid,
    commit_guard: &tokio::sync::OwnedMutexGuard<()>,
    writer_node_id: Uuid,
) -> Result<RoundResult, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let remote = receive_inventory(
        channel,
        start_envelope_message_id,
        &start,
        SyncRoundMode::Bootstrap,
        correlation_id,
    )
    .await?;
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_inventory_received",
        correlation_id,
        json!({
            "round_id": &start.round_id,
            "mode": "bootstrap",
            "object_count": remote.objects.len(),
            "blob_count": remote.blobs.len(),
        }),
    );
    let mut object_updates = BTreeMap::new();
    // Request the ordered map in reverse so documents are staged before the
    // catalog. Candidate construction must not depend on wire arrival order.
    for (object, advertised) in remote.objects.iter().rev() {
        let request_message_id = channel
            .send_progress(
                sync_envelope::Payload::RequestUpdates(RequestReplicaUpdates {
                    round_id: start.round_id.clone(),
                    object: Some(replica_object_to_proto(*object)),
                    from_loro_version_vector: Some(version_vector_to_proto(
                        &VersionVector::default(),
                    )),
                }),
                correlation_id,
                Some(start_envelope_message_id),
                "bootstrap_update_request_send",
            )
            .await?;
        let (payload, transfer_id, staged, start_message_id) = receive_replica_transfer(
            channel,
            replica,
            *object,
            &start.round_id,
            max_chunk_bytes,
            correlation_id,
            request_message_id,
            true,
        )
        .await?;
        if staged.version_vector != advertised.version_vector
            || staged.frontier != advertised.frontier
        {
            send_replica_reject(
                channel,
                &transfer_id,
                ReplicaTransferRejectCode::InvalidRequest,
                "bootstrap transfer differs from its frozen inventory",
                correlation_id,
                Some(start_message_id),
            )
            .await?;
            return Err(RoundError::Protocol(
                "bootstrap transfer differs from its frozen inventory",
            ));
        }
        replica.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_replica_transfer_staged",
            correlation_id,
            json!({
                "round_id": &start.round_id,
                "transfer_id": &transfer_id,
                "object_kind": match object {
                    ReplicaObject::Catalog => "catalog",
                    ReplicaObject::Document(_) => "document",
                },
                "document_id": match object {
                    ReplicaObject::Catalog => None,
                    ReplicaObject::Document(document_id) => Some(document_id.to_string()),
                },
                "bytes": payload.len(),
            }),
        );
        object_updates.insert(*object, payload);
        channel
            .send_progress(
                sync_envelope::Payload::ReplicaTransferAck(ReplicaTransferAck {
                    transfer_id,
                    object: Some(replica_object_to_proto(*object)),
                    staged_loro_frontier: Some(frontier_to_proto(&staged.frontier)),
                }),
                correlation_id,
                Some(start_message_id),
                "bootstrap_replica_ack_send",
            )
            .await?;
    }

    let mut blobs = BTreeMap::new();
    for (sha256, size_bytes) in remote.blobs {
        let request_message_id = channel
            .send_progress(
                sync_envelope::Payload::RequestBlob(RequestBlob {
                    round_id: start.round_id.clone(),
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_envelope_message_id),
                "bootstrap_blob_request_send",
            )
            .await?;
        let (transfer_id, start_message_id, blob) = receive_blob_transfer(
            channel,
            &sha256,
            size_bytes,
            &start.round_id,
            max_chunk_bytes,
            correlation_id,
            request_message_id,
        )
        .await?;
        replica.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_blob_transfer_staged",
            correlation_id,
            json!({
                "round_id": &start.round_id,
                "transfer_id": &transfer_id,
                "sha256": &sha256,
                "bytes": blob.size_bytes,
            }),
        );
        blobs.insert(sha256.clone(), blob);
        channel
            .send_progress(
                sync_envelope::Payload::BlobTransferAck(BlobTransferAck {
                    transfer_id,
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_message_id),
                "bootstrap_blob_ack_send",
            )
            .await?;
    }

    let (commit, liveness_error) = await_with_round_keepalive(
        channel,
        correlation_id,
        "bootstrap_candidate_commit_keepalive",
        replica.commit_bootstrap_candidate(
            BootstrapCandidate {
                claim_id,
                replica_id,
                object_updates,
                blobs,
            },
            commit_guard,
            writer_node_id,
            correlation_id,
        ),
    )
    .await;
    if let Some(error) = liveness_error {
        return Err(RoundError::Session(error));
    }
    let result = match commit {
        Ok(ReplicationCommit::Committed {
            object_count,
            blob_count,
            transferred_bytes,
        }) => RoundResult {
            object_count,
            blob_count,
            transferred_bytes,
        },
        Ok(ReplicationCommit::AlreadySatisfied) => RoundResult::default(),
        Err(ReplicaError::RevisionConflict(_)) => {
            replica.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_candidate_rejected",
                correlation_id,
                json!({
                    "round_id": &start.round_id,
                    "mode": "bootstrap",
                    "error_code": "active_generation_changed",
                }),
            );
            return reject_round(
                channel,
                &start.round_id,
                SyncRoundRejectCode::ActiveGenerationChanged,
                "replica initialized while bootstrap was committing",
                correlation_id,
                start_envelope_message_id,
            )
            .await;
        }
        Err(error) => {
            replica.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_candidate_rejected",
                correlation_id,
                json!({
                    "round_id": &start.round_id,
                    "mode": "bootstrap",
                    "error_code": error.code(),
                }),
            );
            return reject_round(
                channel,
                &start.round_id,
                SyncRoundRejectCode::CandidateInvalid,
                "bootstrap candidate validation failed",
                correlation_id,
                start_envelope_message_id,
            )
            .await
            .map_err(|send_error| match send_error {
                RoundError::Rejected(_) => RoundError::Replica(error),
                other => other,
            });
        }
    };
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_candidate_committed",
        correlation_id,
        json!({
            "round_id": &start.round_id,
            "mode": "bootstrap",
            "object_count": result.object_count,
            "blob_count": result.blob_count,
            "bytes": result.transferred_bytes,
        }),
    );
    channel
        .send_progress(
            sync_envelope::Payload::RoundCommitted(SyncRoundCommitted {
                round_id: start.round_id,
                object_count: result.object_count,
                blob_count: result.blob_count,
                transferred_bytes: result.transferred_bytes,
            }),
            correlation_id,
            Some(start_envelope_message_id),
            "bootstrap_round_commit_send",
        )
        .await?;
    Ok(result)
}
