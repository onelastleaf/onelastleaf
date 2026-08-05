use super::*;

pub(crate) async fn receive_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    start_envelope_message_id: u64,
    start: SyncRoundStart,
    observation: SyncObservation<'_>,
    max_chunk_bytes: u32,
) -> Result<RoundResult, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let correlation_id = observation.correlation_id;
    let started = Instant::now();
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_inventory_capture_started",
        correlation_id,
        json!({
            "mode": "normal",
            "connection_id": observation.connection_id.to_string(),
            "peer_node_id": observation.peer_node_id.to_string(),
            "direction": observation.direction,
        }),
    );
    let local = tokio::time::timeout(ROUND_PROGRESS_DEADLINE, replica.capture_replica_inventory())
        .await
        .map_err(|_| {
            RoundError::Session(SessionError::ProgressDeadlineExceeded {
                failure_stage: "inventory_capture",
            })
        })??;
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_inventory_capture_completed",
        correlation_id,
        json!({
            "mode": "normal",
            "object_count": local.objects.len(),
            "blob_count": local.blobs.len(),
            "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "connection_id": observation.connection_id.to_string(),
            "peer_node_id": observation.peer_node_id.to_string(),
            "direction": observation.direction,
        }),
    );
    let remote = receive_inventory(
        channel,
        start_envelope_message_id,
        &start,
        SyncRoundMode::Normal,
        correlation_id,
    )
    .await?;
    let remote_objects = remote.objects;
    let remote_blobs = remote.blobs;
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_inventory_received",
        correlation_id,
        json!({
            "round_id": &start.round_id,
            "mode": "normal",
            "object_count": remote_objects.len(),
            "blob_count": remote_blobs.len(),
        }),
    );

    let local_objects = local
        .objects
        .iter()
        .map(|summary| (summary.object, &summary.version_vector))
        .collect::<BTreeMap<_, _>>();
    let mut object_updates = BTreeMap::new();
    // Request the ordered map in reverse so document updates are staged before
    // the catalog. Candidate construction must not depend on wire arrival order.
    for (object, summary) in remote_objects.iter().rev() {
        let empty = VersionVector::default();
        let local_version = local_objects.get(object).copied().unwrap_or(&empty);
        if !has_updates(&summary.version_vector, local_version) {
            continue;
        }
        let request_message_id = channel
            .send_progress(
                sync_envelope::Payload::RequestUpdates(RequestReplicaUpdates {
                    round_id: start.round_id.clone(),
                    object: Some(replica_object_to_proto(*object)),
                    from_loro_version_vector: Some(version_vector_to_proto(local_version)),
                }),
                correlation_id,
                Some(start_envelope_message_id),
                "replica_update_request_send",
            )
            .await?;
        let (payload, transfer_id, summary, start_message_id) = receive_replica_transfer(
            channel,
            replica,
            *object,
            &start.round_id,
            max_chunk_bytes,
            correlation_id,
            request_message_id,
            false,
        )
        .await?;
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
                    staged_loro_frontier: Some(frontier_to_proto(&summary.frontier)),
                }),
                correlation_id,
                Some(start_message_id),
                "replica_transfer_ack_send",
            )
            .await?;
    }

    let mut blobs = BTreeMap::new();
    for (sha256, size_bytes) in remote_blobs {
        if local.blobs.contains_key(&sha256) {
            continue;
        }
        let request_message_id = channel
            .send_progress(
                sync_envelope::Payload::RequestBlob(RequestBlob {
                    round_id: start.round_id.clone(),
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_envelope_message_id),
                "blob_request_send",
            )
            .await?;
        let (transfer_id, start_id, blob) = receive_blob_transfer(
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
                Some(start_id),
                "blob_transfer_ack_send",
            )
            .await?;
    }

    let (commit, liveness_error) = await_with_round_keepalive(
        channel,
        correlation_id,
        "candidate_commit_keepalive",
        replica.commit_replication_candidate(
            ReplicationCandidate {
                base_generation_id: local.generation_id,
                base_state_token: local.state_token,
                object_updates,
                blobs,
            },
            correlation_id,
        ),
    )
    .await;
    if let Some(error) = liveness_error {
        return Err(RoundError::Session(error));
    }
    let result = match commit {
        Ok(ReplicationCommit::AlreadySatisfied) => RoundResult::default(),
        Ok(ReplicationCommit::Committed {
            object_count,
            blob_count,
            transferred_bytes,
        }) => RoundResult {
            object_count,
            blob_count,
            transferred_bytes,
        },
        Err(ReplicaError::RevisionConflict(_)) => {
            replica.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_candidate_rejected",
                correlation_id,
                json!({
                    "round_id": &start.round_id,
                    "mode": "normal",
                    "error_code": "active_generation_changed",
                }),
            );
            return reject_round(
                channel,
                &start.round_id,
                SyncRoundRejectCode::ActiveGenerationChanged,
                "active generation changed while committing the round",
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
                    "mode": "normal",
                    "error_code": error.code(),
                }),
            );
            return reject_round(
                channel,
                &start.round_id,
                SyncRoundRejectCode::CandidateInvalid,
                "replication candidate validation failed",
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
            "mode": "normal",
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
            "round_commit_send",
        )
        .await?;
    Ok(result)
}
