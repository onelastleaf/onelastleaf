use super::*;

pub(crate) async fn send_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    observation: SyncObservation<'_>,
    reply_to: Option<u64>,
    max_chunk_bytes: u32,
) -> Result<(u64, RoundResult), RoundError>
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
    let inventory =
        tokio::time::timeout(ROUND_PROGRESS_DEADLINE, replica.capture_replica_inventory())
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
            "object_count": inventory.objects.len(),
            "blob_count": inventory.blobs.len(),
            "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "connection_id": observation.connection_id.to_string(),
            "peer_node_id": observation.peer_node_id.to_string(),
            "direction": observation.direction,
        }),
    );
    send_source_round(
        channel,
        replica,
        &inventory,
        None,
        SyncRoundMode::Normal,
        correlation_id,
        reply_to,
        max_chunk_bytes,
    )
    .await
}

pub(crate) async fn send_bootstrap_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    source: &BootstrapSource,
    correlation_id: &str,
    max_chunk_bytes: u32,
) -> Result<RoundResult, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (_, result) = send_source_round(
        channel,
        replica,
        &source.inventory,
        Some(&source.objects),
        SyncRoundMode::Bootstrap,
        correlation_id,
        None,
        max_chunk_bytes,
    )
    .await?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
async fn send_source_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    inventory: &ReplicaInventory,
    frozen_objects: Option<&BTreeMap<ReplicaObject, ExportedReplicaObject>>,
    mode: SyncRoundMode,
    correlation_id: &str,
    reply_to: Option<u64>,
    max_chunk_bytes: u32,
) -> Result<(u64, RoundResult), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let round_id = Uuid::new_v4().to_string();
    let start_message_id = channel
        .send_progress(
            sync_envelope::Payload::RoundStart(SyncRoundStart {
                round_id: round_id.clone(),
                mode: mode as i32,
            }),
            correlation_id,
            reply_to,
            "round_start_send",
        )
        .await?;
    let batches = inventory_batches(inventory, &round_id, correlation_id)?;
    for (batch_index, (objects, blobs)) in batches.iter().enumerate() {
        channel
            .send_progress(
                sync_envelope::Payload::RoundInventory(SyncRoundInventory {
                    round_id: round_id.clone(),
                    batch_index: u32::try_from(batch_index)
                        .map_err(|_| RoundError::Protocol("too many inventory batches"))?,
                    objects: objects.clone(),
                    blobs: blobs.clone(),
                }),
                correlation_id,
                Some(start_message_id),
                "inventory_send",
            )
            .await?;
    }
    channel
        .send_progress(
            sync_envelope::Payload::RoundInventoryComplete(SyncRoundInventoryComplete {
                round_id: round_id.clone(),
                batch_count: u32::try_from(batches.len())
                    .map_err(|_| RoundError::Protocol("too many inventory batches"))?,
                object_count: u64::try_from(inventory.objects.len())
                    .map_err(|_| RoundError::Protocol("too many inventory objects"))?,
                blob_count: u64::try_from(inventory.blobs.len())
                    .map_err(|_| RoundError::Protocol("too many inventory blobs"))?,
            }),
            correlation_id,
            Some(start_message_id),
            "inventory_complete_send",
        )
        .await?;
    replica.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_inventory_sent",
        correlation_id,
        json!({
            "round_id": &round_id,
            "mode": match mode {
                SyncRoundMode::Normal => "normal",
                SyncRoundMode::Bootstrap => "bootstrap",
                SyncRoundMode::Unspecified => "unspecified",
            },
            "batch_count": batches.len(),
            "object_count": inventory.objects.len(),
            "blob_count": inventory.blobs.len(),
        }),
    );

    loop {
        let envelope = channel.receive_progress("source_response").await?;
        if envelope.correlation_id != correlation_id || envelope.reply_to != Some(start_message_id)
        {
            return Err(RoundError::Protocol(
                "sync round response metadata is invalid",
            ));
        }
        match envelope.payload {
            Some(sync_envelope::Payload::RequestUpdates(request)) => {
                if request.round_id != round_id {
                    return Err(RoundError::Protocol("update request names another round"));
                }
                let object = request
                    .object
                    .as_ref()
                    .ok_or(RoundError::Protocol("update request is missing its object"))
                    .and_then(replica_object_from_proto)?;
                if !inventory
                    .objects
                    .iter()
                    .any(|summary| summary.object == object)
                {
                    send_replica_reject(
                        channel,
                        "",
                        ReplicaTransferRejectCode::UnknownObject,
                        "requested object was not in the inventory",
                        &envelope.correlation_id,
                        Some(envelope.message_id),
                    )
                    .await?;
                    continue;
                }
                let from = version_vector_from_proto(request.from_loro_version_vector.as_ref())?;
                if let Some(objects) = frozen_objects {
                    if from.iter().next().is_some() {
                        send_replica_reject(
                            channel,
                            "",
                            ReplicaTransferRejectCode::InvalidRequest,
                            "bootstrap updates must be requested from an empty version vector",
                            &envelope.correlation_id,
                            Some(envelope.message_id),
                        )
                        .await?;
                        continue;
                    }
                    let exported = objects.get(&object).ok_or(RoundError::Protocol(
                        "bootstrap source is missing an inventoried object",
                    ))?;
                    send_replica_transfer(
                        channel,
                        replica,
                        exported,
                        object,
                        &round_id,
                        max_chunk_bytes,
                        &envelope.correlation_id,
                        envelope.message_id,
                    )
                    .await?;
                } else {
                    let exported = replica.export_replica_updates(object, &from).await?;
                    send_replica_transfer(
                        channel,
                        replica,
                        &exported,
                        object,
                        &round_id,
                        max_chunk_bytes,
                        &envelope.correlation_id,
                        envelope.message_id,
                    )
                    .await?;
                }
            }
            Some(sync_envelope::Payload::RequestBlob(request)) => {
                if request.round_id != round_id || request.sha256.len() != 32 {
                    return Err(RoundError::Protocol("blob request is invalid"));
                }
                let sha256 = crate::replica::lower_hex(&request.sha256);
                if !inventory.blobs.contains_key(&sha256) {
                    send_blob_reject(
                        channel,
                        "",
                        BlobTransferRejectCode::UnknownBlob,
                        "requested blob was not in the inventory",
                        &envelope.correlation_id,
                        Some(envelope.message_id),
                    )
                    .await?;
                    continue;
                }
                send_blob_transfer(
                    channel,
                    replica,
                    &sha256,
                    &round_id,
                    max_chunk_bytes,
                    &envelope.correlation_id,
                    envelope.message_id,
                )
                .await?;
            }
            Some(sync_envelope::Payload::RoundCommitted(committed)) => {
                if committed.round_id != round_id {
                    return Err(RoundError::Protocol("round commit names another round"));
                }
                return Ok((
                    start_message_id,
                    RoundResult {
                        object_count: committed.object_count,
                        blob_count: committed.blob_count,
                        transferred_bytes: committed.transferred_bytes,
                    },
                ));
            }
            Some(sync_envelope::Payload::RoundReject(reject)) => {
                if reject.round_id != round_id {
                    return Err(RoundError::Protocol("round rejection names another round"));
                }
                return Err(RoundError::Rejected(reject.message));
            }
            _ => {
                return Err(RoundError::Protocol(
                    "message is invalid while sourcing a sync round",
                ));
            }
        }
    }
}
