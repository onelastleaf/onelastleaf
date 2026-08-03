use super::*;

pub(super) async fn receive_inventory<S>(
    channel: &mut SessionChannel<S>,
    start_envelope_message_id: u64,
    start: &SyncRoundStart,
    expected_mode: SyncRoundMode,
    correlation_id: &str,
) -> Result<ReceivedInventory, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if SyncRoundMode::try_from(start.mode).ok() != Some(expected_mode)
        || parse_round_id(&start.round_id).is_none()
    {
        return Err(RoundError::Protocol("sync round start is invalid"));
    }
    let mut remote_objects = BTreeMap::new();
    let mut remote_blobs = BTreeMap::new();
    let mut expected_batch = 0_u32;
    loop {
        let envelope = channel.receive(None).await?;
        if envelope.correlation_id != correlation_id
            || envelope.reply_to != Some(start_envelope_message_id)
        {
            return Err(RoundError::Protocol(
                "sync inventory envelope metadata is invalid",
            ));
        }
        match envelope.payload {
            Some(sync_envelope::Payload::RoundInventory(batch)) => {
                if batch.round_id != start.round_id || batch.batch_index != expected_batch {
                    return reject_round(
                        channel,
                        &start.round_id,
                        SyncRoundRejectCode::InvalidInventory,
                        "inventory batch sequence is invalid",
                        correlation_id,
                        start_envelope_message_id,
                    )
                    .await;
                }
                expected_batch = expected_batch
                    .checked_add(1)
                    .ok_or(RoundError::Protocol("inventory batch counter overflowed"))?;
                for summary in batch.objects {
                    let summary = object_summary_from_proto(summary)?;
                    if remote_objects.insert(summary.object, summary).is_some() {
                        return reject_round(
                            channel,
                            &start.round_id,
                            SyncRoundRejectCode::InvalidInventory,
                            "inventory repeats a replica object",
                            correlation_id,
                            start_envelope_message_id,
                        )
                        .await;
                    }
                }
                for blob in batch.blobs {
                    if blob.sha256.len() != 32 {
                        return Err(RoundError::Protocol(
                            "inventory blob hash has an invalid length",
                        ));
                    }
                    let sha256 = crate::replica::lower_hex(&blob.sha256);
                    if remote_blobs.insert(sha256, blob.size_bytes).is_some() {
                        return Err(RoundError::Protocol("inventory repeats a blob"));
                    }
                }
            }
            Some(sync_envelope::Payload::RoundInventoryComplete(complete)) => {
                if complete.round_id != start.round_id
                    || complete.batch_count != expected_batch
                    || complete.object_count
                        != u64::try_from(remote_objects.len()).unwrap_or(u64::MAX)
                    || complete.blob_count != u64::try_from(remote_blobs.len()).unwrap_or(u64::MAX)
                    || !remote_objects.contains_key(&ReplicaObject::Catalog)
                {
                    return reject_round(
                        channel,
                        &start.round_id,
                        SyncRoundRejectCode::InvalidInventory,
                        "inventory completion counts are invalid",
                        correlation_id,
                        start_envelope_message_id,
                    )
                    .await;
                }
                return Ok(ReceivedInventory {
                    objects: remote_objects,
                    blobs: remote_blobs,
                });
            }
            _ => {
                return Err(RoundError::Protocol(
                    "message is invalid while receiving inventory",
                ));
            }
        }
    }
}

#[allow(clippy::type_complexity)]
pub(super) fn inventory_batches(
    inventory: &ReplicaInventory,
    round_id: &str,
    correlation_id: &str,
) -> Result<Vec<(Vec<ProtoObjectSummary>, Vec<BlobRef>)>, RoundError> {
    let objects = inventory
        .objects
        .iter()
        .map(object_summary_to_proto)
        .collect::<Vec<_>>();
    let blobs = inventory
        .blobs
        .iter()
        .map(|(sha256, size_bytes)| {
            Ok(BlobRef {
                sha256: decode_sha256(sha256)?,
                size_bytes: *size_bytes,
            })
        })
        .collect::<Result<Vec<_>, RoundError>>()?;
    let mut batches = Vec::<(Vec<ProtoObjectSummary>, Vec<BlobRef>)>::new();
    let mut current = (Vec::new(), Vec::new());
    for object in objects {
        if current.0.len() + current.1.len() == INVENTORY_BATCH_ITEMS {
            batches.push(std::mem::take(&mut current));
        }
        current.0.push(object);
        if !inventory_batch_fits(round_id, correlation_id, &current) {
            let object = current
                .0
                .pop()
                .expect("the just-added inventory object is present");
            if current.0.is_empty() && current.1.is_empty() {
                return Err(RoundError::Protocol(
                    "one replica-object inventory summary exceeds the frame limit",
                ));
            }
            batches.push(std::mem::take(&mut current));
            current.0.push(object);
            if !inventory_batch_fits(round_id, correlation_id, &current) {
                return Err(RoundError::Protocol(
                    "one replica-object inventory summary exceeds the frame limit",
                ));
            }
        }
    }
    for blob in blobs {
        if current.0.len() + current.1.len() == INVENTORY_BATCH_ITEMS {
            batches.push(std::mem::take(&mut current));
        }
        current.1.push(blob);
        if !inventory_batch_fits(round_id, correlation_id, &current) {
            let blob = current
                .1
                .pop()
                .expect("the just-added inventory blob is present");
            if current.0.is_empty() && current.1.is_empty() {
                return Err(RoundError::Protocol(
                    "one blob inventory summary exceeds the frame limit",
                ));
            }
            batches.push(std::mem::take(&mut current));
            current.1.push(blob);
            if !inventory_batch_fits(round_id, correlation_id, &current) {
                return Err(RoundError::Protocol(
                    "one blob inventory summary exceeds the frame limit",
                ));
            }
        }
    }
    if !current.0.is_empty() || !current.1.is_empty() {
        batches.push(current);
    } else if batches.is_empty() {
        let empty = (Vec::new(), Vec::new());
        if !inventory_batch_fits(round_id, correlation_id, &empty) {
            return Err(RoundError::Protocol(
                "inventory metadata exceeds the frame limit",
            ));
        }
        batches.push(empty);
    }
    Ok(batches)
}

pub(super) fn inventory_batch_fits(
    round_id: &str,
    correlation_id: &str,
    batch: &(Vec<ProtoObjectSummary>, Vec<BlobRef>),
) -> bool {
    SyncEnvelope {
        message_id: u64::MAX,
        reply_to: Some(u64::MAX),
        correlation_id: correlation_id.to_owned(),
        payload: Some(sync_envelope::Payload::RoundInventory(SyncRoundInventory {
            round_id: round_id.to_owned(),
            batch_index: u32::MAX,
            objects: batch.0.clone(),
            blobs: batch.1.clone(),
        })),
    }
    .encoded_len()
        <= MAX_PLAINTEXT
}
