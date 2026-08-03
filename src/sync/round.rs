use std::{collections::BTreeMap, fmt, time::Instant};

use loro::{Frontiers, ID, VersionVector};
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempPath, tempdir};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::{
    node::logging::LogLevel,
    protocol::oll::{
        BlobRef, BlobTransferAck, BlobTransferChunk, BlobTransferComplete, BlobTransferReject,
        BlobTransferRejectCode, BlobTransferStart, CatalogObject, LoroFrontier, LoroId,
        LoroVersionEntry, LoroVersionVector, ReplicaObjectRef,
        ReplicaObjectSummary as ProtoObjectSummary, ReplicaTransferAck, ReplicaTransferChunk,
        ReplicaTransferComplete, ReplicaTransferReject, ReplicaTransferRejectCode,
        ReplicaTransferStart, RequestBlob, RequestReplicaUpdates, SyncEnvelope, SyncRoundCommitted,
        SyncRoundInventory, SyncRoundInventoryComplete, SyncRoundMode, SyncRoundReject,
        SyncRoundRejectCode, SyncRoundStart, replica_object_ref, sync_envelope,
    },
    replica::{
        BootstrapCandidate, BootstrapSource, ExportedReplicaObject, ReplicaError, ReplicaInventory,
        ReplicaObject, ReplicaObjectSummary, ReplicaRuntime, ReplicaUpdateValidationError,
        ReplicationCandidate, ReplicationCommit, StagedBlob,
    },
};

use super::{SessionChannel, SessionError, transport::MAX_PLAINTEXT};

const INVENTORY_BATCH_ITEMS: usize = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RoundResult {
    pub object_count: u64,
    pub blob_count: u64,
    pub transferred_bytes: u64,
}

impl RoundResult {
    pub(crate) fn combine(self, other: Self) -> Self {
        Self {
            object_count: self.object_count.saturating_add(other.object_count),
            blob_count: self.blob_count.saturating_add(other.blob_count),
            transferred_bytes: self
                .transferred_bytes
                .saturating_add(other.transferred_bytes),
        }
    }
}

struct ReceivedInventory {
    objects: BTreeMap<ReplicaObject, ReplicaObjectSummary>,
    blobs: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub(crate) enum RoundError {
    Session(SessionError),
    Replica(ReplicaError),
    Protocol(&'static str),
    Rejected(String),
}

impl fmt::Display for RoundError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(formatter),
            Self::Replica(error) => error.fmt(formatter),
            Self::Protocol(message) => formatter.write_str(message),
            Self::Rejected(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RoundError {}

impl From<SessionError> for RoundError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

impl From<ReplicaError> for RoundError {
    fn from(error: ReplicaError) -> Self {
        Self::Replica(error)
    }
}

enum ChunkError {
    Session(SessionError),
    Sequence,
    Size,
    Store,
}

impl ChunkError {
    fn into_round_error(self) -> RoundError {
        match self {
            Self::Session(error) => RoundError::Session(error),
            Self::Sequence => RoundError::Protocol("transfer chunk sequence is invalid"),
            Self::Size => RoundError::Protocol("transfer size differs from its declaration"),
            Self::Store => RoundError::Protocol("transfer staging failed"),
        }
    }

    fn replica_reject_code(&self) -> Option<ReplicaTransferRejectCode> {
        match self {
            Self::Session(_) => None,
            Self::Sequence => Some(ReplicaTransferRejectCode::ChunkSequence),
            Self::Size => Some(ReplicaTransferRejectCode::SizeMismatch),
            Self::Store => Some(ReplicaTransferRejectCode::StoreFailed),
        }
    }

    fn blob_reject_code(&self) -> Option<BlobTransferRejectCode> {
        match self {
            Self::Session(_) => None,
            Self::Sequence => Some(BlobTransferRejectCode::ChunkSequence),
            Self::Size => Some(BlobTransferRejectCode::SizeMismatch),
            Self::Store => Some(BlobTransferRejectCode::StoreFailed),
        }
    }
}

pub(crate) async fn send_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    correlation_id: &str,
    reply_to: Option<u64>,
    max_chunk_bytes: u32,
) -> Result<(u64, RoundResult), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let inventory = replica.capture_replica_inventory().await?;
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
        .send(
            sync_envelope::Payload::RoundStart(SyncRoundStart {
                round_id: round_id.clone(),
                mode: mode as i32,
            }),
            correlation_id,
            reply_to,
            None,
        )
        .await?;
    let batches = inventory_batches(inventory, &round_id, correlation_id)?;
    for (batch_index, (objects, blobs)) in batches.iter().enumerate() {
        channel
            .send(
                sync_envelope::Payload::RoundInventory(SyncRoundInventory {
                    round_id: round_id.clone(),
                    batch_index: u32::try_from(batch_index)
                        .map_err(|_| RoundError::Protocol("too many inventory batches"))?,
                    objects: objects.clone(),
                    blobs: blobs.clone(),
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
    }
    channel
        .send(
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
            None,
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
        let envelope = channel.receive(None).await?;
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

pub(crate) async fn receive_round<S>(
    channel: &mut SessionChannel<S>,
    replica: &ReplicaRuntime,
    start_envelope_message_id: u64,
    start: SyncRoundStart,
    correlation_id: &str,
    max_chunk_bytes: u32,
) -> Result<RoundResult, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let local = replica.capture_replica_inventory().await?;
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
            .send(
                sync_envelope::Payload::RequestUpdates(RequestReplicaUpdates {
                    round_id: start.round_id.clone(),
                    object: Some(replica_object_to_proto(*object)),
                    from_loro_version_vector: Some(version_vector_to_proto(local_version)),
                }),
                correlation_id,
                Some(start_envelope_message_id),
                None,
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
            .send(
                sync_envelope::Payload::ReplicaTransferAck(ReplicaTransferAck {
                    transfer_id,
                    object: Some(replica_object_to_proto(*object)),
                    staged_loro_frontier: Some(frontier_to_proto(&summary.frontier)),
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
    }

    let mut blobs = BTreeMap::new();
    for (sha256, size_bytes) in remote_blobs {
        if local.blobs.contains_key(&sha256) {
            continue;
        }
        let request_message_id = channel
            .send(
                sync_envelope::Payload::RequestBlob(RequestBlob {
                    round_id: start.round_id.clone(),
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_envelope_message_id),
                None,
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
            .send(
                sync_envelope::Payload::BlobTransferAck(BlobTransferAck {
                    transfer_id,
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_id),
                None,
            )
            .await?;
    }

    let commit = replica
        .commit_replication_candidate(
            ReplicationCandidate {
                base_generation_id: local.generation_id,
                base_state_token: local.state_token,
                object_updates,
                blobs,
            },
            correlation_id,
        )
        .await;
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
        .send(
            sync_envelope::Payload::RoundCommitted(SyncRoundCommitted {
                round_id: start.round_id,
                object_count: result.object_count,
                blob_count: result.blob_count,
                transferred_bytes: result.transferred_bytes,
            }),
            correlation_id,
            Some(start_envelope_message_id),
            None,
        )
        .await?;
    Ok(result)
}

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
            .send(
                sync_envelope::Payload::RequestUpdates(RequestReplicaUpdates {
                    round_id: start.round_id.clone(),
                    object: Some(replica_object_to_proto(*object)),
                    from_loro_version_vector: Some(version_vector_to_proto(
                        &VersionVector::default(),
                    )),
                }),
                correlation_id,
                Some(start_envelope_message_id),
                None,
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
            .send(
                sync_envelope::Payload::ReplicaTransferAck(ReplicaTransferAck {
                    transfer_id,
                    object: Some(replica_object_to_proto(*object)),
                    staged_loro_frontier: Some(frontier_to_proto(&staged.frontier)),
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
    }

    let mut blobs = BTreeMap::new();
    for (sha256, size_bytes) in remote.blobs {
        let request_message_id = channel
            .send(
                sync_envelope::Payload::RequestBlob(RequestBlob {
                    round_id: start.round_id.clone(),
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_envelope_message_id),
                None,
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
            .send(
                sync_envelope::Payload::BlobTransferAck(BlobTransferAck {
                    transfer_id,
                    sha256: decode_sha256(&sha256)?,
                }),
                correlation_id,
                Some(start_message_id),
                None,
            )
            .await?;
    }

    let result = match replica
        .commit_bootstrap_candidate(
            BootstrapCandidate {
                claim_id,
                replica_id,
                object_updates,
                blobs,
            },
            commit_guard,
            writer_node_id,
            correlation_id,
        )
        .await
    {
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
        .send(
            sync_envelope::Payload::RoundCommitted(SyncRoundCommitted {
                round_id: start.round_id,
                object_count: result.object_count,
                blob_count: result.blob_count,
                transferred_bytes: result.transferred_bytes,
            }),
            correlation_id,
            Some(start_envelope_message_id),
            None,
        )
        .await?;
    Ok(result)
}

async fn receive_inventory<S>(
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
fn inventory_batches(
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

fn inventory_batch_fits(
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

#[allow(clippy::too_many_arguments)]
async fn send_replica_transfer<S>(
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

async fn send_blob_transfer<S>(
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

#[allow(clippy::too_many_arguments)]
async fn receive_replica_transfer<S>(
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

async fn receive_blob_transfer<S>(
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
async fn receive_chunks<S>(
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

async fn send_replica_reject<S>(
    channel: &mut SessionChannel<S>,
    transfer_id: &str,
    code: ReplicaTransferRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: Option<u64>,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send(
            sync_envelope::Payload::ReplicaTransferReject(ReplicaTransferReject {
                transfer_id: transfer_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            reply_to,
            None,
        )
        .await?;
    Ok(())
}

async fn send_blob_reject<S>(
    channel: &mut SessionChannel<S>,
    transfer_id: &str,
    code: BlobTransferRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: Option<u64>,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send(
            sync_envelope::Payload::BlobTransferReject(BlobTransferReject {
                transfer_id: transfer_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            reply_to,
            None,
        )
        .await?;
    Ok(())
}

async fn reject_round<S, T>(
    channel: &mut SessionChannel<S>,
    round_id: &str,
    code: SyncRoundRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: u64,
) -> Result<T, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send(
            sync_envelope::Payload::RoundReject(SyncRoundReject {
                round_id: round_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            Some(reply_to),
            None,
        )
        .await?;
    Err(RoundError::Rejected(message.to_owned()))
}

fn replica_object_to_proto(object: ReplicaObject) -> ReplicaObjectRef {
    ReplicaObjectRef {
        object: Some(match object {
            ReplicaObject::Catalog => replica_object_ref::Object::Catalog(CatalogObject {}),
            ReplicaObject::Document(document_id) => {
                replica_object_ref::Object::Document(crate::protocol::oll::DocumentId {
                    value: document_id.to_string(),
                })
            }
        }),
    }
}

fn replica_object_from_proto(object: &ReplicaObjectRef) -> Result<ReplicaObject, RoundError> {
    match object.object.as_ref() {
        Some(replica_object_ref::Object::Catalog(_)) => Ok(ReplicaObject::Catalog),
        Some(replica_object_ref::Object::Document(document)) => {
            let id = Uuid::parse_str(&document.value)
                .map_err(|_| RoundError::Protocol("DocumentId is invalid"))?;
            if id.get_version_num() != 4 || id.to_string() != document.value {
                return Err(RoundError::Protocol("DocumentId is invalid"));
            }
            Ok(ReplicaObject::Document(id))
        }
        None => Err(RoundError::Protocol("replica object reference is empty")),
    }
}

fn object_summary_to_proto(summary: &ReplicaObjectSummary) -> ProtoObjectSummary {
    ProtoObjectSummary {
        object: Some(replica_object_to_proto(summary.object)),
        loro_version_vector: Some(version_vector_to_proto(&summary.version_vector)),
        loro_frontier: Some(frontier_to_proto(&summary.frontier)),
    }
}

fn object_summary_from_proto(
    summary: ProtoObjectSummary,
) -> Result<ReplicaObjectSummary, RoundError> {
    Ok(ReplicaObjectSummary {
        object: summary
            .object
            .as_ref()
            .ok_or(RoundError::Protocol("object summary is missing its object"))
            .and_then(replica_object_from_proto)?,
        version_vector: version_vector_from_proto(summary.loro_version_vector.as_ref())?,
        frontier: frontier_from_proto(summary.loro_frontier.as_ref())?,
    })
}

fn version_vector_to_proto(vector: &VersionVector) -> LoroVersionVector {
    let mut entries = vector
        .iter()
        .map(|(peer_id, counter)| LoroVersionEntry {
            peer_id: *peer_id,
            counter: *counter,
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.peer_id);
    LoroVersionVector { entries }
}

fn version_vector_from_proto(
    vector: Option<&LoroVersionVector>,
) -> Result<VersionVector, RoundError> {
    let entries = vector
        .ok_or(RoundError::Protocol("Loro version vector is missing"))?
        .entries
        .as_slice();
    let mut previous = None;
    let mut decoded = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.counter < 0
            || entry.peer_id == u64::MAX
            || previous.is_some_and(|previous| previous >= entry.peer_id)
        {
            return Err(RoundError::Protocol("Loro version vector is not canonical"));
        }
        previous = Some(entry.peer_id);
        decoded.push((entry.peer_id, entry.counter));
    }
    Ok(decoded.into_iter().collect())
}

fn frontier_to_proto(frontier: &Frontiers) -> LoroFrontier {
    let mut ids = frontier
        .iter()
        .map(|id| LoroId {
            peer_id: id.peer,
            counter: id.counter,
        })
        .collect::<Vec<_>>();
    ids.sort_by_key(|id| (id.peer_id, id.counter));
    LoroFrontier { ids }
}

fn frontier_from_proto(frontier: Option<&LoroFrontier>) -> Result<Frontiers, RoundError> {
    let ids = &frontier
        .ok_or(RoundError::Protocol("Loro frontier is missing"))?
        .ids;
    let mut previous = None;
    let mut decoded = Frontiers::default();
    for id in ids {
        if id.counter < 0
            || id.peer_id == u64::MAX
            || previous.is_some_and(|previous| previous >= (id.peer_id, id.counter))
        {
            return Err(RoundError::Protocol("Loro frontier is not canonical"));
        }
        previous = Some((id.peer_id, id.counter));
        decoded.push(ID::new(id.peer_id, id.counter));
    }
    Ok(decoded)
}

fn has_updates(source: &VersionVector, receiver: &VersionVector) -> bool {
    source
        .iter()
        .any(|(peer, counter)| *counter > receiver.get(peer).copied().unwrap_or_default())
}

fn version_vector_covers(candidate: &VersionVector, required: &VersionVector) -> bool {
    required
        .iter()
        .all(|(peer, counter)| candidate.get(peer).copied().unwrap_or_default() >= *counter)
}

fn chunk_count(size: usize, chunk_bytes: u32) -> Result<u32, RoundError> {
    expected_chunk_count(
        u64::try_from(size).map_err(|_| RoundError::Protocol("transfer size overflowed"))?,
        chunk_bytes,
    )
}

fn expected_chunk_count(size: u64, chunk_bytes: u32) -> Result<u32, RoundError> {
    if chunk_bytes == 0 {
        return Err(RoundError::Protocol("negotiated chunk size is zero"));
    }
    let chunks = if size == 0 {
        0
    } else {
        size.saturating_add(u64::from(chunk_bytes) - 1) / u64::from(chunk_bytes)
    };
    u32::try_from(chunks).map_err(|_| RoundError::Protocol("transfer has too many chunks"))
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, RoundError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(RoundError::Protocol(
            "SHA-256 value is not canonical lower hex",
        ));
    }
    (0..32)
        .map(|index| {
            u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| RoundError::Protocol("SHA-256 value is invalid"))
        })
        .collect()
}

fn parse_round_id(value: &str) -> Option<Uuid> {
    let id = Uuid::parse_str(value).ok()?;
    (id.get_version_num() == 4 && id.to_string() == value).then_some(id)
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use loro::{ExportMode, LoroDoc, UpdateOptions};
    use tempfile::TempDir;
    use tokio::io::duplex;

    use crate::{
        configuration::{NetworkKey, ReplicaStoreConfig},
        node::{NodeIdentity, identity::IdentityCoordinator, logging::NodeLogger},
        protocol::oll::{
            BlobTransferChunk, BlobTransferComplete, BlobTransferStart, ReplicaTransferChunk,
            ReplicaTransferComplete, ReplicaTransferStart,
        },
        sync::{HANDSHAKE_DEADLINE, NoiseTransport, derive_noise_psk},
    };

    use super::*;

    async fn test_channels() -> (
        SessionChannel<tokio::io::DuplexStream>,
        SessionChannel<tokio::io::DuplexStream>,
    ) {
        let key = derive_noise_psk(&NetworkKey::new_for_test(vec![42; 32]));
        let (initiator_stream, responder_stream) = duplex(16 * 1024);
        let deadline = tokio::time::Instant::now() + HANDSHAKE_DEADLINE;
        let (initiator, responder) = tokio::join!(
            NoiseTransport::connect(initiator_stream, &key, deadline),
            NoiseTransport::accept(responder_stream, &key, deadline),
        );
        (
            SessionChannel::new(initiator.unwrap()),
            SessionChannel::new(responder.unwrap()),
        )
    }

    #[test]
    fn loro_versions_require_sorted_unique_entries_and_round_trip() {
        let vector = vec![(7_u64, 4_i32), (2, 3)].into_iter().collect();
        let encoded = version_vector_to_proto(&vector);
        assert_eq!(encoded.entries[0].peer_id, 2);
        assert_eq!(version_vector_from_proto(Some(&encoded)).unwrap(), vector);

        let mut invalid = encoded;
        invalid.entries.reverse();
        assert!(version_vector_from_proto(Some(&invalid)).is_err());
    }

    #[test]
    fn chunk_counts_cover_exact_and_partial_final_chunks() {
        assert_eq!(expected_chunk_count(0, 10).unwrap(), 0);
        assert_eq!(expected_chunk_count(10, 10).unwrap(), 1);
        assert_eq!(expected_chunk_count(11, 10).unwrap(), 2);
        assert!(expected_chunk_count(1, 0).is_err());
    }

    #[test]
    fn inventory_batches_are_split_by_the_encrypted_frame_limit() {
        let version_vector = (1_u64..=500).map(|peer| (peer, 1)).collect();
        let mut objects = vec![ReplicaObjectSummary {
            object: ReplicaObject::Catalog,
            version_vector,
            frontier: Frontiers::default(),
        }];
        for _ in 0..80 {
            objects.push(ReplicaObjectSummary {
                object: ReplicaObject::Document(Uuid::new_v4()),
                version_vector: (1_u64..=500).map(|peer| (peer, 1)).collect(),
                frontier: Frontiers::default(),
            });
        }
        let inventory = ReplicaInventory {
            generation_id: Uuid::new_v4(),
            state_token: [0; 32],
            replica_id: Uuid::new_v4(),
            objects,
            blobs: BTreeMap::new(),
        };
        let round_id = Uuid::new_v4().to_string();
        let batches = inventory_batches(&inventory, &round_id, "inventory-correlation").unwrap();
        assert!(batches.len() > 1);
        assert!(batches.iter().all(|batch| inventory_batch_fits(
            &round_id,
            "inventory-correlation",
            batch
        )));
        assert_eq!(
            batches.iter().map(|batch| batch.0.len()).sum::<usize>(),
            inventory.objects.len()
        );

        let oversized = ReplicaInventory {
            generation_id: Uuid::new_v4(),
            state_token: [0; 32],
            replica_id: Uuid::new_v4(),
            objects: vec![ReplicaObjectSummary {
                object: ReplicaObject::Catalog,
                version_vector: (1_u64..=20_000).map(|peer| (peer, 1)).collect(),
                frontier: Frontiers::default(),
            }],
            blobs: BTreeMap::new(),
        };
        assert!(inventory_batches(&oversized, &round_id, "inventory-correlation").is_err());
    }

    #[tokio::test]
    async fn chunk_staging_classifies_sequence_and_size_rejections() {
        let (mut sender, mut receiver) = test_channels().await;
        sender
            .send(
                sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                    transfer_id: "sequence-transfer".to_owned(),
                    chunk_index: 1,
                    data: vec![1],
                }),
                "sequence-correlation",
                Some(7),
                None,
            )
            .await
            .unwrap();
        let sequence = receive_chunks(
            &mut receiver,
            "sequence-transfer",
            1,
            1,
            8,
            false,
            "sequence-correlation",
            7,
        )
        .await
        .unwrap_err();
        assert!(matches!(sequence, ChunkError::Sequence));
        assert_eq!(
            sequence.blob_reject_code(),
            Some(BlobTransferRejectCode::ChunkSequence)
        );

        let (mut sender, mut receiver) = test_channels().await;
        for _ in 0..2 {
            sender
                .send(
                    sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                        transfer_id: "duplicate-transfer".to_owned(),
                        chunk_index: 0,
                        data: vec![1],
                    }),
                    "duplicate-correlation",
                    Some(8),
                    None,
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            receive_chunks(
                &mut receiver,
                "duplicate-transfer",
                2,
                2,
                1,
                false,
                "duplicate-correlation",
                8,
            )
            .await,
            Err(ChunkError::Sequence)
        ));

        let (mut sender, mut receiver) = test_channels().await;
        sender
            .send(
                sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                    transfer_id: "missing-transfer".to_owned(),
                }),
                "missing-correlation",
                Some(10),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            receive_chunks(
                &mut receiver,
                "missing-transfer",
                1,
                1,
                1,
                false,
                "missing-correlation",
                10,
            )
            .await,
            Err(ChunkError::Sequence)
        ));

        let (sender, mut receiver) = test_channels().await;
        drop(sender);
        assert!(matches!(
            receive_chunks(
                &mut receiver,
                "interrupted-transfer",
                1,
                1,
                1,
                false,
                "interrupted-correlation",
                11,
            )
            .await,
            Err(ChunkError::Session(_))
        ));

        let (mut sender, mut receiver) = test_channels().await;
        let chunk_message_id = sender
            .send(
                sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                    transfer_id: "size-transfer".to_owned(),
                    chunk_index: 0,
                    data: vec![1, 2],
                }),
                "size-correlation",
                Some(9),
                None,
            )
            .await
            .unwrap();
        assert_eq!(chunk_message_id, 1);
        sender
            .send(
                sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                    transfer_id: "size-transfer".to_owned(),
                }),
                "size-correlation",
                Some(9),
                None,
            )
            .await
            .unwrap();
        let size = receive_chunks(
            &mut receiver,
            "size-transfer",
            3,
            1,
            8,
            false,
            "size-correlation",
            9,
        )
        .await
        .unwrap_err();
        assert!(matches!(size, ChunkError::Size));
        assert_eq!(
            size.replica_reject_code(),
            Some(ReplicaTransferRejectCode::SizeMismatch)
        );
    }

    #[tokio::test]
    async fn blob_hash_mismatch_receives_a_typed_rejection() {
        let (mut sender, mut receiver) = test_channels().await;
        let transfer_id = Uuid::new_v4().to_string();
        let round_id = Uuid::new_v4().to_string();
        let expected = Sha256::digest(b"good").to_vec();
        let expected_hex = crate::replica::lower_hex(&expected);
        let sender_task = async {
            let start_id = sender
                .send(
                    sync_envelope::Payload::BlobTransferStart(BlobTransferStart {
                        transfer_id: transfer_id.clone(),
                        round_id: round_id.clone(),
                        sha256: expected,
                        size_bytes: 4,
                        chunk_count: 1,
                    }),
                    "hash-correlation",
                    Some(17),
                    None,
                )
                .await
                .unwrap();
            sender
                .send(
                    sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                        transfer_id: transfer_id.clone(),
                        chunk_index: 0,
                        data: b"evil".to_vec(),
                    }),
                    "hash-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
            sender
                .send(
                    sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                        transfer_id: transfer_id.clone(),
                    }),
                    "hash-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
            sender.receive(None).await.unwrap()
        };
        let receiver_task = receive_blob_transfer(
            &mut receiver,
            &expected_hex,
            4,
            &round_id,
            8,
            "hash-correlation",
            17,
        );
        let (rejection, result) = tokio::join!(sender_task, receiver_task);
        assert!(matches!(
            result,
            Err(RoundError::Protocol("blob transfer SHA-256 mismatch"))
        ));
        let Some(sync_envelope::Payload::BlobTransferReject(rejection)) = rejection.payload else {
            panic!("expected BlobTransferReject");
        };
        assert_eq!(rejection.transfer_id, transfer_id);
        assert_eq!(rejection.code, BlobTransferRejectCode::HashMismatch as i32);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_and_incomplete_loro_payloads_receive_typed_rejections() {
        let deployment = TempDir::new().unwrap();
        let root = deployment.path().join("working");
        let config_root = deployment.path().join("config");
        let log_dir = deployment.path().join("logs");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&config_root).unwrap();
        let identity = NodeIdentity::generate("decode-receiver".parse().unwrap());
        let identities = IdentityCoordinator::new(identity.clone());
        let logger = NodeLogger::open(&log_dir, identity).unwrap();
        let replica = ReplicaRuntime::start(
            config_root,
            root,
            &ReplicaStoreConfig::Sqlite {
                path: deployment.path().join("store/replica.sqlite3"),
            },
            identities,
            logger,
        )
        .await
        .unwrap();

        let (mut sender, mut receiver) = test_channels().await;
        let transfer_id = Uuid::new_v4().to_string();
        let round_id = Uuid::new_v4().to_string();
        let payload = b"not a Loro update".to_vec();
        let payload_hash = Sha256::digest(&payload).to_vec();
        let sender_task = async {
            let start_id = sender
                .send(
                    sync_envelope::Payload::ReplicaTransferStart(ReplicaTransferStart {
                        transfer_id: transfer_id.clone(),
                        round_id: round_id.clone(),
                        object: Some(replica_object_to_proto(ReplicaObject::Catalog)),
                        payload_size: payload.len() as u64,
                        chunk_count: 1,
                        resulting_loro_version_vector: Some(version_vector_to_proto(
                            &VersionVector::default(),
                        )),
                        payload_sha256: payload_hash,
                    }),
                    "decode-correlation",
                    Some(23),
                    None,
                )
                .await
                .unwrap();
            sender
                .send(
                    sync_envelope::Payload::ReplicaTransferChunk(ReplicaTransferChunk {
                        transfer_id: transfer_id.clone(),
                        chunk_index: 0,
                        data: payload,
                    }),
                    "decode-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
            sender
                .send(
                    sync_envelope::Payload::ReplicaTransferComplete(ReplicaTransferComplete {
                        transfer_id: transfer_id.clone(),
                    }),
                    "decode-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
            sender.receive(None).await.unwrap()
        };
        let receiver_task = receive_replica_transfer(
            &mut receiver,
            &replica,
            ReplicaObject::Catalog,
            &round_id,
            1024,
            "decode-correlation",
            23,
            true,
        );
        let (rejection, result) = tokio::join!(sender_task, receiver_task);
        assert!(matches!(
            result,
            Err(RoundError::Protocol(
                "replica transfer Loro payload could not be decoded"
            ))
        ));
        let Some(sync_envelope::Payload::ReplicaTransferReject(rejection)) = rejection.payload
        else {
            panic!("expected ReplicaTransferReject");
        };
        assert_eq!(rejection.transfer_id, transfer_id);
        assert_eq!(
            rejection.code,
            ReplicaTransferRejectCode::LoroDecodeFailed as i32
        );

        let document_id = Uuid::new_v4();
        let dependency_document = LoroDoc::new();
        dependency_document.set_peer_id(42).unwrap();
        let _ = dependency_document.get_map("data");
        let content = dependency_document.get_text("content");
        content
            .update("required history", UpdateOptions::default())
            .unwrap();
        dependency_document.commit();
        let first_version = dependency_document.oplog_vv();
        content
            .update("increment without its dependency", UpdateOptions::default())
            .unwrap();
        dependency_document.commit();
        let payload = dependency_document
            .export(ExportMode::updates(&first_version))
            .unwrap();
        let resulting_version = dependency_document.oplog_vv();
        let payload_hash = Sha256::digest(&payload).to_vec();
        let (mut sender, mut receiver) = test_channels().await;
        let transfer_id = Uuid::new_v4().to_string();
        let round_id = Uuid::new_v4().to_string();
        let sender_task = async {
            let start_id = sender
                .send(
                    sync_envelope::Payload::ReplicaTransferStart(ReplicaTransferStart {
                        transfer_id: transfer_id.clone(),
                        round_id: round_id.clone(),
                        object: Some(replica_object_to_proto(ReplicaObject::Document(
                            document_id,
                        ))),
                        payload_size: payload.len() as u64,
                        chunk_count: chunk_count(payload.len(), 1024).unwrap(),
                        resulting_loro_version_vector: Some(version_vector_to_proto(
                            &resulting_version,
                        )),
                        payload_sha256: payload_hash,
                    }),
                    "import-correlation",
                    Some(29),
                    None,
                )
                .await
                .unwrap();
            for (index, chunk) in payload.chunks(1024).enumerate() {
                sender
                    .send(
                        sync_envelope::Payload::ReplicaTransferChunk(ReplicaTransferChunk {
                            transfer_id: transfer_id.clone(),
                            chunk_index: index as u32,
                            data: chunk.to_vec(),
                        }),
                        "import-correlation",
                        Some(start_id),
                        None,
                    )
                    .await
                    .unwrap();
            }
            sender
                .send(
                    sync_envelope::Payload::ReplicaTransferComplete(ReplicaTransferComplete {
                        transfer_id: transfer_id.clone(),
                    }),
                    "import-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
            sender.receive(None).await.unwrap()
        };
        let receiver_task = receive_replica_transfer(
            &mut receiver,
            &replica,
            ReplicaObject::Document(document_id),
            &round_id,
            1024,
            "import-correlation",
            29,
            true,
        );
        let (rejection, result) = tokio::join!(sender_task, receiver_task);
        assert!(matches!(
            result,
            Err(RoundError::Protocol(
                "replica transfer Loro payload could not be imported"
            ))
        ));
        let Some(sync_envelope::Payload::ReplicaTransferReject(rejection)) = rejection.payload
        else {
            panic!("expected ReplicaTransferReject");
        };
        assert_eq!(rejection.transfer_id, transfer_id);
        assert_eq!(
            rejection.code,
            ReplicaTransferRejectCode::LoroImportFailed as i32
        );

        replica
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}
