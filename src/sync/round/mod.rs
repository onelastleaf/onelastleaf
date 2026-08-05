use std::{collections::BTreeMap, fmt, future::Future, time::Instant};

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

use super::{
    SessionChannel, SessionError, SyncObservation,
    session::{ROUND_KEEPALIVE_INTERVAL, ROUND_PROGRESS_DEADLINE},
    transport::MAX_PLAINTEXT,
};

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

async fn await_with_round_keepalive<S, F, T>(
    channel: &mut SessionChannel<S>,
    correlation_id: &str,
    failure_stage: &'static str,
    operation: F,
) -> (T, Option<SessionError>)
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: Future<Output = T>,
{
    tokio::pin!(operation);
    let sleep = tokio::time::sleep(ROUND_KEEPALIVE_INTERVAL);
    tokio::pin!(sleep);
    let mut liveness_error = None;
    loop {
        tokio::select! {
            biased;
            result = &mut operation => return (result, liveness_error),
            _ = &mut sleep, if liveness_error.is_none() => {
                if let Err(error) = channel
                    .send_round_keepalive(correlation_id, failure_stage)
                    .await
                {
                    liveness_error = Some(error);
                } else {
                    sleep.as_mut().reset(tokio::time::Instant::now() + ROUND_KEEPALIVE_INTERVAL);
                }
            }
        }
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

mod convert;
mod inventory;
mod receive;
mod receive_bootstrap;
mod receive_normal;
mod reject;
mod send;
mod source;

#[cfg(test)]
mod tests;

use convert::*;
use inventory::*;
use receive::*;
use reject::*;
use send::*;

pub(crate) use receive_bootstrap::receive_bootstrap_round;
pub(crate) use receive_normal::receive_round;
pub(crate) use source::{send_bootstrap_round, send_round};
