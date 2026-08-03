use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::node::logging::LogLevel;

use super::super::{ReplicaError, types::parse_uuid_v4};

pub(super) fn parse_manifest_uuid(value: &str, field: &'static str) -> Result<Uuid, ReplicaError> {
    parse_uuid_v4(value, field)
        .map_err(|_| ReplicaError::InvalidSnapshot(format!("{field} is not a canonical UUID v4")))
}

pub(super) fn validate_loro_and_collect_peers(
    bytes: &[u8],
    peers: &mut BTreeSet<u64>,
) -> Result<loro::LoroDoc, ReplicaError> {
    let doc = loro::LoroDoc::new();
    doc.import(bytes).map_err(|error| {
        ReplicaError::InvalidSnapshot(format!("Loro snapshot cannot be decoded: {error}"))
    })?;
    peers.extend(doc.oplog_vv().keys().copied());
    Ok(doc)
}

pub(super) fn hex_sha256(bytes: &[u8]) -> String {
    super::super::lower_hex(&Sha256::digest(bytes))
}

pub(super) fn snapshot_failure_level(error: &ReplicaError) -> LogLevel {
    if matches!(
        error,
        ReplicaError::Uninitialized
            | ReplicaError::InvalidArgument(_)
            | ReplicaError::NotFound(_)
            | ReplicaError::AlreadyExists(_)
            | ReplicaError::RevisionConflict(_)
            | ReplicaError::InvalidSnapshot(_)
    ) {
        LogLevel::Warn
    } else {
        LogLevel::Error
    }
}

pub(super) fn elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
