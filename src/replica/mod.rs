//! One logical replica, its SQL recovery store, and its editable working-tree
//! projection.

mod api;
mod classification;
mod model;
mod snapshot;
mod store;
mod types;
mod watcher;

#[cfg(test)]
mod tests;

use std::{fmt, io};

pub use snapshot::{SnapshotInspection, inspect_snapshot, verify_snapshot};
pub use types::{OperationKind, OperationRecord, OperationSource, ReplicaStatus};
pub use watcher::ReplicaRuntime;

fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[derive(Debug)]
pub enum ReplicaError {
    Uninitialized,
    InvalidArgument(String),
    NotFound(String),
    AlreadyExists(String),
    RevisionConflict(String),
    CorruptStore(String),
    InvalidSnapshot(String),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    Store(String),
    Internal(String),
}

impl ReplicaError {
    pub fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Uninitialized => "failed_precondition",
            Self::InvalidArgument(_) => "invalid_argument",
            Self::NotFound(_) => "not_found",
            Self::AlreadyExists(_) => "already_exists",
            Self::RevisionConflict(_) => "revision_conflict",
            Self::CorruptStore(_) => "corrupt_store",
            Self::InvalidSnapshot(_) => "invalid_snapshot",
            Self::Io { .. } => "io",
            Self::Store(_) => "store",
            Self::Internal(_) => "internal",
        }
    }
}

impl fmt::Display for ReplicaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => formatter.write_str("no local replica yet"),
            Self::InvalidArgument(message)
            | Self::NotFound(message)
            | Self::AlreadyExists(message)
            | Self::RevisionConflict(message)
            | Self::CorruptStore(message)
            | Self::InvalidSnapshot(message)
            | Self::Store(message)
            | Self::Internal(message) => formatter.write_str(message),
            Self::Io { operation, source } => write!(formatter, "cannot {operation}: {source}"),
        }
    }
}

impl std::error::Error for ReplicaError {}
