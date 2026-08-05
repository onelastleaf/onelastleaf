use std::{future::Future, time::Duration};

use crate::{plugin::PluginError, replica::ReplicaError, sync::SyncError};

use super::NodeError;

pub(super) fn in_runtime<T>(
    future: impl Future<Output = Result<T, NodeError>>,
) -> Result<T, NodeError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            NodeError::Internal(format!("cannot initialize Tokio runtime: {error}"))
        })?;
    let result = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

pub(super) fn replica_node_error(error: ReplicaError) -> NodeError {
    match error {
        ReplicaError::Uninitialized => NodeError::Unavailable("no local replica yet".to_owned()),
        ReplicaError::Configuration(message) => NodeError::Config(message),
        ReplicaError::InvalidArgument(message)
        | ReplicaError::NotFound(message)
        | ReplicaError::AlreadyExists(message)
        | ReplicaError::RevisionConflict(message)
        | ReplicaError::InvalidSnapshot(message) => NodeError::Operation(message),
        ReplicaError::Io { operation, source } => NodeError::io(operation, source),
        ReplicaError::CorruptStore(_) | ReplicaError::Store(_) | ReplicaError::Internal(_) => {
            NodeError::Internal(error.to_string())
        }
    }
}

pub(super) fn sync_node_error(error: SyncError) -> NodeError {
    match error {
        SyncError::NotFound(message)
        | SyncError::FailedPrecondition(message)
        | SyncError::Protocol(message) => NodeError::Operation(message),
        SyncError::Unavailable(message) => NodeError::Unavailable(message),
        SyncError::SessionLost(error) => NodeError::Unavailable(error.to_string()),
        SyncError::ProgressTimeout { failure_stage } => NodeError::Unavailable(format!(
            "sync round made no progress during {failure_stage}"
        )),
        SyncError::Store | SyncError::Internal(_) => NodeError::Internal(error.to_string()),
    }
}

pub(super) fn plugin_node_error(error: PluginError) -> NodeError {
    match error {
        PluginError::InvalidArgument(message)
        | PluginError::NotFound(message)
        | PluginError::AlreadyExists(message)
        | PluginError::Aborted(message)
        | PluginError::FailedPrecondition(message) => NodeError::Operation(message),
        PluginError::Io { operation, source } => NodeError::io(operation, source),
        PluginError::CorruptStore(_) | PluginError::Store(_) => {
            NodeError::Internal(error.to_string())
        }
    }
}
