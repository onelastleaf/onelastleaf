use std::{
    io::{self, BufRead, Write},
    path::Path,
};

use serde_json::json;

use crate::{
    cli::{ConfirmationRequirement, OutputFormat},
    node::{admin, lock::admin_socket_path, logging::new_correlation_id},
    protocol::oll::{
        GetStatusResponse, InspectReplicaDocumentResponse, NodeLifecycleState,
        PeerConnectionDirection, PeerConnectionState, PeerSyncOutcome, ReplicaOperationKind,
        ReplicaOperationSource, ReplicaState as ProtoReplicaState,
    },
    replica::{SnapshotInspection, inspect_snapshot, verify_snapshot},
};

use super::{NodeError, blocking::replica_node_error};

pub(super) fn show_snapshot_inspection(path: &Path, as_json: bool) -> Result<(), NodeError> {
    let inspection = inspect_snapshot(path).map_err(replica_node_error)?;
    if as_json {
        println!(
            "{}",
            serde_json::to_string(&inspection).map_err(|_| {
                NodeError::Internal("cannot serialize snapshot inspection".to_owned())
            })?
        );
    } else {
        print_snapshot_inspection(&inspection);
    }
    Ok(())
}

fn print_snapshot_inspection(inspection: &SnapshotInspection) {
    println!("format: {}", inspection.format);
    println!("format_version: {}", inspection.format_version);
    println!("snapshot_id: {}", inspection.snapshot_id);
    println!("replica_id: {}", inspection.replica_id);
    println!("created_at: {}", inspection.created_at);
    println!("live_documents: {}", inspection.live_documents);
    println!("tombstoned_documents: {}", inspection.tombstoned_documents);
    println!("blobs: {}", inspection.blobs);
    println!("catalog_bytes: {}", inspection.catalog_bytes);
    println!("document_bytes: {}", inspection.document_bytes);
    println!("blob_bytes: {}", inspection.blob_bytes);
}

pub(super) fn verify_local_snapshot(path: &Path) -> Result<(), NodeError> {
    let inspection = verify_snapshot(path).map_err(replica_node_error)?;
    println!("verified snapshot {}", inspection.snapshot_id);
    Ok(())
}

pub(super) async fn inspect_replica_document(
    config_root: &Path,
    document: &Path,
) -> Result<(), NodeError> {
    let response = admin::inspect_replica_document(
        &admin_socket_path(config_root),
        document,
        new_correlation_id(),
    )
    .await?;
    print_document_inspection(&response)
}

fn print_document_inspection(response: &InspectReplicaDocumentResponse) -> Result<(), NodeError> {
    let catalog_node_id = response
        .catalog_node_id
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted catalog_node_id".to_owned()))?;
    let catalog_revision = response
        .catalog_revision
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted catalog_revision".to_owned()))?;
    let document_id = response
        .document_id
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted document_id".to_owned()))?;
    let document_revision = response
        .document_revision
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted document_revision".to_owned()))?;
    let path = response
        .path
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted document path".to_owned()))?;
    println!("catalog_node_id: {}", catalog_node_id.value);
    println!("catalog_revision: {}", encode_hex(&catalog_revision.token));
    println!("document_id: {}", document_id.value);
    println!(
        "document_revision: {}",
        encode_hex(&document_revision.token)
    );
    println!("path: {}", path.value);
    println!("media_type: {}", response.media_type);
    println!("encoding: {}", response.encoding);
    println!("has_byte_order_mark: {}", response.has_byte_order_mark);
    println!("size_bytes: {}", response.size_bytes);
    Ok(())
}

pub(super) async fn show_replica_operations(
    config_root: &Path,
    document: &Path,
    limit: Option<usize>,
    format: OutputFormat,
) -> Result<(), NodeError> {
    let response = admin::list_replica_operations(
        &admin_socket_path(config_root),
        document,
        limit,
        new_correlation_id(),
    )
    .await?;
    let operations = response
        .operations
        .iter()
        .map(operation_json)
        .collect::<Result<Vec<_>, _>>()?;
    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string(&json!({ "operations": operations })).map_err(|_| {
                    NodeError::Internal("cannot serialize replica operation history".to_owned())
                })?
            );
        }
        OutputFormat::Text => {
            for operation in operations {
                println!(
                    "{} {} {} operation_id={} catalog_node_id={} document_id={} path_before={} path_after={} correlation_id={}",
                    operation["timestamp"].as_str().unwrap_or(""),
                    operation["source"].as_str().unwrap_or("unknown"),
                    operation["kind"].as_str().unwrap_or("unknown"),
                    operation["operation_id"].as_str().unwrap_or(""),
                    operation["catalog_node_id"].as_str().unwrap_or(""),
                    operation["document_id"].as_str().unwrap_or(""),
                    operation["path_before"].as_str().unwrap_or("-"),
                    operation["path_after"].as_str().unwrap_or("-"),
                    operation["correlation_id"].as_str().unwrap_or(""),
                );
            }
        }
    }
    Ok(())
}

fn operation_json(
    operation: &crate::protocol::oll::ReplicaOperation,
) -> Result<serde_json::Value, NodeError> {
    let timestamp = operation
        .timestamp
        .as_ref()
        .map(format_timestamp)
        .ok_or_else(|| NodeError::Internal("daemon omitted operation timestamp".to_owned()))?;
    let source = match ReplicaOperationSource::try_from(operation.source)
        .unwrap_or(ReplicaOperationSource::Unspecified)
    {
        ReplicaOperationSource::Filesystem => "filesystem",
        ReplicaOperationSource::Plugin => "plugin",
        ReplicaOperationSource::Sync => "sync",
        ReplicaOperationSource::SnapshotImport => "snapshot_import",
        ReplicaOperationSource::Unspecified => {
            return Err(NodeError::Internal(
                "daemon returned an unspecified operation source".to_owned(),
            ));
        }
    };
    let kind = match ReplicaOperationKind::try_from(operation.kind)
        .unwrap_or(ReplicaOperationKind::Unspecified)
    {
        ReplicaOperationKind::Create => "create",
        ReplicaOperationKind::Update => "update",
        ReplicaOperationKind::Move => "move",
        ReplicaOperationKind::Delete => "delete",
        ReplicaOperationKind::Replace => "replace",
        ReplicaOperationKind::Unspecified => {
            return Err(NodeError::Internal(
                "daemon returned an unspecified operation kind".to_owned(),
            ));
        }
    };
    let catalog_node_id = operation
        .catalog_node_id
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted operation CatalogNodeId".to_owned()))?;
    let document_id = operation
        .document_id
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted operation DocumentId".to_owned()))?;
    Ok(json!({
        "timestamp": timestamp,
        "operation_id": operation.operation_id,
        "source": source,
        "kind": kind,
        "catalog_node_id": catalog_node_id.value,
        "document_id": document_id.value,
        "path_before": operation.path_before.as_ref().map(|path| &path.value),
        "path_after": operation.path_after.as_ref().map(|path| &path.value),
        "correlation_id": operation.correlation_id,
    }))
}

pub(super) async fn export_replica(config_root: &Path, output: &Path) -> Result<(), NodeError> {
    let response = admin::export_replica(
        &admin_socket_path(config_root),
        output,
        new_correlation_id(),
    )
    .await?;
    let replica_id = response
        .replica_id
        .ok_or_else(|| NodeError::Internal("daemon omitted exported ReplicaId".to_owned()))?;
    println!(
        "exported snapshot {} for replica {}",
        response.snapshot_id, replica_id.value
    );
    Ok(())
}

pub(super) async fn import_replica(config_root: &Path, snapshot: &Path) -> Result<(), NodeError> {
    let response = admin::import_replica(
        &admin_socket_path(config_root),
        snapshot,
        new_correlation_id(),
    )
    .await?;
    let replica_id = response
        .replica_id
        .ok_or_else(|| NodeError::Internal("daemon omitted imported ReplicaId".to_owned()))?;
    println!(
        "imported snapshot {} as replica {}",
        response.snapshot_id, replica_id.value
    );
    Ok(())
}

pub(super) async fn ping_peer(
    config_root: &Path,
    node_name: &crate::cli::NodeName,
) -> Result<(), NodeError> {
    let response = admin::ping_peer(
        &admin_socket_path(config_root),
        node_name,
        new_correlation_id(),
    )
    .await?;
    let identity = response
        .node
        .ok_or_else(|| NodeError::Internal("daemon omitted pinged NodeIdentity".to_owned()))?;
    let node_id = identity
        .node_id
        .ok_or_else(|| NodeError::Internal("daemon omitted pinged NodeId".to_owned()))?;
    let node_name = identity
        .node_name
        .ok_or_else(|| NodeError::Internal("daemon omitted pinged NodeName".to_owned()))?;
    println!(
        "{} ({}) replied in {} ms",
        node_name.value, node_id.value, response.round_trip_millis
    );
    Ok(())
}

pub(super) async fn synchronize_peers(
    config_root: &Path,
    node_name: Option<&crate::cli::NodeName>,
    total_attempts: u32,
) -> Result<(), NodeError> {
    let response = admin::synchronize_peers(
        &admin_socket_path(config_root),
        node_name,
        total_attempts,
        new_correlation_id(),
    )
    .await?;
    let mut failed = false;
    for peer in response.peers {
        let target = peer
            .node
            .as_ref()
            .and_then(|identity| identity.node_name.as_ref())
            .map(|name| name.value.clone())
            .or_else(|| peer.connect_target.clone())
            .unwrap_or_else(|| "unknown-peer".to_owned());
        match PeerSyncOutcome::try_from(peer.outcome).unwrap_or(PeerSyncOutcome::Unspecified) {
            PeerSyncOutcome::Synchronized => println!(
                "{target}: synchronized in {} attempt(s), {} object(s), {} blob(s), {} bytes",
                peer.attempts_used, peer.object_count, peer.blob_count, peer.transferred_bytes
            ),
            PeerSyncOutcome::AlreadySatisfied => println!(
                "{target}: already synchronized ({} attempt(s))",
                peer.attempts_used
            ),
            PeerSyncOutcome::Failed => {
                failed = true;
                println!(
                    "{target}: failed after {} attempt(s): {}",
                    peer.attempts_used, peer.error_message
                );
            }
            PeerSyncOutcome::Unspecified => {
                return Err(NodeError::Internal(
                    "daemon returned an unspecified synchronization outcome".to_owned(),
                ));
            }
        }
    }
    if failed {
        Err(NodeError::Operation(
            "one or more peers failed to synchronize".to_owned(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn confirm_replica_import() -> Result<bool, NodeError> {
    for requirement in [
        ConfirmationRequirement::ReplicaBackupCreated,
        ConfirmationRequirement::ReplicaReplacementApproved,
    ] {
        let mut stderr = io::stderr().lock();
        write!(stderr, "oll: {} [y/N] ", requirement.prompt())
            .map_err(|error| NodeError::io("write replica import confirmation", error))?;
        stderr
            .flush()
            .map_err(|error| NodeError::io("flush replica import confirmation", error))?;
        drop(stderr);

        let mut answer = String::new();
        let count = io::stdin()
            .lock()
            .read_line(&mut answer)
            .map_err(|error| NodeError::io("read replica import confirmation", error))?;
        if count == 0 || !matches!(answer.trim(), "y" | "yes") {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) async fn show_status(config_root: &Path, as_json: bool) -> Result<(), NodeError> {
    let status = admin::get_status(&admin_socket_path(config_root), new_correlation_id()).await?;
    if as_json {
        println!("{}", status_json(&status)?);
    } else {
        print_status(&status)?;
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn status_json(status: &GetStatusResponse) -> Result<serde_json::Value, NodeError> {
    let node = status.node.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node identity".to_owned())
    })?;
    let node_id = node.node_id.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node ID".to_owned())
    })?;
    let node_name = node.node_name.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node name".to_owned())
    })?;
    let peers = status
        .peers
        .iter()
        .map(|peer| {
            json!({
                "connect_target": peer.connect_target,
                "direction": peer_direction_name(peer.direction),
                "connection_state": peer_state_name(peer.connection_state),
                "node": peer.node.as_ref().map(|node| json!({
                    "node_id": node.node_id.as_ref().map(|value| value.value.clone()),
                    "node_name": node.node_name.as_ref().map(|value| value.value.clone()),
                })),
            })
        })
        .collect::<Vec<_>>();
    let (replica_state, replica_id) = replica_status_fields(status)?;
    Ok(json!({
        "node_id": node_id.value,
        "node_name": node_name.value,
        "lifecycle": lifecycle_name(status.lifecycle),
        "started_at": status.started_at.as_ref().map(format_timestamp),
        "process_id": status.process_id,
        "configured_listen_address": status.configured_listen_address,
        "replica_state": replica_state,
        "replica_id": replica_id,
        "peers": peers,
    }))
}

fn print_status(status: &GetStatusResponse) -> Result<(), NodeError> {
    let node = status.node.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node identity".to_owned())
    })?;
    let node_id = node.node_id.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node ID".to_owned())
    })?;
    let node_name = node.node_name.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned status without a node name".to_owned())
    })?;
    println!("Node: {}", node_name.value);
    println!("Node ID: {}", node_id.value);
    println!("Lifecycle: {}", lifecycle_name(status.lifecycle));
    let (replica_state, replica_id) = replica_status_fields(status)?;
    println!("Replica: {replica_state}");
    if let Some(replica_id) = replica_id {
        println!("Replica ID: {replica_id}");
    }
    if let Some(started_at) = status.started_at.as_ref() {
        println!("Started: {}", format_timestamp(started_at));
    }
    println!("Process: {}", status.process_id);
    println!(
        "Listen: {}",
        status
            .configured_listen_address
            .as_deref()
            .unwrap_or("not configured")
    );
    if status.peers.is_empty() {
        println!("Peers: none");
    } else {
        println!("Peers:");
        for peer in &status.peers {
            let label = peer
                .connect_target
                .as_deref()
                .or_else(|| {
                    peer.node
                        .as_ref()
                        .and_then(|node| node.node_name.as_ref())
                        .map(|name| name.value.as_str())
                })
                .unwrap_or("inbound peer");
            println!(
                "  {} ({}, {})",
                label,
                peer_direction_name(peer.direction),
                peer_state_name(peer.connection_state)
            );
        }
    }
    Ok(())
}

fn replica_status_fields(
    status: &GetStatusResponse,
) -> Result<(&'static str, Option<&str>), NodeError> {
    let state =
        ProtoReplicaState::try_from(status.replica_state).unwrap_or(ProtoReplicaState::Unspecified);
    match (state, status.replica_id.as_ref()) {
        (ProtoReplicaState::Uninitialized, None) => Ok(("uninitialized", None)),
        (ProtoReplicaState::InitializedEmpty, Some(replica_id)) => {
            Ok(("initialized_empty", Some(replica_id.value.as_str())))
        }
        (ProtoReplicaState::InitializedPopulated, Some(replica_id)) => {
            Ok(("initialized_populated", Some(replica_id.value.as_str())))
        }
        _ => Err(NodeError::Internal(
            "daemon returned an inconsistent replica status".to_owned(),
        )),
    }
}

pub(super) async fn set_log_filter(
    config_root: &Path,
    target: crate::cli::LogTarget,
    level: crate::cli::LogFilterLevel,
) -> Result<(), NodeError> {
    let response = admin::set_log_filter(
        &admin_socket_path(config_root),
        &target,
        level,
        new_correlation_id(),
    )
    .await?;
    let level = crate::protocol::oll::LogLevel::try_from(response.level)
        .map(|level| {
            level
                .as_str_name()
                .trim_start_matches("LOG_LEVEL_")
                .to_ascii_lowercase()
        })
        .unwrap_or_else(|_| "unknown".to_owned());
    println!("updated live log filter {}={level}", response.target);
    Ok(())
}

fn lifecycle_name(value: i32) -> &'static str {
    match NodeLifecycleState::try_from(value).unwrap_or(NodeLifecycleState::Unspecified) {
        NodeLifecycleState::Starting => "starting",
        NodeLifecycleState::Running => "running",
        NodeLifecycleState::Stopping => "stopping",
        NodeLifecycleState::Unspecified => "unknown",
    }
}

fn peer_state_name(value: i32) -> &'static str {
    match PeerConnectionState::try_from(value).unwrap_or(PeerConnectionState::Unspecified) {
        PeerConnectionState::Pending => "pending",
        PeerConnectionState::Connecting => "connecting",
        PeerConnectionState::Ready => "ready",
        PeerConnectionState::Backoff => "backoff",
        PeerConnectionState::Closing => "closing",
        PeerConnectionState::Unspecified => "unknown",
    }
}

fn peer_direction_name(value: i32) -> &'static str {
    match PeerConnectionDirection::try_from(value).unwrap_or(PeerConnectionDirection::Unspecified) {
        PeerConnectionDirection::Outbound => "outbound",
        PeerConnectionDirection::Inbound => "inbound",
        PeerConnectionDirection::Unspecified => "unknown",
    }
}

fn format_timestamp(timestamp: &prost_types::Timestamp) -> String {
    let fallback = || format!("{}.{:09}Z", timestamp.seconds, timestamp.nanos);
    let Ok(time) = time::OffsetDateTime::from_unix_timestamp(timestamp.seconds) else {
        return fallback();
    };
    let Ok(time) = time.replace_nanosecond(timestamp.nanos.max(0) as u32) else {
        return fallback();
    };
    time.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| fallback())
}
