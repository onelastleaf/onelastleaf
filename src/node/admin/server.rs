use std::{
    convert::TryFrom,
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{net::UnixListener, sync::watch, time::Instant};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::{
    cli::LogTarget,
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{
            AdminCallContext, AdminShutdownRequest, AdminShutdownResponse, CatalogNodeId,
            CatalogRevision, DocumentId, DocumentPath, DocumentRevision, ExportReplicaRequest,
            ExportReplicaResponse, GetStatusRequest, GetStatusResponse, ImportReplicaRequest,
            ImportReplicaResponse, InspectReplicaDocumentRequest, InspectReplicaDocumentResponse,
            ListReplicaOperationsRequest, ListReplicaOperationsResponse, LogLevel as ProtoLogLevel,
            NativePath, PingPeerRequest, PingPeerResponse, ReplicaId, ReplicaOperation,
            ReplicaOperationKind, ReplicaOperationSource, ReplicaState as ProtoReplicaState,
            SetLogFilterRequest, SetLogFilterResponse, SynchronizePeersRequest,
            SynchronizePeersResponse,
            admin_server::{Admin, AdminServer},
        },
    },
    replica::{OperationKind, OperationRecord, OperationSource, ReplicaError, ReplicaStatus},
    sync::SyncError,
};

use super::{AdminState, ShutdownNotice};
use crate::node::{logging::LogLevel, runtime::NodeError};

#[derive(Clone)]
struct AdminService {
    state: Arc<AdminState>,
}

impl AdminService {
    #[allow(clippy::result_large_err)]
    fn validate_context(&self, context: Option<AdminCallContext>) -> Result<String, Status> {
        let context =
            context.ok_or_else(|| Status::invalid_argument("missing Admin call context"))?;
        if context.protocol_schema_sha256.as_slice() != PROTOCOL_SCHEMA_SHA256 {
            return Err(Status::failed_precondition(
                "protocol schema fingerprint differs; restart the running daemon",
            ));
        }
        let trace = context
            .trace
            .ok_or_else(|| Status::invalid_argument("missing Admin trace context"))?;
        if trace.correlation_id.is_empty() {
            return Err(Status::invalid_argument(
                "Admin correlation_id must not be empty",
            ));
        }
        Ok(trace.correlation_id)
    }

    #[allow(clippy::result_large_err)]
    fn require_serving(&self) -> Result<(), Status> {
        if self.state.is_stopping() {
            Err(Status::unavailable("daemon is stopping"))
        } else {
            Ok(())
        }
    }

    fn log_rpc(&self, correlation_id: &str, method: &str, outcome: &str, started: Instant) {
        self.state.logger.emit(
            LogLevel::Trace,
            "oll::admin",
            "admin_request",
            correlation_id,
            serde_json::json!({
                "method": method,
                "outcome": outcome,
                "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            }),
        );
    }

    #[allow(clippy::result_large_err)]
    fn native_path(path: Option<NativePath>, field: &'static str) -> Result<PathBuf, Status> {
        let bytes = path
            .ok_or_else(|| Status::invalid_argument(format!("missing {field}")))?
            .unix_path;
        if bytes.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{field} must not be empty"
            )));
        }
        if bytes.contains(&0) {
            return Err(Status::invalid_argument(format!(
                "{field} must not contain NUL"
            )));
        }
        let path = PathBuf::from(OsString::from_vec(bytes));
        if !path.is_absolute() {
            return Err(Status::invalid_argument(format!(
                "{field} must be absolute"
            )));
        }
        Ok(path)
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let started = Instant::now();
        let correlation_id = self.validate_context(request.into_inner().context)?;
        self.require_serving()?;
        let peers = self.state.sync.status().await;
        let (replica_state, replica_id) = match self.state.replica.status().await {
            ReplicaStatus::Uninitialized => (ProtoReplicaState::Uninitialized, None),
            ReplicaStatus::InitializedEmpty { replica_id } => (
                ProtoReplicaState::InitializedEmpty,
                Some(ReplicaId {
                    value: replica_id.to_string(),
                }),
            ),
            ReplicaStatus::InitializedPopulated { replica_id } => (
                ProtoReplicaState::InitializedPopulated,
                Some(ReplicaId {
                    value: replica_id.to_string(),
                }),
            ),
        };
        let identity = self.state.identities.node().await;
        self.log_rpc(&correlation_id, "GetStatus", "ok", started);
        Ok(Response::new(GetStatusResponse {
            node: Some(identity.to_proto()),
            lifecycle: self.state.lifecycle() as i32,
            started_at: Some(timestamp(self.state.started_at)),
            process_id: std::process::id(),
            peers,
            configured_listen_address: self.state.config.listen.map(|address| address.to_string()),
            replica_state: replica_state as i32,
            replica_id,
        }))
    }

    async fn shutdown(
        &self,
        request: Request<AdminShutdownRequest>,
    ) -> Result<Response<AdminShutdownResponse>, Status> {
        let started = Instant::now();
        let correlation_id = self.validate_context(request.into_inner().context)?;
        let first_shutdown = self.state.begin_shutdown();
        self.log_rpc(&correlation_id, "Shutdown", "accepted", started);
        if first_shutdown {
            self.state.logger.emit(
                LogLevel::Info,
                "oll::node",
                "node_shutdown_requested",
                &correlation_id,
                serde_json::json!({}),
            );
            let shutdown = self.state.shutdown.clone();
            let notice = ShutdownNotice::requested(correlation_id);
            tokio::spawn(async move {
                // Let tonic serialize the accepted response before the server starts closing.
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = shutdown.send(notice);
            });
        }
        Ok(Response::new(AdminShutdownResponse { accepted: true }))
    }

    async fn set_log_filter(
        &self,
        request: Request<SetLogFilterRequest>,
    ) -> Result<Response<SetLogFilterResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let target = request
            .target
            .parse::<LogTarget>()
            .map_err(Status::invalid_argument)?;
        let level = ProtoLogLevel::try_from(request.level)
            .ok()
            .and_then(LogLevel::from_proto)
            .ok_or_else(|| Status::invalid_argument("log level must be specified"))?;
        self.state
            .logger
            .set_filter(target.as_str().to_owned(), level)
            .map_err(|_| Status::internal("cannot update live log filter"))?;
        self.log_rpc(&correlation_id, "SetLogFilter", "ok", started);
        Ok(Response::new(SetLogFilterResponse {
            target: target.as_str().to_owned(),
            level: level.to_proto() as i32,
        }))
    }

    async fn inspect_replica_document(
        &self,
        request: Request<InspectReplicaDocumentRequest>,
    ) -> Result<Response<InspectReplicaDocumentResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let path = Self::native_path(request.document_path, "document_path")?;
        let inspection = match self.state.replica.inspect_document(&path).await {
            Ok(inspection) => inspection,
            Err(error) => {
                self.log_rpc(&correlation_id, "InspectReplicaDocument", "error", started);
                return Err(replica_status(error));
            }
        };
        self.log_rpc(&correlation_id, "InspectReplicaDocument", "ok", started);
        Ok(Response::new(InspectReplicaDocumentResponse {
            catalog_node_id: Some(CatalogNodeId {
                value: inspection.catalog_node_id.to_string(),
            }),
            catalog_revision: Some(CatalogRevision {
                token: inspection.catalog_revision.to_vec(),
            }),
            document_id: Some(DocumentId {
                value: inspection.document_id.to_string(),
            }),
            document_revision: Some(DocumentRevision {
                token: inspection.document_revision.to_vec(),
            }),
            path: Some(DocumentPath {
                value: inspection.path,
            }),
            media_type: inspection.media_type,
            encoding: inspection.encoding,
            has_byte_order_mark: inspection.has_byte_order_mark,
            size_bytes: inspection.size_bytes,
        }))
    }

    async fn list_replica_operations(
        &self,
        request: Request<ListReplicaOperationsRequest>,
    ) -> Result<Response<ListReplicaOperationsResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let path = Self::native_path(request.document_path, "document_path")?;
        let limit = if request.limit == 0 {
            usize::try_from(i64::MAX).unwrap_or(usize::MAX)
        } else {
            usize::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("operation limit is too large"))?
        };
        let operations = match self.state.replica.list_operations(&path, limit).await {
            Ok(operations) => operations.into_iter().map(operation_to_proto).collect(),
            Err(error) => {
                self.log_rpc(&correlation_id, "ListReplicaOperations", "error", started);
                return Err(replica_status(error));
            }
        };
        self.log_rpc(&correlation_id, "ListReplicaOperations", "ok", started);
        Ok(Response::new(ListReplicaOperationsResponse { operations }))
    }

    async fn export_replica(
        &self,
        request: Request<ExportReplicaRequest>,
    ) -> Result<Response<ExportReplicaResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let path = Self::native_path(request.snapshot_path, "snapshot_path")?;
        let (snapshot_id, replica_id) = match self
            .state
            .replica
            .export_snapshot(&path, &correlation_id)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                self.log_rpc(&correlation_id, "ExportReplica", "error", started);
                return Err(replica_status(error));
            }
        };
        self.log_rpc(&correlation_id, "ExportReplica", "ok", started);
        Ok(Response::new(ExportReplicaResponse {
            snapshot_id: snapshot_id.to_string(),
            replica_id: Some(ReplicaId {
                value: replica_id.to_string(),
            }),
        }))
    }

    async fn import_replica(
        &self,
        request: Request<ImportReplicaRequest>,
    ) -> Result<Response<ImportReplicaResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let path = Self::native_path(request.snapshot_path, "snapshot_path")?;
        let (snapshot_id, replica_id) = match self
            .state
            .replica
            .import_snapshot(&path, &correlation_id)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                self.log_rpc(&correlation_id, "ImportReplica", "error", started);
                return Err(replica_status(error));
            }
        };
        self.log_rpc(&correlation_id, "ImportReplica", "ok", started);
        Ok(Response::new(ImportReplicaResponse {
            snapshot_id: snapshot_id.to_string(),
            replica_id: Some(ReplicaId {
                value: replica_id.to_string(),
            }),
        }))
    }

    async fn synchronize_peers(
        &self,
        request: Request<SynchronizePeersRequest>,
    ) -> Result<Response<SynchronizePeersResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        if request.total_attempts == 0 {
            return Err(Status::invalid_argument(
                "total_attempts must be greater than zero",
            ));
        }
        let node_name = request
            .node
            .map(|node| node.value.parse().map_err(Status::invalid_argument))
            .transpose()?;
        let peers = match self
            .state
            .sync
            .synchronize(node_name.as_ref(), request.total_attempts, &correlation_id)
            .await
        {
            Ok(peers) => peers,
            Err(error) => {
                self.log_rpc(&correlation_id, "SynchronizePeers", "error", started);
                return Err(sync_status(error));
            }
        };
        self.log_rpc(&correlation_id, "SynchronizePeers", "ok", started);
        Ok(Response::new(SynchronizePeersResponse { peers }))
    }

    async fn ping_peer(
        &self,
        request: Request<PingPeerRequest>,
    ) -> Result<Response<PingPeerResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let correlation_id = self.validate_context(request.context)?;
        self.require_serving()?;
        let node_name = request
            .node
            .ok_or_else(|| Status::invalid_argument("missing ping node name"))?
            .value
            .parse()
            .map_err(Status::invalid_argument)?;
        let (identity, round_trip) = match self.state.sync.ping(&node_name, &correlation_id).await {
            Ok(result) => result,
            Err(error) => {
                self.log_rpc(&correlation_id, "PingPeer", "error", started);
                return Err(sync_status(error));
            }
        };
        self.log_rpc(&correlation_id, "PingPeer", "ok", started);
        Ok(Response::new(PingPeerResponse {
            node: Some(identity.to_proto()),
            round_trip_millis: u64::try_from(round_trip.as_millis()).unwrap_or(u64::MAX),
        }))
    }
}

fn operation_to_proto(operation: OperationRecord) -> ReplicaOperation {
    let source = match operation.source {
        OperationSource::Filesystem => ReplicaOperationSource::Filesystem,
        OperationSource::Plugin => ReplicaOperationSource::Plugin,
        OperationSource::Sync => ReplicaOperationSource::Sync,
        OperationSource::SnapshotImport => ReplicaOperationSource::SnapshotImport,
    };
    let kind = match operation.kind {
        OperationKind::Create => ReplicaOperationKind::Create,
        OperationKind::Update => ReplicaOperationKind::Update,
        OperationKind::Move => ReplicaOperationKind::Move,
        OperationKind::Delete => ReplicaOperationKind::Delete,
        OperationKind::Replace => ReplicaOperationKind::Replace,
    };
    ReplicaOperation {
        timestamp: Some(prost_types::Timestamp {
            seconds: operation.timestamp.unix_timestamp(),
            nanos: operation.timestamp.nanosecond() as i32,
        }),
        operation_id: operation.operation_id,
        source: source as i32,
        kind: kind as i32,
        catalog_node_id: Some(CatalogNodeId {
            value: operation.catalog_node_id.to_string(),
        }),
        document_id: Some(DocumentId {
            value: operation.document_id.to_string(),
        }),
        path_before: operation.path_before.map(|value| DocumentPath { value }),
        path_after: operation.path_after.map(|value| DocumentPath { value }),
        correlation_id: operation.correlation_id,
    }
}

#[allow(clippy::result_large_err)]
fn replica_status(error: ReplicaError) -> Status {
    match error {
        ReplicaError::Uninitialized => Status::failed_precondition("no local replica yet"),
        ReplicaError::Configuration(_) => Status::internal("replica identity is inconsistent"),
        ReplicaError::InvalidArgument(message) | ReplicaError::InvalidSnapshot(message) => {
            Status::invalid_argument(message)
        }
        ReplicaError::NotFound(message) => Status::not_found(message),
        ReplicaError::AlreadyExists(message) => Status::already_exists(message),
        ReplicaError::RevisionConflict(message) => Status::aborted(message),
        ReplicaError::CorruptStore(_)
        | ReplicaError::Io { .. }
        | ReplicaError::Store(_)
        | ReplicaError::Internal(_) => {
            Status::internal("replica operation failed; inspect the correlated daemon log")
        }
    }
}

#[allow(clippy::result_large_err)]
fn sync_status(error: SyncError) -> Status {
    match error {
        SyncError::NotFound(message) => Status::not_found(message),
        SyncError::FailedPrecondition(message) => Status::failed_precondition(message),
        SyncError::Unavailable(message) => Status::unavailable(message),
        SyncError::Protocol(message) => Status::aborted(message),
        SyncError::Store | SyncError::Internal(_) => {
            Status::internal("sync operation failed; inspect the correlated daemon log")
        }
    }
}

fn timestamp(time: SystemTime) -> prost_types::Timestamp {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        },
    }
}

pub async fn serve(
    listener: UnixListener,
    state: Arc<AdminState>,
    mut shutdown: watch::Receiver<ShutdownNotice>,
) -> Result<(), NodeError> {
    let incoming = UnixListenerStream::new(listener);
    let service = AdminService { state };
    let shutdown_future = async move {
        if !shutdown.borrow().is_requested() {
            let _ = shutdown.changed().await;
        }
    };

    #[cfg(debug_assertions)]
    {
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(crate::protocol::oll::FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|error| {
                NodeError::Internal(format!("cannot build Admin reflection service: {error}"))
            })?;
        Server::builder()
            .add_service(AdminServer::new(service))
            .add_service(reflection)
            .serve_with_incoming_shutdown(incoming, shutdown_future)
            .await
            .map_err(|error| NodeError::Internal(format!("cannot serve Admin UDS: {error}")))
    }

    #[cfg(not(debug_assertions))]
    {
        Server::builder()
            .add_service(AdminServer::new(service))
            .serve_with_incoming_shutdown(incoming, shutdown_future)
            .await
            .map_err(|error| NodeError::Internal(format!("cannot serve Admin UDS: {error}")))
    }
}
