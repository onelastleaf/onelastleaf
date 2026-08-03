use std::{
    convert::TryFrom,
    ffi::OsString,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use hyper_util::rt::TokioIo;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::watch,
    time::Instant,
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint, Server},
};
use tower::service_fn;

use crate::{
    cli::{LogFilterLevel, LogTarget},
    configuration::ResolvedNodeConfig,
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{
            AdminCallContext, AdminShutdownRequest, AdminShutdownResponse, CatalogNodeId,
            CatalogRevision, DocumentId, DocumentPath, DocumentRevision, ExportReplicaRequest,
            ExportReplicaResponse, GetStatusRequest, GetStatusResponse, ImportReplicaRequest,
            ImportReplicaResponse, InspectReplicaDocumentRequest, InspectReplicaDocumentResponse,
            ListReplicaOperationsRequest, ListReplicaOperationsResponse, LogLevel as ProtoLogLevel,
            NativePath, NodeLifecycleState, PeerConnectionDirection, PeerConnectionState,
            PeerStatus, PingPeerRequest, PingPeerResponse, ReplicaId, ReplicaOperation,
            ReplicaOperationKind, ReplicaOperationSource, ReplicaState as ProtoReplicaState,
            SetLogFilterRequest, SetLogFilterResponse, SynchronizePeersRequest,
            SynchronizePeersResponse, TraceContext,
            admin_client::AdminClient,
            admin_server::{Admin, AdminServer},
        },
    },
    replica::{
        OperationKind, OperationRecord, OperationSource, ReplicaError, ReplicaRuntime,
        ReplicaStatus,
    },
};

use super::{
    identity::NodeIdentity,
    logging::{LogLevel, NodeLogger},
    runtime::NodeError,
};

const LIFECYCLE_STARTING: u8 = 1;
const LIFECYCLE_RUNNING: u8 = 2;
const LIFECYCLE_STOPPING: u8 = 3;
const ADMIN_CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const ADMIN_SHORT_CALL_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default)]
pub struct ShutdownNotice {
    correlation_id: Option<String>,
    requested_at: Option<Instant>,
}

impl ShutdownNotice {
    pub fn requested(correlation_id: String) -> Self {
        Self {
            correlation_id: Some(correlation_id),
            requested_at: Some(Instant::now()),
        }
    }

    pub fn is_requested(&self) -> bool {
        self.correlation_id.is_some()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    pub fn requested_at(&self) -> Option<Instant> {
        self.requested_at
    }
}

pub struct AdminState {
    identity: NodeIdentity,
    config: ResolvedNodeConfig,
    started_at: SystemTime,
    lifecycle: AtomicU8,
    logger: Arc<NodeLogger>,
    replica: Arc<ReplicaRuntime>,
    shutdown: watch::Sender<ShutdownNotice>,
}

impl AdminState {
    pub fn new(
        identity: NodeIdentity,
        config: ResolvedNodeConfig,
        logger: Arc<NodeLogger>,
        replica: Arc<ReplicaRuntime>,
        shutdown: watch::Sender<ShutdownNotice>,
    ) -> Self {
        Self {
            identity,
            config,
            started_at: SystemTime::now(),
            lifecycle: AtomicU8::new(LIFECYCLE_STARTING),
            logger,
            replica,
            shutdown,
        }
    }

    pub fn mark_running(&self) {
        self.lifecycle.store(LIFECYCLE_RUNNING, Ordering::Release);
    }

    pub fn lifecycle(&self) -> NodeLifecycleState {
        match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_STARTING => NodeLifecycleState::Starting,
            LIFECYCLE_RUNNING => NodeLifecycleState::Running,
            LIFECYCLE_STOPPING => NodeLifecycleState::Stopping,
            _ => NodeLifecycleState::Unspecified,
        }
    }

    pub fn begin_shutdown(&self) -> bool {
        self.lifecycle
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_STOPPING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn is_stopping(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_STOPPING
    }
}

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
        let peers = self
            .state
            .config
            .connect
            .iter()
            .map(|url| PeerStatus {
                connect_target: Some(url.to_string()),
                node: None,
                connection_state: PeerConnectionState::Pending as i32,
                direction: PeerConnectionDirection::Outbound as i32,
            })
            .collect();
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
        self.log_rpc(&correlation_id, "GetStatus", "ok", started);
        Ok(Response::new(GetStatusResponse {
            node: Some(self.state.identity.to_proto()),
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
        let correlation_id = self.validate_context(request.into_inner().context)?;
        self.require_serving()?;
        self.log_rpc(
            &correlation_id,
            "SynchronizePeers",
            "unimplemented",
            started,
        );
        Err(Status::unimplemented("command is not implemented"))
    }

    async fn ping_peer(
        &self,
        request: Request<PingPeerRequest>,
    ) -> Result<Response<PingPeerResponse>, Status> {
        let started = Instant::now();
        let correlation_id = self.validate_context(request.into_inner().context)?;
        self.require_serving()?;
        self.log_rpc(&correlation_id, "PingPeer", "unimplemented", started);
        Err(Status::unimplemented("command is not implemented"))
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

pub async fn connect(socket: &Path) -> Result<AdminClient<Channel>, NodeError> {
    let socket = Arc::new(PathBuf::from(socket));
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|error| NodeError::Internal(format!("cannot create Admin endpoint: {error}")))?
        .connect_timeout(ADMIN_CONNECT_DEADLINE)
        .connect_with_connector(service_fn(move |_| {
            let socket = Arc::clone(&socket);
            async move {
                UnixStream::connect(socket.as_path())
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .map_err(|_| NodeError::Unavailable("cannot connect to the local oll daemon".to_owned()))?;
    Ok(AdminClient::new(channel))
}

pub fn call_context(correlation_id: String) -> AdminCallContext {
    AdminCallContext {
        protocol_schema_sha256: PROTOCOL_SCHEMA_SHA256.to_vec(),
        trace: Some(TraceContext {
            correlation_id,
            parent_call_id: None,
            call_depth: 0,
            causal_depth: 0,
            task_id: None,
            task_group_id: None,
        }),
    }
}

pub async fn get_status(
    socket: &Path,
    correlation_id: String,
) -> Result<GetStatusResponse, NodeError> {
    let mut client = connect(socket).await?;
    let mut request = Request::new(GetStatusRequest {
        context: Some(call_context(correlation_id)),
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    client
        .get_status(request)
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn request_shutdown(socket: &Path, correlation_id: String) -> Result<(), NodeError> {
    let mut client = connect(socket).await?;
    let mut request = Request::new(AdminShutdownRequest {
        context: Some(call_context(correlation_id)),
        reason: "requested by oll stop".to_owned(),
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    let response = client
        .shutdown(request)
        .await
        .map_err(status_error)?
        .into_inner();
    if response.accepted {
        Ok(())
    } else {
        Err(NodeError::Unavailable(
            "daemon did not accept the shutdown request".to_owned(),
        ))
    }
}

pub async fn set_log_filter(
    socket: &Path,
    target: &LogTarget,
    level: LogFilterLevel,
    correlation_id: String,
) -> Result<SetLogFilterResponse, NodeError> {
    let mut client = connect(socket).await?;
    let mut request = Request::new(SetLogFilterRequest {
        context: Some(call_context(correlation_id)),
        target: target.as_str().to_owned(),
        level: LogLevel::from_cli(level).to_proto() as i32,
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    client
        .set_log_filter(request)
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn inspect_replica_document(
    socket: &Path,
    document_path: &Path,
    correlation_id: String,
) -> Result<InspectReplicaDocumentResponse, NodeError> {
    let mut client = connect(socket).await?;
    let mut request = Request::new(InspectReplicaDocumentRequest {
        context: Some(call_context(correlation_id)),
        document_path: Some(native_path(document_path)),
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    client
        .inspect_replica_document(request)
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn list_replica_operations(
    socket: &Path,
    document_path: &Path,
    limit: Option<usize>,
    correlation_id: String,
) -> Result<ListReplicaOperationsResponse, NodeError> {
    let limit = limit
        .map(u64::try_from)
        .transpose()
        .map_err(|_| NodeError::Config("operation limit is too large".to_owned()))?
        .unwrap_or(0);
    let mut client = connect(socket).await?;
    let mut request = Request::new(ListReplicaOperationsRequest {
        context: Some(call_context(correlation_id)),
        document_path: Some(native_path(document_path)),
        limit,
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    client
        .list_replica_operations(request)
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn export_replica(
    socket: &Path,
    snapshot_path: &Path,
    correlation_id: String,
) -> Result<ExportReplicaResponse, NodeError> {
    let mut client = connect(socket).await?;
    client
        .export_replica(ExportReplicaRequest {
            context: Some(call_context(correlation_id)),
            snapshot_path: Some(native_path(snapshot_path)),
        })
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn import_replica(
    socket: &Path,
    snapshot_path: &Path,
    correlation_id: String,
) -> Result<ImportReplicaResponse, NodeError> {
    let mut client = connect(socket).await?;
    client
        .import_replica(ImportReplicaRequest {
            context: Some(call_context(correlation_id)),
            snapshot_path: Some(native_path(snapshot_path)),
        })
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

fn native_path(path: &Path) -> NativePath {
    NativePath {
        unix_path: path.as_os_str().as_bytes().to_vec(),
    }
}

fn status_error(status: Status) -> NodeError {
    match status.code() {
        tonic::Code::FailedPrecondition
        | tonic::Code::Unavailable
        | tonic::Code::Cancelled
        | tonic::Code::DeadlineExceeded => NodeError::Unavailable(status.message().to_owned()),
        tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::AlreadyExists
        | tonic::Code::Aborted => NodeError::Operation(status.message().to_owned()),
        _ => NodeError::Internal(status.message().to_owned()),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use tokio::{net::UnixListener, sync::watch, time::timeout};
    use uuid::Uuid;

    use crate::{
        configuration::{ReplicaStoreConfig, ResolvedNodeConfig},
        protocol::oll::GetStatusRequest,
    };

    use super::*;
    use crate::node::{identity::NodeIdentity, logging::NodeLogger};

    struct SlowAdmin;

    #[tonic::async_trait]
    impl Admin for SlowAdmin {
        async fn get_status(
            &self,
            _request: Request<GetStatusRequest>,
        ) -> Result<Response<GetStatusResponse>, Status> {
            tokio::time::sleep(Duration::from_secs(11)).await;
            Ok(Response::new(GetStatusResponse::default()))
        }

        async fn shutdown(
            &self,
            _request: Request<AdminShutdownRequest>,
        ) -> Result<Response<AdminShutdownResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }

        async fn set_log_filter(
            &self,
            _request: Request<SetLogFilterRequest>,
        ) -> Result<Response<SetLogFilterResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }

        async fn inspect_replica_document(
            &self,
            _request: Request<InspectReplicaDocumentRequest>,
        ) -> Result<Response<InspectReplicaDocumentResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }

        async fn list_replica_operations(
            &self,
            _request: Request<ListReplicaOperationsRequest>,
        ) -> Result<Response<ListReplicaOperationsResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }

        async fn export_replica(
            &self,
            _request: Request<ExportReplicaRequest>,
        ) -> Result<Response<ExportReplicaResponse>, Status> {
            tokio::time::sleep(Duration::from_secs(11)).await;
            Ok(Response::new(ExportReplicaResponse {
                snapshot_id: Uuid::new_v4().to_string(),
                replica_id: None,
            }))
        }

        async fn import_replica(
            &self,
            _request: Request<ImportReplicaRequest>,
        ) -> Result<Response<ImportReplicaResponse>, Status> {
            tokio::time::sleep(Duration::from_secs(11)).await;
            Ok(Response::new(ImportReplicaResponse {
                snapshot_id: Uuid::new_v4().to_string(),
                replica_id: None,
            }))
        }

        async fn synchronize_peers(
            &self,
            _request: Request<SynchronizePeersRequest>,
        ) -> Result<Response<SynchronizePeersResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }

        async fn ping_peer(
            &self,
            _request: Request<PingPeerRequest>,
        ) -> Result<Response<PingPeerResponse>, Status> {
            Err(Status::unimplemented("not used by deadline test"))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn short_admin_calls_have_deadlines_but_snapshot_calls_do_not() {
        let directory = TempDir::new().unwrap();
        let socket = directory.path().join("slow-admin.sock");
        let listener = match UnixListener::bind(&socket) {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("cannot bind test Admin socket: {error}"),
        };
        let incoming = UnixListenerStream::new(listener);
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(AdminServer::new(SlowAdmin))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;

        let error = get_status(&socket, "slow-status-correlation".to_owned())
            .await
            .unwrap_err();
        assert!(matches!(error, NodeError::Unavailable(_)), "{error:?}");

        let snapshot = directory.path().join("slow.ollsnap");
        export_replica(&socket, &snapshot, "slow-export-correlation".to_owned())
            .await
            .unwrap();
        import_replica(&socket, &snapshot, "slow-import-correlation".to_owned())
            .await
            .unwrap();

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn uds_admin_validates_fingerprint_reports_identity_and_shuts_down() {
        let directory = TempDir::new().unwrap();
        let identity = NodeIdentity::generate("home-node".parse().unwrap());
        let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
        logger
            .set_filter("oll::admin".to_owned(), LogLevel::Trace)
            .unwrap();
        let config = ResolvedNodeConfig {
            replica_root: directory.path().join("replica"),
            replica_store: ReplicaStoreConfig::Sqlite {
                path: directory.path().join("replica.sqlite3"),
            },
            log_dir: directory.path().join("log"),
            listen: Some("127.0.0.1:7443".parse().unwrap()),
            connect: vec!["https://peer.example".parse().unwrap()],
        };
        std::fs::create_dir(&config.replica_root).unwrap();
        let document_path = config.replica_root.join("admin.md");
        std::fs::write(&document_path, "admin protocol").unwrap();
        let replica = ReplicaRuntime::start(
            config.replica_root.clone(),
            &config.replica_store,
            identity.node_id(),
            Arc::clone(&logger),
        )
        .await
        .unwrap();
        let socket = directory.path().join("admin.sock");
        let listener = match UnixListener::bind(&socket) {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("cannot bind test Admin socket: {error}"),
        };
        let (shutdown, receiver) = watch::channel(ShutdownNotice::default());
        let state = Arc::new(AdminState::new(
            identity.clone(),
            config,
            Arc::clone(&logger),
            Arc::clone(&replica),
            shutdown,
        ));
        state.mark_running();
        let task = tokio::spawn(serve(listener, state, receiver));

        let status = get_status(&socket, "status-correlation".to_owned())
            .await
            .unwrap();
        assert_eq!(
            status
                .node
                .as_ref()
                .unwrap()
                .node_name
                .as_ref()
                .unwrap()
                .value,
            "home-node"
        );
        assert_eq!(status.peers.len(), 1);
        assert_eq!(
            status.peers[0].connect_target.as_deref(),
            Some("https://peer.example/")
        );
        assert_eq!(
            status.peers[0].direction,
            PeerConnectionDirection::Outbound as i32
        );
        assert_eq!(status.lifecycle, NodeLifecycleState::Running as i32);
        assert_eq!(
            status.replica_state,
            ProtoReplicaState::InitializedPopulated as i32
        );
        assert!(status.replica_id.is_some());

        let inspection =
            inspect_replica_document(&socket, &document_path, "inspect-correlation".to_owned())
                .await
                .unwrap();
        assert_eq!(inspection.path.unwrap().value, "/admin.md");
        assert_eq!(inspection.encoding, "UTF-8");
        assert!(inspection.document_id.is_some());
        assert!(inspection.catalog_revision.is_some());
        assert!(inspection.document_revision.is_some());

        let operations = list_replica_operations(
            &socket,
            &document_path,
            Some(10),
            "operations-correlation".to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(operations.operations.len(), 1);
        assert_eq!(
            operations.operations[0].source,
            ReplicaOperationSource::Filesystem as i32
        );

        let snapshot_path = directory.path().join("admin.ollsnap");
        let exported = export_replica(&socket, &snapshot_path, "export-correlation".to_owned())
            .await
            .unwrap();
        let imported = import_replica(&socket, &snapshot_path, "import-correlation".to_owned())
            .await
            .unwrap();
        assert_eq!(imported.snapshot_id, exported.snapshot_id);
        assert_eq!(imported.replica_id, exported.replica_id);
        let imported_operations = list_replica_operations(
            &socket,
            &document_path,
            Some(10),
            "import-operations-correlation".to_owned(),
        )
        .await
        .unwrap();
        assert!(imported_operations.operations.iter().any(|operation| {
            operation.source == ReplicaOperationSource::SnapshotImport as i32
                && operation.correlation_id == "import-correlation"
        }));

        logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        let events = std::fs::read_to_string(directory.path().join("log/oll.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        for (method, event, correlation_id) in [
            (
                "ExportReplica",
                "snapshot_export_started",
                "export-correlation",
            ),
            (
                "ExportReplica",
                "snapshot_export_completed",
                "export-correlation",
            ),
            (
                "ImportReplica",
                "snapshot_import_started",
                "import-correlation",
            ),
            (
                "ImportReplica",
                "snapshot_import_completed",
                "import-correlation",
            ),
        ] {
            assert!(events.iter().any(|record| {
                record["event"] == event && record["correlation_id"] == correlation_id
            }));
            assert!(events.iter().any(|record| {
                record["event"] == "admin_request"
                    && record["method"] == method
                    && record["correlation_id"] == correlation_id
            }));
        }

        let filter = set_log_filter(
            &socket,
            &"oll::sync".parse().unwrap(),
            LogFilterLevel::Trace,
            "filter-correlation".to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(filter.level, ProtoLogLevel::Trace as i32);

        let mut client = connect(&socket).await.unwrap();
        let error = client
            .inspect_replica_document(InspectReplicaDocumentRequest {
                context: Some(call_context("invalid-path-correlation".to_owned())),
                document_path: Some(NativePath {
                    unix_path: b"relative.md".to_vec(),
                }),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        let error = client
            .get_status(GetStatusRequest {
                context: Some(AdminCallContext {
                    protocol_schema_sha256: vec![0; 32],
                    trace: Some(TraceContext {
                        correlation_id: "bad-fingerprint".to_owned(),
                        parent_call_id: None,
                        call_depth: 0,
                        causal_depth: 0,
                        task_id: None,
                        task_group_id: None,
                    }),
                }),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);

        request_shutdown(&socket, "shutdown-correlation".to_owned())
            .await
            .unwrap();
        request_shutdown(&socket, "second-shutdown-correlation".to_owned())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        replica
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
}
