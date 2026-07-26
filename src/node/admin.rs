use std::{
    convert::TryFrom,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hyper_util::rt::TokioIo;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::watch,
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
            AdminCallContext, AdminShutdownRequest, AdminShutdownResponse, GetStatusRequest,
            GetStatusResponse, LogLevel as ProtoLogLevel, NodeLifecycleState, PeerConnectionState,
            PeerStatus, SetLogFilterRequest, SetLogFilterResponse, TraceContext,
            admin_client::AdminClient,
            admin_server::{Admin, AdminServer},
        },
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
const ADMIN_CALL_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default)]
pub struct ShutdownNotice {
    correlation_id: Option<String>,
}

impl ShutdownNotice {
    pub fn requested(correlation_id: String) -> Self {
        Self {
            correlation_id: Some(correlation_id),
        }
    }

    pub fn is_requested(&self) -> bool {
        self.correlation_id.is_some()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
}

pub struct AdminState {
    identity: NodeIdentity,
    config: ResolvedNodeConfig,
    started_at: SystemTime,
    lifecycle: AtomicU8,
    logger: Arc<NodeLogger>,
    shutdown: watch::Sender<ShutdownNotice>,
}

impl AdminState {
    pub fn new(
        identity: NodeIdentity,
        config: ResolvedNodeConfig,
        logger: Arc<NodeLogger>,
        shutdown: watch::Sender<ShutdownNotice>,
    ) -> Self {
        Self {
            identity,
            config,
            started_at: SystemTime::now(),
            lifecycle: AtomicU8::new(LIFECYCLE_STARTING),
            logger,
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
        let _ = self.state.logger.emit(
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
        self.log_rpc(&correlation_id, "GetStatus", "ok", started);

        let peers = self
            .state
            .config
            .connect
            .iter()
            .map(|url| PeerStatus {
                connect_url: url.to_string(),
                node: None,
                connection_state: PeerConnectionState::Pending as i32,
            })
            .collect();
        Ok(Response::new(GetStatusResponse {
            node: Some(self.state.identity.to_proto()),
            lifecycle: self.state.lifecycle() as i32,
            started_at: Some(timestamp(self.state.started_at)),
            process_id: std::process::id(),
            peers,
            configured_listen_address: self.state.config.listen.map(|address| address.to_string()),
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
            let _ = self.state.logger.emit(
                LogLevel::Info,
                "oll::node",
                "node_shutdown_requested",
                &correlation_id,
                serde_json::json!({}),
            );
            let shutdown = self.state.shutdown.clone();
            tokio::spawn(async move {
                // Let tonic serialize the accepted response before the server starts closing.
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = shutdown.send(ShutdownNotice::requested(correlation_id));
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
        .connect_timeout(ADMIN_CALL_DEADLINE)
        .timeout(ADMIN_CALL_DEADLINE)
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
    client
        .get_status(GetStatusRequest {
            context: Some(call_context(correlation_id)),
        })
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn request_shutdown(socket: &Path, correlation_id: String) -> Result<(), NodeError> {
    let mut client = connect(socket).await?;
    let response = client
        .shutdown(AdminShutdownRequest {
            context: Some(call_context(correlation_id)),
            reason: "requested by oll stop".to_owned(),
        })
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
    client
        .set_log_filter(SetLogFilterRequest {
            context: Some(call_context(correlation_id)),
            target: target.as_str().to_owned(),
            level: LogLevel::from_cli(level).to_proto() as i32,
        })
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

fn status_error(status: Status) -> NodeError {
    match status.code() {
        tonic::Code::FailedPrecondition | tonic::Code::Unavailable => {
            NodeError::Unavailable(status.message().to_owned())
        }
        tonic::Code::InvalidArgument => NodeError::Config(status.message().to_owned()),
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

    use crate::{configuration::ResolvedNodeConfig, protocol::oll::GetStatusRequest};

    use super::*;
    use crate::node::{identity::NodeIdentity, logging::NodeLogger};

    #[tokio::test]
    async fn uds_admin_validates_fingerprint_reports_identity_and_shuts_down() {
        let directory = TempDir::new().unwrap();
        let identity = NodeIdentity::generate("home-node".parse().unwrap());
        let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
        let config = ResolvedNodeConfig {
            replica_root: directory.path().join("replica"),
            log_dir: directory.path().join("log"),
            listen: Some("127.0.0.1:7443".parse().unwrap()),
            connect: vec!["https://peer.example".parse().unwrap()],
        };
        let socket = directory.path().join("admin.sock");
        let listener = match UnixListener::bind(&socket) {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("cannot bind test Admin socket: {error}"),
        };
        let (shutdown, receiver) = watch::channel(ShutdownNotice::default());
        let state = Arc::new(AdminState::new(identity.clone(), config, logger, shutdown));
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
        assert_eq!(status.lifecycle, NodeLifecycleState::Running as i32);

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
    }
}
