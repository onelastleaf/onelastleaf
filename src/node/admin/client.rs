use std::{
    convert::TryFrom,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};
use tower::service_fn;

use crate::{
    cli::{LogFilterLevel, LogTarget},
    protocol::{
        PROTOCOL_SCHEMA_SHA256,
        oll::{
            AdminCallContext, AdminShutdownRequest, ExportReplicaRequest, ExportReplicaResponse,
            GetStatusRequest, GetStatusResponse, ImportReplicaRequest, ImportReplicaResponse,
            InspectReplicaDocumentRequest, InspectReplicaDocumentResponse,
            ListReplicaOperationsRequest, ListReplicaOperationsResponse, NativePath,
            PingPeerRequest, PingPeerResponse, SetLogFilterRequest, SetLogFilterResponse,
            SynchronizePeersRequest, SynchronizePeersResponse, TraceContext,
            admin_client::AdminClient,
        },
    },
};

use super::{ADMIN_CONNECT_DEADLINE, ADMIN_SHORT_CALL_DEADLINE};
use crate::node::{logging::LogLevel, runtime::NodeError};

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

pub async fn ping_peer(
    socket: &Path,
    node_name: &crate::cli::NodeName,
    correlation_id: String,
) -> Result<PingPeerResponse, NodeError> {
    let mut client = connect(socket).await?;
    let mut request = Request::new(PingPeerRequest {
        context: Some(call_context(correlation_id)),
        node: Some(crate::protocol::oll::NodeName {
            value: node_name.as_str().to_owned(),
        }),
    });
    request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
    client
        .ping_peer(request)
        .await
        .map(Response::into_inner)
        .map_err(status_error)
}

pub async fn synchronize_peers(
    socket: &Path,
    node_name: Option<&crate::cli::NodeName>,
    total_attempts: u32,
    correlation_id: String,
) -> Result<SynchronizePeersResponse, NodeError> {
    let mut client = connect(socket).await?;
    client
        .synchronize_peers(SynchronizePeersRequest {
            context: Some(call_context(correlation_id)),
            node: node_name.map(|node_name| crate::protocol::oll::NodeName {
                value: node_name.as_str().to_owned(),
            }),
            total_attempts,
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
