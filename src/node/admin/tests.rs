use std::{sync::Arc, time::Duration};

use tempfile::TempDir;
use tokio::{net::UnixListener, sync::watch, time::timeout};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

use crate::{
    cli::LogFilterLevel,
    configuration::{ConfigRuntime, ReplicaStoreConfig, ResolvedNodeConfig},
    plugin::PluginRuntime,
    protocol::oll::{
        AdminCallContext, AdminShutdownRequest, AdminShutdownResponse, ExportReplicaRequest,
        ExportReplicaResponse, GetStatusRequest, GetStatusResponse, ImportReplicaRequest,
        ImportReplicaResponse, InspectReplicaDocumentRequest, InspectReplicaDocumentResponse,
        ListReplicaOperationsRequest, ListReplicaOperationsResponse, LogLevel as ProtoLogLevel,
        NativePath, PeerConnectionDirection, PingPeerRequest, PingPeerResponse,
        ReplicaOperationSource, ReplicaState as ProtoReplicaState, SetLogFilterRequest,
        SetLogFilterResponse, SynchronizePeersRequest, SynchronizePeersResponse, TraceContext,
        admin_server::{Admin, AdminServer},
    },
};

use super::client::{call_context, connect};
use super::*;
use crate::node::{
    NodeError,
    identity::NodeIdentity,
    logging::{LogLevel, NodeLogger},
};

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

    async fn reconcile_plugin_installations(
        &self,
        request: Request<crate::protocol::oll::ReconcilePluginInstallationsRequest>,
    ) -> Result<Response<crate::protocol::oll::ReconcilePluginInstallationsResponse>, Status> {
        assert_eq!(
            request
                .into_inner()
                .context
                .unwrap()
                .trace
                .unwrap()
                .correlation_id,
            "slow-plugin-reconcile-correlation"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::ReconcilePluginInstallationsResponse::default(),
        ))
    }

    async fn remove_plugin(
        &self,
        request: Request<crate::protocol::oll::RemovePluginRequest>,
    ) -> Result<Response<crate::protocol::oll::RemovePluginResponse>, Status> {
        assert_eq!(
            request
                .into_inner()
                .context
                .unwrap()
                .trace
                .unwrap()
                .correlation_id,
            "slow-plugin-remove-correlation"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::RemovePluginResponse::default(),
        ))
    }

    async fn list_plugins(
        &self,
        request: Request<crate::protocol::oll::ListPluginsRequest>,
    ) -> Result<Response<crate::protocol::oll::ListPluginsResponse>, Status> {
        assert_eq!(
            request
                .into_inner()
                .context
                .unwrap()
                .trace
                .unwrap()
                .correlation_id,
            "slow-plugin-list-correlation"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::ListPluginsResponse::default(),
        ))
    }

    async fn get_plugin(
        &self,
        _request: Request<crate::protocol::oll::GetPluginRequest>,
    ) -> Result<Response<crate::protocol::oll::GetPluginResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::GetPluginResponse::default(),
        ))
    }

    async fn list_plugin_releases(
        &self,
        request: Request<crate::protocol::oll::ListPluginReleasesRequest>,
    ) -> Result<Response<crate::protocol::oll::ListPluginReleasesResponse>, Status> {
        assert_eq!(
            request
                .into_inner()
                .context
                .unwrap()
                .trace
                .unwrap()
                .correlation_id,
            "slow-plugin-releases-correlation"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::ListPluginReleasesResponse::default(),
        ))
    }

    async fn set_plugin_desired_state(
        &self,
        _request: Request<crate::protocol::oll::SetPluginDesiredStateRequest>,
    ) -> Result<Response<crate::protocol::oll::SetPluginDesiredStateResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::SetPluginDesiredStateResponse::default(),
        ))
    }

    async fn restart_plugin(
        &self,
        _request: Request<crate::protocol::oll::RestartPluginRequest>,
    ) -> Result<Response<crate::protocol::oll::RestartPluginResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::RestartPluginResponse::default(),
        ))
    }

    async fn start_plugin_job(
        &self,
        request: Request<crate::protocol::oll::StartPluginJobRequest>,
    ) -> Result<Response<crate::protocol::oll::StartPluginJobResponse>, Status> {
        assert_eq!(
            request
                .into_inner()
                .context
                .unwrap()
                .trace
                .unwrap()
                .correlation_id,
            "slow-plugin-job-start-correlation"
        );
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::StartPluginJobResponse::default(),
        ))
    }

    async fn list_plugin_jobs(
        &self,
        _request: Request<crate::protocol::oll::ListPluginJobsRequest>,
    ) -> Result<Response<crate::protocol::oll::ListPluginJobsResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::ListPluginJobsResponse::default(),
        ))
    }

    async fn get_plugin_job(
        &self,
        _request: Request<crate::protocol::oll::GetPluginJobRequest>,
    ) -> Result<Response<crate::protocol::oll::GetPluginJobResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::GetPluginJobResponse::default(),
        ))
    }

    async fn stop_plugin_job(
        &self,
        _request: Request<crate::protocol::oll::StopPluginJobRequest>,
    ) -> Result<Response<crate::protocol::oll::StopPluginJobResponse>, Status> {
        tokio::time::sleep(Duration::from_secs(11)).await;
        Ok(Response::new(
            crate::protocol::oll::StopPluginJobResponse::default(),
        ))
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

    let error = list_plugins(
        &socket,
        crate::protocol::oll::ListPluginsRequest::default(),
        "slow-plugin-list-correlation".to_owned(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, NodeError::Unavailable(_)), "{error:?}");

    macro_rules! assert_short_plugin_deadline {
        ($call:expr) => {{
            let error = $call.await.unwrap_err();
            assert!(matches!(error, NodeError::Unavailable(_)), "{error:?}");
        }};
    }
    assert_short_plugin_deadline!(get_plugin(
        &socket,
        crate::protocol::oll::GetPluginRequest::default(),
        "slow-plugin-get-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(set_plugin_desired_state(
        &socket,
        crate::protocol::oll::SetPluginDesiredStateRequest::default(),
        "slow-plugin-state-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(restart_plugin(
        &socket,
        crate::protocol::oll::RestartPluginRequest::default(),
        "slow-plugin-restart-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(start_plugin_job(
        &socket,
        crate::protocol::oll::StartPluginJobRequest::default(),
        "slow-plugin-job-start-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(list_plugin_jobs(
        &socket,
        crate::protocol::oll::ListPluginJobsRequest::default(),
        "slow-plugin-job-list-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(get_plugin_job(
        &socket,
        crate::protocol::oll::GetPluginJobRequest::default(),
        "slow-plugin-job-get-correlation".to_owned(),
    ));
    assert_short_plugin_deadline!(stop_plugin_job(
        &socket,
        crate::protocol::oll::StopPluginJobRequest::default(),
        "slow-plugin-job-stop-correlation".to_owned(),
    ));

    reconcile_plugin_installations(
        &socket,
        crate::protocol::oll::ReconcilePluginInstallationsRequest {
            context: None,
            operation: Some(
                crate::protocol::oll::reconcile_plugin_installations_request::Operation::InstallDeclared(
                    crate::protocol::oll::InstallDeclaredPlugins {},
                ),
            ),
        },
        "slow-plugin-reconcile-correlation".to_owned(),
    )
    .await
    .unwrap();
    remove_plugin(
        &socket,
        crate::protocol::oll::RemovePluginRequest::default(),
        "slow-plugin-remove-correlation".to_owned(),
    )
    .await
    .unwrap();
    list_plugin_releases(
        &socket,
        crate::protocol::oll::ListPluginReleasesRequest::default(),
        "slow-plugin-releases-correlation".to_owned(),
    )
    .await
    .unwrap();

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn uds_admin_validates_fingerprint_reports_identity_and_shuts_down() {
    let directory = TempDir::new().unwrap();
    let identity = NodeIdentity::generate("home-node".parse().unwrap());
    let identities = IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&directory.path().join("log"), identity.clone(), None).unwrap();
    logger
        .set_filter("oll::admin".to_owned(), LogLevel::Trace)
        .unwrap();
    let config = ResolvedNodeConfig {
        replica_root: directory.path().join("replica"),
        replica_store: ReplicaStoreConfig::Sqlite {
            path: directory.path().join("replica.sqlite3"),
        },
        log_dir: directory.path().join("log"),
        artifact_download_dir: directory.path().join("downloads/oll"),
        listen: None,
        connect: vec!["oll://127.0.0.1:9".parse().unwrap()],
        network_key: Some(crate::configuration::NetworkKey::new_for_test(
            b"test-network-key-with-thirty-two-bytes".to_vec(),
        )),
    };
    std::fs::write(
        directory.path().join("config.lua"),
        format!(
            "return {{\n  format_version = 1,\n  node = {{\n    replica_root = {:?},\n    replica_store = {{ driver = \"sqlite\", path = {:?} }},\n    log_dir = {:?},\n    artifact_download_dir = {:?},\n    listen = nil,\n    connect = {{}},\n  }},\n}}\n",
            config.replica_root.to_str().unwrap(),
            directory.path().join("replica.sqlite3").to_str().unwrap(),
            config.log_dir.to_str().unwrap(),
            config.artifact_download_dir.to_str().unwrap(),
        ),
    )
    .unwrap();
    std::fs::write(directory.path().join("plugins.lua"), "return {}\n").unwrap();
    let (config_runtime, _) = ConfigRuntime::load(directory.path()).unwrap();
    std::fs::create_dir(&config.replica_root).unwrap();
    let document_path = config.replica_root.join("admin.md");
    std::fs::write(&document_path, "admin protocol").unwrap();
    let replica = ReplicaRuntime::start(
        directory.path().to_owned(),
        config.replica_root.clone(),
        &config.replica_store,
        Arc::clone(&identities),
        Arc::clone(&logger),
    )
    .await
    .unwrap();
    let sync = SyncRuntime::start(
        &config,
        Arc::clone(&identities),
        Arc::clone(&replica),
        Arc::clone(&logger),
    )
    .await
    .unwrap();
    let parent_liveness = Arc::new(crate::node::liveness::ParentLivenessPipe::create().unwrap());
    let plugins = PluginRuntime::start(
        directory.path().to_owned(),
        directory.path().join("plugin-data"),
        config.artifact_download_dir.clone(),
        config_runtime,
        Arc::clone(&replica),
        Arc::clone(&identities),
        Arc::clone(&logger),
        Arc::clone(&parent_liveness),
        "admin-plugin-test-startup",
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
        identities,
        config,
        Arc::clone(&logger),
        Arc::clone(&replica),
        Arc::clone(&sync),
        Arc::clone(&plugins),
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
        Some("oll://127.0.0.1:9")
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

    let plugin_list = list_plugins(
        &socket,
        crate::protocol::oll::ListPluginsRequest::default(),
        "plugin-list-correlation".to_owned(),
    )
    .await
    .unwrap();
    assert!(plugin_list.plugins.is_empty());

    let empty_reconcile = reconcile_plugin_installations(
        &socket,
        crate::protocol::oll::ReconcilePluginInstallationsRequest {
            context: None,
            operation: Some(
                crate::protocol::oll::reconcile_plugin_installations_request::Operation::ExactReconciliation(
                    crate::protocol::oll::ExactPluginReconciliation {},
                ),
            ),
        },
        "empty-plugin-reconcile-correlation".to_owned(),
    )
    .await
    .unwrap();
    assert!(empty_reconcile.results.is_empty());

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
    assert!(events.iter().any(|record| {
        record["event"] == "plugin_package_operation_completed"
            && record["correlation_id"] == "empty-plugin-reconcile-correlation"
            && record["operation"] == "reconcile_exact"
            && record["outcome"] == "succeeded"
            && record["result_count"] == 0
            && record["failed_count"] == 0
    }));
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
        .start_plugin_job(crate::protocol::oll::StartPluginJobRequest {
            context: Some(call_context("invalid-plugin-job-correlation".to_owned())),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

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
    plugins
        .shutdown(Instant::now() + Duration::from_secs(1), "test-shutdown")
        .await
        .unwrap();
    sync.shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    replica
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}
