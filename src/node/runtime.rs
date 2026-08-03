use std::{
    fmt,
    future::Future,
    io::{self, BufRead, Read, Write},
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use getrandom::fill as fill_random;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener},
    process::Child,
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep, timeout, timeout_at},
};

use crate::{
    cli::{
        CliIntent, ClientDependency, ConfirmationRequirement, LogIntent, OutputFormat,
        PreparedCliIntent, PreparedClientIntent, PreparedRunIntent, ReplicaIntent,
    },
    configuration::{ConfigRuntime, validate_storage_layout},
    protocol::oll::{
        GetStatusResponse, InspectReplicaDocumentResponse, NodeLifecycleState,
        PeerConnectionDirection, PeerConnectionState, ReplicaOperationKind, ReplicaOperationSource,
        ReplicaState as ProtoReplicaState,
    },
    replica::{
        ReplicaError, ReplicaRuntime, SnapshotInspection, inspect_snapshot, verify_snapshot,
    },
};

use super::{
    admin::{self, AdminState, ShutdownNotice},
    identity::NodeIdentity,
    init::{self, InitResult},
    liveness::ParentLivenessPipe,
    lock::{DeploymentLock, admin_socket_path, ensure_runtime_directory},
    logging::{LogLevel, NodeLogger, new_correlation_id},
};

const STARTUP_DEADLINE: Duration = Duration::from_secs(10);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const LAUNCHER_TERMINATION_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub enum NodeError {
    Config(String),
    Unavailable(String),
    Operation(String),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Internal(String),
    NotImplemented,
}

impl NodeError {
    pub fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }

    pub fn config_io(operation: &'static str, path: PathBuf, source: io::Error) -> Self {
        Self::ConfigIo {
            operation,
            path,
            source,
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_) | Self::ConfigIo { .. } => crate::cli::EXIT_CONFIG,
            Self::Unavailable(_) | Self::NotImplemented => crate::cli::EXIT_UNAVAILABLE,
            Self::Operation(_) | Self::Io { .. } | Self::Internal(_) => 1,
        }
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message)
            | Self::Unavailable(message)
            | Self::Operation(message)
            | Self::Internal(message) => formatter.write_str(message),
            Self::Io { operation, source } => write!(formatter, "cannot {operation}: {source}"),
            Self::ConfigIo {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "cannot {operation} at {}: {source}",
                path.display()
            ),
            Self::NotImplemented => formatter.write_str("command is not implemented"),
        }
    }
}

pub fn execute(intent: PreparedCliIntent) -> Result<(), NodeError> {
    match intent {
        PreparedCliIntent::Init(intent) => match init::initialize(intent)? {
            InitResult::Initialized(identity) => {
                println!(
                    "initialized node {} ({})",
                    identity.node_name(),
                    identity.node_id()
                );
                Ok(())
            }
            InitResult::Cancelled => Ok(()),
        },
        PreparedCliIntent::Run(intent) => run_daemon(intent),
        PreparedCliIntent::Client(intent) => execute_client(intent),
    }
}

fn execute_client(intent: PreparedClientIntent) -> Result<(), NodeError> {
    match (intent.intent, intent.dependency) {
        (
            CliIntent::Replica(ReplicaIntent::SnapshotInspect { snapshot, json }),
            ClientDependency::None,
        ) => show_snapshot_inspection(&snapshot, json),
        (
            CliIntent::Replica(ReplicaIntent::SnapshotVerify { snapshot }),
            ClientDependency::None,
        ) => verify_local_snapshot(&snapshot),
        (CliIntent::Start, ClientDependency::ConfigRoot(config_root)) => start(&config_root),
        (CliIntent::Stop, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(stop(&config_root))
        }
        (CliIntent::Status { json }, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(show_status(&config_root, json))
        }
        (
            CliIntent::Log(LogIntent::Set { target, level }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(set_log_filter(&config_root, target, level)),
        (
            CliIntent::Replica(ReplicaIntent::Inspect { document }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(inspect_replica_document(&config_root, &document)),
        (
            CliIntent::Replica(ReplicaIntent::Ops {
                document,
                limit,
                format,
            }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(show_replica_operations(
            &config_root,
            &document,
            limit.map(|limit| limit.get()),
            format,
        )),
        (
            CliIntent::Replica(ReplicaIntent::Export { output }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(export_replica(&config_root, &output)),
        (
            CliIntent::Replica(ReplicaIntent::Import { snapshot }),
            ClientDependency::ConfigRoot(config_root),
        ) => {
            if !confirm_replica_import()? {
                return Ok(());
            }
            in_runtime(import_replica(&config_root, &snapshot))
        }
        _ => Err(NodeError::NotImplemented),
    }
}

fn run_daemon(intent: PreparedRunIntent) -> Result<(), NodeError> {
    in_runtime(run_daemon_async(intent))
}

async fn run_daemon_async(intent: PreparedRunIntent) -> Result<(), NodeError> {
    let lock = DeploymentLock::acquire_for_runtime(&intent.config_root)?;
    let identity = NodeIdentity::load(&intent.config_root)?;
    let (config_runtime, mut config) = ConfigRuntime::load(&intent.config_root)
        .map_err(|error| NodeError::Config(error.to_string()))?;
    intent.overrides.apply_to(&mut config);
    validate_storage_layout(
        &intent.config_root,
        &config.replica_root,
        &config.log_dir,
        &config.replica_store,
    )
    .map_err(|error| NodeError::Config(format!("invalid storage layout: {error}")))?;
    ensure_replica_slot(&config.replica_root)?;
    let parent_liveness = ParentLivenessPipe::create()?;

    let logger = NodeLogger::open(&config.log_dir, identity.clone())?;
    let startup_correlation = new_correlation_id();
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "node_starting",
        &startup_correlation,
        json!({ "config_root": intent.config_root.display().to_string() }),
    );

    let replica = match ReplicaRuntime::start(
        config.replica_root.clone(),
        &config.replica_store,
        identity.node_id(),
        Arc::clone(&logger),
    )
    .await
    {
        Ok(replica) => replica,
        Err(error) => {
            logger.emit(
                LogLevel::Error,
                "oll::node",
                "node_start_failed",
                &startup_correlation,
                json!({ "reason": "replica_runtime" }),
            );
            return Err(replica_node_error(error));
        }
    };

    let (listener, socket_guard) = match bind_admin_socket(&intent.config_root) {
        Ok(result) => result,
        Err(error) => {
            logger.emit(
                LogLevel::Error,
                "oll::node",
                "node_start_failed",
                &startup_correlation,
                json!({ "reason": "admin_socket" }),
            );
            let _ = replica.shutdown(Instant::now() + SHUTDOWN_DEADLINE).await;
            return Err(error);
        }
    };
    let (shutdown_tx, mut shutdown_rx) = watch::channel(ShutdownNotice::default());
    let state = Arc::new(AdminState::new(
        identity,
        config.clone(),
        Arc::clone(&logger),
        Arc::clone(&replica),
        shutdown_tx.clone(),
    ));
    state.mark_running();
    let mut admin_task = tokio::spawn(admin::serve(
        listener,
        Arc::clone(&state),
        shutdown_tx.subscribe(),
    ));
    let signal_task = tokio::spawn(wait_for_signal(
        Arc::clone(&state),
        shutdown_tx.clone(),
        Arc::clone(&logger),
    ));

    if let Err(error) = wait_for_admin_ready(&socket_guard.path).await {
        state.begin_shutdown();
        let _ = shutdown_tx.send(ShutdownNotice::requested(new_correlation_id()));
        let _ = wait_for_shutdown(&mut admin_task, &mut shutdown_rx, &replica).await;
        signal_task.abort();
        let _ = signal_task.await;
        return Err(error);
    }
    if let Some(pingback) = intent.pingback
        && let Err(error) = complete_pingback(pingback.as_socket_addr()).await
    {
        state.begin_shutdown();
        let _ = shutdown_tx.send(ShutdownNotice::requested(new_correlation_id()));
        let _ = wait_for_shutdown(&mut admin_task, &mut shutdown_rx, &replica).await;
        signal_task.abort();
        let _ = signal_task.await;
        return Err(error);
    }

    logger.emit(
        LogLevel::Info,
        "oll::node",
        "node_ready",
        &startup_correlation,
        json!({
            "process_id": std::process::id(),
            "configured_listen_address": config.listen.map(|address| address.to_string()),
        }),
    );

    let (shutdown_correlation, server_result, shutdown_deadline) =
        wait_for_shutdown(&mut admin_task, &mut shutdown_rx, &replica).await;
    signal_task.abort();
    let _ = signal_task.await;
    logger.emit(
        if server_result.is_ok() {
            LogLevel::Info
        } else {
            LogLevel::Error
        },
        "oll::node",
        if server_result.is_ok() {
            "node_shutdown_completed"
        } else {
            "node_shutdown_failed"
        },
        &shutdown_correlation,
        json!({}),
    );
    let _ = logger.flush_until(shutdown_deadline.into_std());
    drop(socket_guard);
    drop(lock);
    drop(parent_liveness);
    drop(config_runtime);
    server_result.map(|_| ())
}

async fn wait_for_shutdown(
    admin_task: &mut JoinHandle<Result<(), NodeError>>,
    shutdown: &mut watch::Receiver<ShutdownNotice>,
    replica: &ReplicaRuntime,
) -> (String, Result<(), NodeError>, Instant) {
    let (completed_admin, trigger_error) = tokio::select! {
        result = &mut *admin_task => {
            (Some(result), None)
        },
        changed = shutdown.changed() => {
            let error = changed.err().map(|_| {
                NodeError::Internal("daemon shutdown channel closed unexpectedly".to_owned())
            });
            (None, error)
        }
    };
    let notice = shutdown.borrow_and_update().clone();
    let correlation_id = notice
        .correlation_id()
        .map(str::to_owned)
        .unwrap_or_else(new_correlation_id);
    let deadline = notice.requested_at().unwrap_or_else(Instant::now) + SHUTDOWN_DEADLINE;

    let admin_drain = async {
        if let Some(result) = completed_admin {
            return join_admin_task(result);
        }
        match timeout_at(deadline, &mut *admin_task).await {
            Ok(result) => join_admin_task(result),
            Err(_) => {
                admin_task.abort();
                let _ = admin_task.await;
                Err(NodeError::Unavailable(
                    "daemon shutdown exceeded its graceful deadline".to_owned(),
                ))
            }
        }
    };
    let replica_drain = async { replica.shutdown(deadline).await.map_err(replica_node_error) };
    let (admin_result, replica_result) = tokio::join!(admin_drain, replica_drain);
    let result = trigger_error.map_or(admin_result, Err).and(replica_result);
    (correlation_id, result, deadline)
}

fn join_admin_task(
    result: Result<Result<(), NodeError>, tokio::task::JoinError>,
) -> Result<(), NodeError> {
    result.map_err(|error| NodeError::Internal(format!("Admin task failed: {error}")))?
}

async fn wait_for_admin_ready(socket: &Path) -> Result<(), NodeError> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    loop {
        match admin::get_status(socket, new_correlation_id()).await {
            Ok(_) => return Ok(()),
            Err(NodeError::Unavailable(_)) if Instant::now() < deadline => {
                sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_signal(
    state: Arc<AdminState>,
    shutdown: watch::Sender<ShutdownNotice>,
    logger: Arc<NodeLogger>,
) {
    let Ok(mut interrupt) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
    else {
        return;
    };
    let Ok(mut terminate) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        return;
    };

    let first = tokio::select! {
        _ = interrupt.recv() => libc::SIGINT,
        _ = terminate.recv() => libc::SIGTERM,
    };
    if state.begin_shutdown() {
        let correlation_id = new_correlation_id();
        logger.emit(
            LogLevel::Info,
            "oll::node",
            "node_shutdown_requested",
            &correlation_id,
            json!({ "signal": first }),
        );
        let _ = shutdown.send(ShutdownNotice::requested(correlation_id));
    }

    let second = tokio::select! {
        _ = interrupt.recv() => libc::SIGINT,
        _ = terminate.recv() => libc::SIGTERM,
    };
    std::process::exit(128 + second);
}

async fn complete_pingback(address: std::net::SocketAddr) -> Result<(), NodeError> {
    let nonce = tokio::task::spawn_blocking(|| -> Result<[u8; 32], NodeError> {
        let mut nonce = [0_u8; 32];
        let mut input = io::stdin().lock();
        input
            .read_exact(&mut nonce)
            .map_err(|error| NodeError::io("read startup pingback nonce", error))?;
        let mut extra = [0_u8; 1];
        if input
            .read(&mut extra)
            .map_err(|error| NodeError::io("read startup pingback nonce", error))?
            != 0
        {
            return Err(NodeError::Unavailable(
                "startup pingback nonce has unexpected trailing bytes".to_owned(),
            ));
        }
        Ok(nonce)
    })
    .await
    .map_err(|error| NodeError::Internal(format!("cannot join pingback reader: {error}")))??;
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| NodeError::io("connect startup pingback listener", error))?;
    stream
        .write_all(&nonce)
        .await
        .map_err(|error| NodeError::io("write startup pingback nonce", error))?;
    stream
        .shutdown()
        .await
        .map_err(|error| NodeError::io("close startup pingback connection", error))
}

fn ensure_replica_slot(path: &Path) -> Result<(), NodeError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(NodeError::Config(format!(
            "replica root {} is not a real directory",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path)
                .map_err(|error| NodeError::io("create empty replica slot", error))?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| NodeError::io("set replica slot permissions", error))
        }
        Err(error) => Err(NodeError::io("inspect replica slot", error)),
    }
}

struct AdminSocketGuard {
    path: PathBuf,
}

impl Drop for AdminSocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn bind_admin_socket(config_root: &Path) -> Result<(UnixListener, AdminSocketGuard), NodeError> {
    ensure_runtime_directory(config_root)?;
    let path = admin_socket_path(config_root);
    recover_stale_socket(&path)?;
    let listener =
        UnixListener::bind(&path).map_err(|error| NodeError::io("bind Admin UDS", error))?;
    let guard = AdminSocketGuard { path };
    if let Err(error) =
        std::fs::set_permissions(&guard.path, std::fs::Permissions::from_mode(0o600))
    {
        drop(guard);
        return Err(NodeError::io("set Admin UDS permissions", error));
    }
    Ok((listener, guard))
}

fn recover_stale_socket(path: &Path) -> Result<(), NodeError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(NodeError::io("inspect Admin UDS path", error)),
    };
    if !metadata.file_type().is_socket() {
        return Err(NodeError::Config(format!(
            "Admin UDS path {} is not a socket",
            path.display()
        )));
    }
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        return Err(NodeError::Unavailable(
            "an Admin endpoint already answers for this deployment".to_owned(),
        ));
    }
    std::fs::remove_file(path).map_err(|error| NodeError::io("remove stale Admin UDS", error))
}

fn start(config_root: &Path) -> Result<(), NodeError> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    DeploymentLock::preflight(config_root)?;
    let admin_socket = admin_socket_path(config_root);
    if std::os::unix::net::UnixStream::connect(&admin_socket).is_ok() {
        return Err(NodeError::Unavailable(
            "an Admin endpoint already answers for this deployment".to_owned(),
        ));
    }
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|error| NodeError::io("bind startup pingback listener", error))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|error| NodeError::io("configure startup pingback listener", error))?;
    let address = std_listener
        .local_addr()
        .map_err(|error| NodeError::io("read startup pingback address", error))?;
    let mut nonce = [0_u8; 32];
    fill_random(&mut nonce)
        .map_err(|error| NodeError::Internal(format!("cannot generate startup nonce: {error}")))?;

    let executable = std::env::current_exe()
        .map_err(|error| NodeError::io("locate the oll executable", error))?;
    let mut command = tokio::process::Command::new(executable);
    command
        .arg("run")
        .arg("--config")
        .arg(config_root)
        .arg("--pingback")
        .arg(address.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    in_runtime(start_async(std_listener, command, nonce, deadline))
}

async fn start_async(
    std_listener: std::net::TcpListener,
    mut command: tokio::process::Command,
    nonce: [u8; 32],
    deadline: Instant,
) -> Result<(), NodeError> {
    let listener = TcpListener::from_std(std_listener)
        .map_err(|error| NodeError::io("adopt startup pingback listener", error))?;
    let mut child = command
        .spawn()
        .map_err(|error| NodeError::io("spawn detached oll run", error))?;
    let write_result = async {
        let stdin = child.stdin.as_mut().ok_or_else(|| {
            NodeError::Internal("detached oll run did not expose its stdin pipe".to_owned())
        })?;
        stdin
            .write_all(&nonce)
            .await
            .map_err(|error| NodeError::io("write startup nonce", error))?;
        stdin
            .shutdown()
            .await
            .map_err(|error| NodeError::io("close startup nonce pipe", error))
    }
    .await;
    if let Err(error) = write_result {
        terminate_child(&mut child).await;
        return Err(error);
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        terminate_child(&mut child).await;
        return Err(NodeError::Unavailable(
            "oll start timed out before daemon readiness".to_owned(),
        ));
    }
    let handshake = tokio::select! {
        result = async {
            timeout(remaining, async {
                let (mut stream, _) = listener.accept().await
                    .map_err(|error| NodeError::io("accept startup pingback", error))?;
                let mut reply = [0_u8; 32];
                stream.read_exact(&mut reply).await
                    .map_err(|error| NodeError::io("read startup pingback", error))?;
                Ok::<[u8; 32], NodeError>(reply)
            }).await
        } => match result {
            Ok(result) => result,
            Err(_) => Err(NodeError::Unavailable("oll start timed out before daemon readiness".to_owned())),
        },
        status = child.wait() => match status {
            Ok(status) => Err(NodeError::Unavailable(format!("oll run exited before readiness: {status}"))),
            Err(error) => Err(NodeError::io("wait for detached oll run", error)),
        },
    };
    match handshake {
        Ok(reply) if constant_time_eq(&nonce, &reply) => Ok(()),
        Ok(_) => {
            terminate_child(&mut child).await;
            Err(NodeError::Unavailable(
                "oll start received an invalid readiness pingback".to_owned(),
            ))
        }
        Err(error) => {
            terminate_child(&mut child).await;
            Err(error)
        }
    }
}

async fn terminate_child(child: &mut Child) {
    if let Some(process_id) = child.id() {
        unsafe {
            libc::kill(process_id as i32, libc::SIGTERM);
        }
    }
    if timeout(LAUNCHER_TERMINATION_GRACE, child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

async fn stop(config_root: &Path) -> Result<(), NodeError> {
    let socket = admin_socket_path(config_root);
    // A daemon that has already accepted another Shutdown no longer serves
    // GetStatus, but Shutdown itself remains idempotently available until the
    // Admin server begins closing its listener.
    let process_id = match admin::get_status(&socket, new_correlation_id()).await {
        Ok(status) => Some(status.process_id),
        Err(NodeError::Unavailable(_)) => None,
        Err(error) => return Err(error),
    };
    admin::request_shutdown(&socket, new_correlation_id()).await?;

    let deadline = Instant::now() + SHUTDOWN_DEADLINE;
    loop {
        let lock_free = match DeploymentLock::preflight(config_root) {
            Ok(()) => true,
            Err(NodeError::Unavailable(_)) => false,
            Err(error) => return Err(error),
        };
        let socket_gone = !socket.exists();
        let process_exited = process_id.is_some_and(process_has_exited);
        if lock_free && (socket_gone || process_exited) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(NodeError::Unavailable(
                "oll stop timed out waiting for the daemon to exit".to_owned(),
            ));
        }
        sleep(Duration::from_millis(25)).await;
    }
}

fn process_has_exited(process_id: u32) -> bool {
    let result = unsafe { libc::kill(process_id as i32, 0) };
    result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

fn show_snapshot_inspection(path: &Path, as_json: bool) -> Result<(), NodeError> {
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

fn verify_local_snapshot(path: &Path) -> Result<(), NodeError> {
    let inspection = verify_snapshot(path).map_err(replica_node_error)?;
    println!("verified snapshot {}", inspection.snapshot_id);
    Ok(())
}

async fn inspect_replica_document(config_root: &Path, document: &Path) -> Result<(), NodeError> {
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

async fn show_replica_operations(
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

async fn export_replica(config_root: &Path, output: &Path) -> Result<(), NodeError> {
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

async fn import_replica(config_root: &Path, snapshot: &Path) -> Result<(), NodeError> {
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

fn confirm_replica_import() -> Result<bool, NodeError> {
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

async fn show_status(config_root: &Path, as_json: bool) -> Result<(), NodeError> {
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

async fn set_log_filter(
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

fn in_runtime<T>(future: impl Future<Output = Result<T, NodeError>>) -> Result<T, NodeError> {
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

fn replica_node_error(error: ReplicaError) -> NodeError {
    match error {
        ReplicaError::Uninitialized => NodeError::Unavailable("no local replica yet".to_owned()),
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

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink};

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn replica_slot_must_be_a_real_directory() {
        let directory = TempDir::new().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("replica");
        fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        assert!(matches!(
            ensure_replica_slot(&link),
            Err(NodeError::Config(_))
        ));
    }
}
