use std::{
    fmt,
    future::Future,
    io::{self, Read},
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use getrandom::fill as fill_random;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener},
    process::Child,
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout},
};

use crate::{
    cli::{
        CliIntent, ClientDependency, LogIntent, PreparedCliIntent, PreparedClientIntent,
        PreparedRunIntent,
    },
    configuration::ConfigRuntime,
    protocol::oll::{GetStatusResponse, NodeLifecycleState, PeerConnectionState},
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
            Self::Io { .. } | Self::Internal(_) => 1,
        }
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) | Self::Unavailable(message) | Self::Internal(message) => {
                formatter.write_str(message)
            }
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
    let ClientDependency::ConfigRoot(config_root) = intent.dependency else {
        return Err(NodeError::NotImplemented);
    };
    match intent.intent {
        CliIntent::Start => start(&config_root),
        CliIntent::Stop => in_runtime(stop(&config_root)),
        CliIntent::Status { json } => in_runtime(show_status(&config_root, json)),
        CliIntent::Log(LogIntent::Set { target, level }) => {
            in_runtime(set_log_filter(&config_root, target, level))
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
    )?;

    let (listener, socket_guard) = match bind_admin_socket(&intent.config_root) {
        Ok(result) => result,
        Err(error) => {
            let _ = logger.emit(
                LogLevel::Error,
                "oll::node",
                "node_start_failed",
                &startup_correlation,
                json!({ "reason": "admin_socket" }),
            );
            return Err(error);
        }
    };
    let (shutdown_tx, mut shutdown_rx) = watch::channel(ShutdownNotice::default());
    let state = Arc::new(AdminState::new(
        identity,
        config.clone(),
        Arc::clone(&logger),
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
        let _ = shutdown_tx.send(ShutdownNotice::requested(new_correlation_id()));
        let _ = admin_task.await;
        signal_task.abort();
        return Err(error);
    }
    if let Some(pingback) = intent.pingback
        && let Err(error) = complete_pingback(pingback.as_socket_addr()).await
    {
        let _ = shutdown_tx.send(ShutdownNotice::requested(new_correlation_id()));
        let _ = admin_task.await;
        signal_task.abort();
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
    )?;

    let server_result = wait_for_shutdown(&mut admin_task, &mut shutdown_rx).await;
    signal_task.abort();

    let shutdown_correlation = server_result
        .as_ref()
        .ok()
        .cloned()
        .unwrap_or_else(new_correlation_id);
    let _ = logger.emit(
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
    logger.flush()?;
    drop(socket_guard);
    drop(lock);
    drop(parent_liveness);
    drop(config_runtime);
    server_result.map(|_| ())
}

async fn wait_for_shutdown(
    admin_task: &mut JoinHandle<Result<(), NodeError>>,
    shutdown: &mut watch::Receiver<ShutdownNotice>,
) -> Result<String, NodeError> {
    tokio::select! {
        result = &mut *admin_task => {
            let correlation_id = shutdown
                .borrow()
                .correlation_id()
                .map(str::to_owned)
                .unwrap_or_else(new_correlation_id);
            join_admin_task(result).map(|_| correlation_id)
        },
        changed = shutdown.changed() => {
            changed.map_err(|_| NodeError::Internal("daemon shutdown channel closed unexpectedly".to_owned()))?;
            let correlation_id = shutdown
                .borrow_and_update()
                .correlation_id()
                .map(str::to_owned)
                .unwrap_or_else(new_correlation_id);
            match timeout(SHUTDOWN_DEADLINE, &mut *admin_task).await {
                Ok(result) => join_admin_task(result).map(|_| correlation_id),
                Err(_) => {
                    admin_task.abort();
                    let _ = admin_task.await;
                    Err(NodeError::Unavailable("daemon shutdown exceeded its graceful deadline".to_owned()))
                }
            }
        }
    }
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
        let _ = logger.emit(
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
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(NodeError::Config(format!(
            "replica root {} is not a directory",
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

async fn show_status(config_root: &Path, as_json: bool) -> Result<(), NodeError> {
    let status = admin::get_status(&admin_socket_path(config_root), new_correlation_id()).await?;
    if as_json {
        println!("{}", status_json(&status)?);
    } else {
        print_status(&status)?;
    }
    Ok(())
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
                "connect_url": peer.connect_url,
                "connection_state": peer_state_name(peer.connection_state),
                "node": peer.node.as_ref().map(|node| json!({
                    "node_id": node.node_id.as_ref().map(|value| value.value.clone()),
                    "node_name": node.node_name.as_ref().map(|value| value.value.clone()),
                })),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "node_id": node_id.value,
        "node_name": node_name.value,
        "lifecycle": lifecycle_name(status.lifecycle),
        "started_at": status.started_at.as_ref().map(format_timestamp),
        "process_id": status.process_id,
        "configured_listen_address": status.configured_listen_address,
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
            println!(
                "  {} ({})",
                peer.connect_url,
                peer_state_name(peer.connection_state)
            );
        }
    }
    Ok(())
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
        PeerConnectionState::Unspecified => "unknown",
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
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| NodeError::Internal(format!("cannot initialize Tokio runtime: {error}")))?
        .block_on(future)
}
