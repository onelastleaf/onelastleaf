use std::{
    io::{self, Read},
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::Arc,
    time::Duration,
};

use serde_json::json;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};

use crate::{
    cli::PreparedRunIntent,
    configuration::{ConfigRuntime, validate_storage_layout},
    node::{
        admin::{self, AdminState, ShutdownNotice},
        identity::{IdentityCoordinator, NodeIdentity},
        liveness::ParentLivenessPipe,
        lock::DeploymentLock,
        logging::{LogLevel, NodeLogger, new_correlation_id},
    },
    replica::ReplicaRuntime,
    sync::SyncRuntime,
};

use super::{
    NodeError, SHUTDOWN_DEADLINE, STARTUP_DEADLINE,
    blocking::{in_runtime, replica_node_error, sync_node_error},
    identity_watch::IdentityWatch,
    socket::bind_admin_socket,
};

pub(super) fn run_daemon(intent: PreparedRunIntent) -> Result<(), NodeError> {
    in_runtime(run_daemon_async(intent))
}

async fn run_daemon_async(intent: PreparedRunIntent) -> Result<(), NodeError> {
    let lock = DeploymentLock::acquire_for_runtime(&intent.config_root)?;
    let identity = NodeIdentity::load(&intent.config_root)?;
    let (config_runtime, mut config) = ConfigRuntime::load(&intent.config_root)
        .map_err(|error| NodeError::Config(error.to_string()))?;
    intent.overrides.apply_to(&mut config);
    config
        .validate_sync_topology()
        .map_err(|error| NodeError::Config(error.to_owned()))?;
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
    let identities = IdentityCoordinator::new(identity);
    let startup_correlation = new_correlation_id();
    if config
        .network_key
        .as_ref()
        .is_some_and(|network_key| network_key.expose().len() < 32)
    {
        println!(
            "WARNING: node.network_key is shorter than 32 bytes; use `oll psk` to generate a stronger key"
        );
        logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "weak_network_key_configured",
            &startup_correlation,
            json!({ "minimum_recommended_bytes": 32 }),
        );
    }
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "node_starting",
        &startup_correlation,
        json!({ "config_root": intent.config_root.display().to_string() }),
    );

    let replica = match ReplicaRuntime::start(
        intent.config_root.clone(),
        config.replica_root.clone(),
        &config.replica_store,
        Arc::clone(&identities),
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

    let sync = match SyncRuntime::start(
        &config,
        Arc::clone(&identities),
        Arc::clone(&replica),
        Arc::clone(&logger),
    )
    .await
    {
        Ok(sync) => sync,
        Err(error) => {
            logger.emit(
                LogLevel::Error,
                "oll::node",
                "node_start_failed",
                &startup_correlation,
                json!({ "reason": "sync_runtime" }),
            );
            let _ = replica.shutdown(Instant::now() + SHUTDOWN_DEADLINE).await;
            return Err(sync_node_error(error));
        }
    };

    let mut identity_watch = match IdentityWatch::start(
        &intent.config_root,
        Arc::clone(&identities),
        Arc::clone(&replica),
        Arc::clone(&logger),
    )
    .await
    {
        Ok(watch) => watch,
        Err(error) => {
            logger.emit(
                LogLevel::Error,
                "oll::node",
                "node_start_failed",
                &startup_correlation,
                json!({ "reason": "identity_watcher" }),
            );
            let deadline = Instant::now() + SHUTDOWN_DEADLINE;
            let _ = sync.shutdown(deadline).await;
            let _ = replica.shutdown(deadline).await;
            return Err(error);
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
            let deadline = Instant::now() + SHUTDOWN_DEADLINE;
            let _ = identity_watch.shutdown(deadline).await;
            let _ = sync.shutdown(deadline).await;
            let _ = replica.shutdown(deadline).await;
            return Err(error);
        }
    };
    let (shutdown_tx, mut shutdown_rx) = watch::channel(ShutdownNotice::default());
    let state = Arc::new(AdminState::new(
        identities,
        config.clone(),
        Arc::clone(&logger),
        Arc::clone(&replica),
        Arc::clone(&sync),
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
        let _ = wait_for_shutdown(
            &mut admin_task,
            &mut shutdown_rx,
            &replica,
            &sync,
            &mut identity_watch,
        )
        .await;
        signal_task.abort();
        let _ = signal_task.await;
        return Err(error);
    }
    if let Some(pingback) = intent.pingback
        && let Err(error) = complete_pingback(pingback.as_socket_addr()).await
    {
        state.begin_shutdown();
        let _ = shutdown_tx.send(ShutdownNotice::requested(new_correlation_id()));
        let _ = wait_for_shutdown(
            &mut admin_task,
            &mut shutdown_rx,
            &replica,
            &sync,
            &mut identity_watch,
        )
        .await;
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

    let (shutdown_correlation, server_result, shutdown_deadline) = wait_for_shutdown(
        &mut admin_task,
        &mut shutdown_rx,
        &replica,
        &sync,
        &mut identity_watch,
    )
    .await;
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
    sync: &SyncRuntime,
    identity_watch: &mut IdentityWatch,
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
    let sync_drain = async { sync.shutdown(deadline).await.map_err(sync_node_error) };
    let identity_drain = identity_watch.shutdown(deadline);
    let (admin_result, sync_result, replica_result, identity_result) =
        tokio::join!(admin_drain, sync_drain, replica_drain, identity_drain);
    let result = trigger_error
        .map_or(admin_result, Err)
        .and(sync_result)
        .and(replica_result)
        .and(identity_result);
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

pub(super) fn ensure_replica_slot(path: &Path) -> Result<(), NodeError> {
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
