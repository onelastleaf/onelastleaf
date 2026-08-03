use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    future::Future,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify, RwLock, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep, sleep_until, timeout_at},
};
use uuid::Uuid;

use crate::{
    cli::NodeName,
    configuration::{ConnectUrl, ResolvedNodeConfig},
    node::{
        identity::{IdentityCoordinator, NodeIdentity},
        logging::{LogLevel, NodeLogger, new_correlation_id},
    },
    protocol::oll::{
        ErrorCode, PeerConnectionDirection, PeerConnectionState, PeerStatus, PeerSyncOutcome,
        PeerSyncResult, SyncCloseCode, SyncPing, SyncPong, SyncRoundRequest, sync_envelope,
    },
    replica::{BootstrapClaim, PeerBinding, ReplicaError, ReplicaRuntime, ReplicaStatus},
};

use super::{
    HANDSHAKE_DEADLINE, NoiseTransport, PendingSession, RoundError, RoundResult, SessionChannel,
    SessionError, SessionReplicaMode, derive_noise_psk, receive_bootstrap_round, receive_round,
    security::NoisePsk, send_bootstrap_round, send_round,
};

const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAXIMUM_BACKOFF: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const PING_CALL_DEADLINE: Duration = Duration::from_secs(9);
#[cfg(test)]
const PING_CALL_DEADLINE: Duration = Duration::from_millis(300);
#[cfg(not(test))]
const PING_RESPONSE_DEADLINE: Duration = Duration::from_secs(8);
#[cfg(test)]
const PING_RESPONSE_DEADLINE: Duration = Duration::from_millis(100);
const SESSION_WAIT_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SyncError {
    NotFound(String),
    FailedPrecondition(String),
    Unavailable(String),
    Protocol(String),
    Store,
    Internal(String),
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message)
            | Self::FailedPrecondition(message)
            | Self::Unavailable(message)
            | Self::Protocol(message)
            | Self::Internal(message) => formatter.write_str(message),
            Self::Store => formatter.write_str("sync peer state could not be persisted"),
        }
    }
}

impl std::error::Error for SyncError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn to_proto(self) -> PeerConnectionDirection {
        match self {
            Self::Inbound => PeerConnectionDirection::Inbound,
            Self::Outbound => PeerConnectionDirection::Outbound,
        }
    }
}

struct ActiveSession {
    session_id: Uuid,
    direction: Direction,
    connect_target: Option<String>,
    preferred_direction: bool,
    handshake_hash: [u8; 32],
    commands: mpsc::Sender<SessionCommand>,
    cancel: watch::Sender<Option<SyncCloseCode>>,
}

enum SessionCommand {
    Ping {
        correlation_id: String,
        response: oneshot::Sender<Result<Duration, SyncError>>,
    },
    Synchronize {
        correlation_id: String,
        response: oneshot::Sender<Result<RoundResult, SyncError>>,
    },
}

struct PendingPing {
    sent_message_id: u64,
    started: Instant,
    deadline: Instant,
    response: oneshot::Sender<Result<Duration, SyncError>>,
}

pub(crate) struct SyncRuntime {
    identities: Arc<IdentityCoordinator>,
    replica: Arc<ReplicaRuntime>,
    logger: Arc<NodeLogger>,
    psk: Option<Arc<NoisePsk>>,
    configured_targets: Vec<ConnectUrl>,
    target_states: RwLock<HashMap<String, PeerConnectionState>>,
    bindings: RwLock<Vec<PeerBinding>>,
    sessions: Mutex<HashMap<Uuid, ActiveSession>>,
    session_changed: Notify,
    shutdown: watch::Sender<bool>,
    accepting_tasks: AtomicBool,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
}

impl SyncRuntime {
    pub(crate) async fn start(
        config: &ResolvedNodeConfig,
        identities: Arc<IdentityCoordinator>,
        replica: Arc<ReplicaRuntime>,
        logger: Arc<NodeLogger>,
    ) -> Result<Arc<Self>, SyncError> {
        let listener = match config.listen {
            Some(address) => Some(TcpListener::bind(address).await.map_err(|error| {
                SyncError::Unavailable(format!("cannot bind sync listener {address}: {error}"))
            })?),
            None => None,
        };
        let psk = config
            .network_key
            .as_ref()
            .map(derive_noise_psk)
            .map(Arc::new);
        let bindings = replica
            .sync_peer_bindings()
            .await
            .map_err(|_| SyncError::Store)?;
        let target_states = config
            .connect
            .iter()
            .map(|target| (target.to_string(), PeerConnectionState::Pending))
            .collect();
        let (shutdown, _) = watch::channel(false);
        let runtime = Arc::new(Self {
            identities,
            replica,
            logger,
            psk,
            configured_targets: config.connect.clone(),
            target_states: RwLock::new(target_states),
            bindings: RwLock::new(bindings),
            sessions: Mutex::new(HashMap::new()),
            session_changed: Notify::new(),
            shutdown,
            accepting_tasks: AtomicBool::new(true),
            tasks: StdMutex::new(Vec::new()),
        });

        if let Some(listener) = listener {
            let weak = Arc::downgrade(&runtime);
            let shutdown = runtime.shutdown.subscribe();
            runtime.spawn(async move {
                run_listener(weak, listener, shutdown).await;
            });
        }
        for target in runtime.configured_targets.clone() {
            let weak = Arc::downgrade(&runtime);
            let shutdown = runtime.shutdown.subscribe();
            runtime.spawn(async move {
                run_outbound(weak, target, shutdown).await;
            });
        }
        Ok(runtime)
    }

    fn spawn(self: &Arc<Self>, future: impl Future<Output = ()> + Send + 'static) {
        if !self.accepting_tasks.load(Ordering::Acquire) {
            return;
        }
        let handle = tokio::spawn(future);
        let mut tasks = self
            .tasks
            .lock()
            .expect("sync task registry lock is poisoned");
        tasks.retain(|task| !task.is_finished());
        if self.accepting_tasks.load(Ordering::Acquire) {
            tasks.push(handle);
        } else {
            handle.abort();
        }
    }

    pub(crate) async fn status(&self) -> Vec<PeerStatus> {
        let bindings = self.bindings.read().await.clone();
        let sessions = self.sessions.lock().await;
        let target_states = self.target_states.read().await;
        let mut represented = BTreeSet::new();
        let mut statuses = Vec::new();
        for target in &self.configured_targets {
            let target = target.to_string();
            let binding = bindings
                .iter()
                .find(|binding| binding.connect_targets.iter().any(|known| known == &target));
            if let Some(binding) = binding {
                represented.insert(binding.identity.node_id());
            }
            let active = binding.and_then(|binding| sessions.get(&binding.identity.node_id()));
            statuses.push(PeerStatus {
                connect_target: Some(target.clone()),
                node: binding.map(|binding| binding.identity.to_proto()),
                connection_state: active.map_or_else(
                    || {
                        target_states
                            .get(&target)
                            .copied()
                            .unwrap_or(PeerConnectionState::Pending) as i32
                    },
                    |_| PeerConnectionState::Ready as i32,
                ),
                direction: active.map_or(PeerConnectionDirection::Outbound as i32, |session| {
                    session.direction.to_proto() as i32
                }),
            });
        }
        for binding in bindings {
            if represented.contains(&binding.identity.node_id()) {
                continue;
            }
            let active = sessions.get(&binding.identity.node_id());
            statuses.push(PeerStatus {
                connect_target: active.and_then(|session| session.connect_target.clone()),
                node: Some(binding.identity.to_proto()),
                connection_state: active.map_or(PeerConnectionState::Pending as i32, |_| {
                    PeerConnectionState::Ready as i32
                }),
                direction: active.map_or(PeerConnectionDirection::Inbound as i32, |session| {
                    session.direction.to_proto() as i32
                }),
            });
        }
        statuses
    }

    pub(crate) async fn ping(
        &self,
        node_name: &NodeName,
        correlation_id: &str,
    ) -> Result<(NodeIdentity, Duration), SyncError> {
        let started = Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_ping_started",
            correlation_id,
            json!({ "peer_node_name": node_name.as_str() }),
        );
        let result = async {
            let binding = self
                .bindings
                .read()
                .await
                .iter()
                .find(|binding| binding.identity.node_name() == node_name)
                .cloned()
                .ok_or_else(|| {
                    SyncError::NotFound("sync node name has not been authenticated".to_owned())
                })?;
            let deadline = Instant::now() + PING_CALL_DEADLINE;
            let commands = loop {
                let notified = self.session_changed.notified();
                if let Some(commands) = self
                    .sessions
                    .lock()
                    .await
                    .get(&binding.identity.node_id())
                    .map(|session| session.commands.clone())
                {
                    break commands;
                }
                timeout_at(deadline, notified).await.map_err(|_| {
                    SyncError::Unavailable("authenticated sync peer is not connected".to_owned())
                })?;
            };
            let (response, receiver) = oneshot::channel();
            commands
                .send(SessionCommand::Ping {
                    correlation_id: correlation_id.to_owned(),
                    response,
                })
                .await
                .map_err(|_| {
                    SyncError::Unavailable("sync peer session closed before ping".to_owned())
                })?;
            let duration = timeout_at(deadline, receiver)
                .await
                .map_err(|_| SyncError::Unavailable("sync ping timed out".to_owned()))?
                .map_err(|_| {
                    SyncError::Unavailable("sync peer session closed during ping".to_owned())
                })??;
            Ok((binding.identity, duration))
        }
        .await;
        match &result {
            Ok((identity, duration)) => self.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_ping_completed",
                correlation_id,
                json!({
                    "peer_node_id": identity.node_id().to_string(),
                    "peer_node_name": identity.node_name().as_str(),
                    "round_trip_ms": u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            ),
            Err(error) => self.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_ping_failed",
                correlation_id,
                json!({
                    "peer_node_name": node_name.as_str(),
                    "error_code": sync_error_name(error),
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            ),
        }
        result
    }

    pub(crate) async fn synchronize(
        &self,
        node_name: Option<&NodeName>,
        total_attempts: u32,
        correlation_id: &str,
    ) -> Result<Vec<PeerSyncResult>, SyncError> {
        if total_attempts == 0 {
            return Err(SyncError::FailedPrecondition(
                "synchronization attempts must be greater than zero".to_owned(),
            ));
        }
        let bindings = self.bindings.read().await.clone();
        let targets = if let Some(node_name) = node_name {
            let binding = bindings
                .iter()
                .find(|binding| binding.identity.node_name() == node_name)
                .ok_or_else(|| {
                    SyncError::NotFound("sync node name has not been authenticated".to_owned())
                })?;
            let connect_target = binding
                .connect_targets
                .iter()
                .find(|candidate| {
                    self.configured_targets
                        .iter()
                        .any(|configured| configured.to_string() == candidate.as_str())
                })
                .cloned();
            vec![(connect_target, Some(binding.identity.clone()))]
        } else {
            if self.configured_targets.is_empty() {
                return Err(SyncError::FailedPrecondition(
                    "no configured sync peers are available".to_owned(),
                ));
            }
            self.configured_targets
                .iter()
                .map(|target| {
                    let target = target.to_string();
                    let identity = bindings
                        .iter()
                        .find(|binding| {
                            binding.connect_targets.iter().any(|known| known == &target)
                        })
                        .map(|binding| binding.identity.clone());
                    (Some(target), identity)
                })
                .collect()
        };

        let mut results = Vec::with_capacity(targets.len());
        for (connect_target, mut identity) in targets {
            self.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_round_started",
                correlation_id,
                json!({
                    "connect_target": connect_target,
                    "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                    "total_attempts": total_attempts,
                }),
            );
            let mut last_error = None;
            let mut success = None;
            let mut attempts_used = 0;
            for attempt in 1..=total_attempts {
                attempts_used = attempt;
                match self
                    .synchronize_once(connect_target.as_deref(), &mut identity, correlation_id)
                    .await
                {
                    Ok(result) => {
                        success = Some(result);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            let result = match success {
                Some(round) => {
                    let outcome = if round == RoundResult::default() {
                        PeerSyncOutcome::AlreadySatisfied
                    } else {
                        PeerSyncOutcome::Synchronized
                    };
                    self.logger.emit(
                        LogLevel::Info,
                        "oll::sync",
                        "sync_round_completed",
                        correlation_id,
                        json!({
                            "connect_target": connect_target,
                            "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                            "attempts_used": attempts_used,
                            "object_count": round.object_count,
                            "blob_count": round.blob_count,
                            "bytes": round.transferred_bytes,
                        }),
                    );
                    PeerSyncResult {
                        connect_target,
                        node: identity.as_ref().map(NodeIdentity::to_proto),
                        attempts_used,
                        outcome: outcome as i32,
                        object_count: round.object_count,
                        blob_count: round.blob_count,
                        transferred_bytes: round.transferred_bytes,
                        error_code: ErrorCode::Unspecified as i32,
                        error_message: String::new(),
                    }
                }
                None => {
                    let error = last_error.unwrap_or_else(|| {
                        SyncError::Unavailable("sync attempt did not run".to_owned())
                    });
                    self.logger.emit(
                        LogLevel::Warn,
                        "oll::sync",
                        "sync_round_failed",
                        correlation_id,
                        json!({
                            "connect_target": connect_target,
                            "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                            "attempts_used": attempts_used,
                            "error_code": sync_error_name(&error),
                        }),
                    );
                    PeerSyncResult {
                        connect_target,
                        node: identity.as_ref().map(NodeIdentity::to_proto),
                        attempts_used,
                        outcome: PeerSyncOutcome::Failed as i32,
                        object_count: 0,
                        blob_count: 0,
                        transferred_bytes: 0,
                        error_code: sync_error_code(&error) as i32,
                        error_message: error.to_string(),
                    }
                }
            };
            results.push(result);
        }
        Ok(results)
    }

    async fn synchronize_once(
        &self,
        connect_target: Option<&str>,
        identity: &mut Option<NodeIdentity>,
        correlation_id: &str,
    ) -> Result<RoundResult, SyncError> {
        let deadline = Instant::now() + SESSION_WAIT_DEADLINE;
        let commands = loop {
            let notified = self.session_changed.notified();
            if identity.is_none() {
                *identity = self
                    .bindings
                    .read()
                    .await
                    .iter()
                    .find(|binding| {
                        connect_target.is_some_and(|target| {
                            binding.connect_targets.iter().any(|known| known == target)
                        })
                    })
                    .map(|binding| binding.identity.clone());
            }
            if let Some(identity) = identity
                && let Some(commands) = self
                    .sessions
                    .lock()
                    .await
                    .get(&identity.node_id())
                    .map(|session| session.commands.clone())
            {
                break commands;
            }
            timeout_at(deadline, notified).await.map_err(|_| {
                SyncError::Unavailable("authenticated sync peer is not connected".to_owned())
            })?;
        };
        let (response, receiver) = oneshot::channel();
        commands
            .send(SessionCommand::Synchronize {
                correlation_id: correlation_id.to_owned(),
                response,
            })
            .await
            .map_err(|_| {
                SyncError::Unavailable("sync peer session closed before the round".to_owned())
            })?;
        receiver.await.map_err(|_| {
            SyncError::Unavailable("sync peer session closed during the round".to_owned())
        })?
    }

    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<(), SyncError> {
        self.accepting_tasks.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
        for session in self.sessions.lock().await.values() {
            let _ = session.cancel.send(Some(SyncCloseCode::ShuttingDown));
        }
        self.session_changed.notify_waiters();
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .expect("sync task registry lock is poisoned"),
        );
        for mut task in tasks {
            match timeout_at(deadline, &mut task).await {
                Ok(_) => {}
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        let sessions_exceeded_deadline = {
            let mut sessions = self.sessions.lock().await;
            let exceeded = !sessions.is_empty();
            sessions.clear();
            exceeded
        };
        self.session_changed.notify_waiters();
        if sessions_exceeded_deadline {
            return Err(SyncError::Unavailable(
                "sync sessions exceeded the node shutdown deadline".to_owned(),
            ));
        }
        Ok(())
    }

    async fn register_session(
        &self,
        remote: NodeIdentity,
        direction: Direction,
        connect_target: Option<String>,
        handshake_hash: [u8; 32],
        commands: mpsc::Sender<SessionCommand>,
        cancel: watch::Sender<Option<SyncCloseCode>>,
    ) -> Result<Uuid, SyncError> {
        let local_id = self.identities.node_id().await;
        let preferred_direction = match direction {
            Direction::Outbound => local_id < remote.node_id(),
            Direction::Inbound => remote.node_id() < local_id,
        };
        let session_id = Uuid::new_v4();
        let mut sessions = self.sessions.lock().await;
        if let Some(existing) = sessions.get(&remote.node_id()) {
            let existing_rank = (!existing.preferred_direction, existing.handshake_hash);
            let new_rank = (!preferred_direction, handshake_hash);
            if existing_rank <= new_rank {
                return Err(SyncError::Protocol(
                    "duplicate sync session lost arbitration".to_owned(),
                ));
            }
            let _ = existing.cancel.send(Some(SyncCloseCode::DuplicateSession));
        }
        sessions.insert(
            remote.node_id(),
            ActiveSession {
                session_id,
                direction,
                connect_target,
                preferred_direction,
                handshake_hash,
                commands,
                cancel,
            },
        );
        drop(sessions);
        self.session_changed.notify_waiters();
        Ok(session_id)
    }

    async fn remove_session(&self, remote_id: Uuid, session_id: Uuid) {
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(&remote_id)
            .is_some_and(|session| session.session_id == session_id)
        {
            sessions.remove(&remote_id);
        }
        drop(sessions);
        self.session_changed.notify_waiters();
    }

    async fn refresh_bindings(&self) -> Result<(), SyncError> {
        *self.bindings.write().await = self
            .replica
            .sync_peer_bindings()
            .await
            .map_err(|_| SyncError::Store)?;
        self.session_changed.notify_waiters();
        Ok(())
    }

    fn log_transport_failure(
        &self,
        correlation_id: &str,
        direction: Direction,
        error: &SessionError,
    ) {
        self.logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "sync_session_failed",
            correlation_id,
            json!({
                "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
                "error_code": session_error_code(error),
            }),
        );
    }
}

async fn run_listener(
    runtime: std::sync::Weak<SyncRuntime>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let stopping = changed.is_err() || *shutdown.borrow_and_update();
                if stopping {
                    break;
                }
                continue;
            }
            accepted = listener.accept() => accepted,
        };
        let Some(runtime) = runtime.upgrade() else {
            break;
        };
        match accepted {
            Ok((stream, _)) => {
                let connection_runtime = Arc::clone(&runtime);
                let correlation_id = new_correlation_id();
                runtime.spawn(async move {
                    run_connection(
                        connection_runtime,
                        stream,
                        Direction::Inbound,
                        None,
                        correlation_id,
                    )
                    .await;
                });
            }
            Err(error) => {
                let correlation_id = new_correlation_id();
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_listener_accept_failed",
                    &correlation_id,
                    json!({ "error_kind": format!("{:?}", error.kind()) }),
                );
            }
        }
    }
}

async fn run_outbound(
    runtime: std::sync::Weak<SyncRuntime>,
    target: ConnectUrl,
    mut shutdown: watch::Receiver<bool>,
) {
    let target_string = target.to_string();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let Some(runtime) = runtime.upgrade() else {
            break;
        };
        if *shutdown.borrow() {
            break;
        }
        let correlation_id = new_correlation_id();
        runtime
            .target_states
            .write()
            .await
            .insert(target_string.clone(), PeerConnectionState::Connecting);
        runtime.session_changed.notify_waiters();
        let connection = timeout_at(
            Instant::now() + CONNECT_DEADLINE,
            TcpStream::connect((target.host(), target.port())),
        )
        .await;
        match connection {
            Ok(Ok(stream)) => {
                backoff = INITIAL_BACKOFF;
                run_connection(
                    Arc::clone(&runtime),
                    stream,
                    Direction::Outbound,
                    Some(target_string.clone()),
                    correlation_id.clone(),
                )
                .await;
            }
            Ok(Err(error)) => {
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_connect_failed",
                    &correlation_id,
                    json!({
                        "connect_target": &target_string,
                        "error_kind": format!("{:?}", error.kind()),
                    }),
                );
            }
            Err(_) => {
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_connect_failed",
                    &correlation_id,
                    json!({
                        "connect_target": &target_string,
                        "error_kind": "timeout",
                    }),
                );
            }
        }
        if *shutdown.borrow() {
            break;
        }
        runtime
            .target_states
            .write()
            .await
            .insert(target_string.clone(), PeerConnectionState::Backoff);
        runtime.session_changed.notify_waiters();
        let delay = jittered(backoff);
        backoff = backoff.saturating_mul(2).min(MAXIMUM_BACKOFF);
        runtime.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_reconnect_scheduled",
            &correlation_id,
            json!({
                "connect_target": &target_string,
                "backoff_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            }),
        );
        tokio::select! {
            _ = sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
            }
        }
    }
}

async fn run_connection(
    runtime: Arc<SyncRuntime>,
    stream: TcpStream,
    direction: Direction,
    connect_target: Option<String>,
    correlation_id: String,
) {
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_started",
        &correlation_id,
        json!({
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
            "connect_target": connect_target.as_deref(),
        }),
    );
    let Some(psk) = runtime.psk.as_ref() else {
        return;
    };
    let _ = stream.set_nodelay(true);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let transport = match direction {
        Direction::Outbound => NoiseTransport::connect(stream, psk, deadline).await,
        Direction::Inbound => NoiseTransport::accept(stream, psk, deadline).await,
    };
    let transport = match transport {
        Ok(transport) => transport,
        Err(error) => {
            runtime.log_transport_failure(
                &correlation_id,
                direction,
                &SessionError::Transport(error),
            );
            return;
        }
    };
    let bound_epoch = runtime.identities.epoch();
    let local_identity = runtime.identities.node().await;
    let mut pending = match PendingSession::begin(
        transport,
        &local_identity,
        runtime.replica.status().await,
        &correlation_id,
        deadline,
    )
    .await
    {
        Ok(pending) => pending,
        Err(error) => {
            runtime.log_transport_failure(&correlation_id, direction, &error);
            return;
        }
    };
    if runtime.identities.epoch() != bound_epoch {
        pending
            .channel
            .close(
                SyncCloseCode::Normal,
                "local identity changed during handshake",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    let operation_correlation_id = pending
        .bootstrap_correlation_id
        .clone()
        .unwrap_or_else(|| correlation_id.clone());
    if let Err(error) = runtime
        .replica
        .bind_sync_peer(&pending.remote, connect_target.as_deref())
        .await
    {
        let code = if matches!(error, ReplicaError::RevisionConflict(_)) {
            SyncCloseCode::IdentityCollision
        } else {
            SyncCloseCode::InternalError
        };
        pending
            .channel
            .close(
                code,
                "remote identity binding was rejected",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    let mut bootstrap_claim = None;
    let mut bootstrap_guard = None;
    if pending.mode == SessionReplicaMode::BootstrapReceiver {
        let claim = BootstrapClaim {
            claim_id: Uuid::new_v4(),
            source_node_id: pending.remote.node_id(),
            correlation_id: operation_correlation_id.clone(),
        };
        match runtime.replica.acquire_bootstrap_claim(&claim).await {
            Ok(true) => {
                runtime.logger.emit(
                    LogLevel::Info,
                    "oll::sync",
                    "sync_bootstrap_claim_acquired",
                    &operation_correlation_id,
                    json!({
                        "source_node_id": pending.remote.node_id().to_string(),
                        "claim_id": claim.claim_id.to_string(),
                    }),
                );
            }
            Ok(false) => {
                let (code, message) = match runtime.replica.status().await {
                    ReplicaStatus::Uninitialized => (
                        SyncCloseCode::BootstrapInProgress,
                        "another authenticated source is bootstrapping this replica",
                    ),
                    ReplicaStatus::InitializedEmpty { replica_id }
                    | ReplicaStatus::InitializedPopulated { replica_id }
                        if replica_id == pending.replica_id =>
                    {
                        (
                            SyncCloseCode::Normal,
                            "replica became initialized; reconnect for normal sync",
                        )
                    }
                    _ => (
                        SyncCloseCode::ReplicaMismatch,
                        "local replica changed while bootstrap was negotiated",
                    ),
                };
                pending
                    .channel
                    .close(code, message, &operation_correlation_id, Some(deadline))
                    .await;
                return;
            }
            Err(_) => {
                pending
                    .channel
                    .close(
                        SyncCloseCode::InternalError,
                        "bootstrap claim could not be persisted",
                        &operation_correlation_id,
                        Some(deadline),
                    )
                    .await;
                return;
            }
        }
        let guard = match timeout_at(deadline, runtime.identities.commit_guard_owned()).await {
            Ok(guard) => guard,
            Err(_) => {
                let _ = runtime
                    .replica
                    .release_bootstrap_claim(claim.claim_id)
                    .await;
                pending
                    .channel
                    .close(
                        SyncCloseCode::InternalError,
                        "bootstrap could not pause local commits before the handshake deadline",
                        &operation_correlation_id,
                        Some(deadline),
                    )
                    .await;
                return;
            }
        };
        if !matches!(runtime.replica.status().await, ReplicaStatus::Uninitialized) {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
            pending
                .channel
                .close(
                    SyncCloseCode::Normal,
                    "replica became initialized; reconnect for normal sync",
                    &operation_correlation_id,
                    Some(deadline),
                )
                .await;
            return;
        }
        bootstrap_claim = Some(claim);
        bootstrap_guard = Some(guard);
    }
    if runtime.refresh_bindings().await.is_err() {
        if let Some(claim) = bootstrap_claim.as_ref() {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
        }
        pending
            .channel
            .close(
                SyncCloseCode::InternalError,
                "peer directory reload failed",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    if let Err(error) = pending.exchange_ready(&correlation_id, deadline).await {
        if let Some(claim) = bootstrap_claim.as_ref() {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
        }
        runtime.log_transport_failure(&correlation_id, direction, &error);
        return;
    }
    let remote = pending.remote.clone();
    let mode = pending.mode;
    let max_chunk_bytes = pending.max_chunk_bytes;
    let handshake_hash = *pending.channel.handshake_hash();
    if mode != SessionReplicaMode::Normal {
        run_bootstrap_session(
            &runtime,
            pending,
            bound_epoch,
            bootstrap_claim,
            bootstrap_guard,
            &operation_correlation_id,
        )
        .await;
        return;
    }
    let (commands_tx, commands_rx) = mpsc::channel(8);
    let (cancel_tx, cancel_rx) = watch::channel(None);
    let session_id = match runtime
        .register_session(
            remote.clone(),
            direction,
            connect_target.clone(),
            handshake_hash,
            commands_tx,
            cancel_tx,
        )
        .await
    {
        Ok(session_id) => session_id,
        Err(_) => {
            pending
                .channel
                .close(
                    SyncCloseCode::DuplicateSession,
                    "duplicate sync session lost arbitration",
                    &correlation_id,
                    None,
                )
                .await;
            return;
        }
    };
    if let Some(target) = connect_target.as_ref() {
        runtime
            .target_states
            .write()
            .await
            .insert(target.clone(), PeerConnectionState::Ready);
    }
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_ready",
        &correlation_id,
        json!({
            "connection_id": session_id.to_string(),
            "remote_node_id": remote.node_id().to_string(),
            "remote_node_name": remote.node_name().as_str(),
            "replica_id": pending.replica_id.to_string(),
            "connect_target": connect_target.as_deref(),
            "max_chunk_bytes": max_chunk_bytes,
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
        }),
    );
    run_ready_session(
        &runtime,
        pending.channel,
        bound_epoch,
        remote.node_id(),
        mode,
        max_chunk_bytes,
        commands_rx,
        cancel_rx,
    )
    .await;
    runtime.remove_session(remote.node_id(), session_id).await;
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_closed",
        &correlation_id,
        json!({
            "connection_id": session_id.to_string(),
            "remote_node_id": remote.node_id().to_string(),
            "remote_node_name": remote.node_name().as_str(),
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
        }),
    );
}

async fn run_bootstrap_session(
    runtime: &SyncRuntime,
    mut pending: PendingSession<TcpStream>,
    bound_epoch: u64,
    bootstrap_claim: Option<BootstrapClaim>,
    mut bootstrap_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    correlation_id: &str,
) {
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_bootstrap_started",
        correlation_id,
        json!({
            "source_node_id": match pending.mode {
                SessionReplicaMode::BootstrapSource => runtime.identities.node_id().await,
                SessionReplicaMode::BootstrapReceiver => pending.remote.node_id(),
                SessionReplicaMode::Normal => unreachable!("normal sessions use the ready-session loop"),
            }.to_string(),
            "replica_id": pending.replica_id.to_string(),
        }),
    );
    let mut shutdown = runtime.shutdown.subscribe();
    let mut epoch = runtime.identities.subscribe_epoch();
    // A receiver holds the identity commit gate for the whole transfer. Its own
    // successful activation advances this epoch before projection finishes;
    // external identity replacements cannot pass the gate until the guard drops.
    let watch_identity_changes_during_transfer =
        pending.mode == SessionReplicaMode::BootstrapSource;
    let mut cancellation = if *shutdown.borrow() {
        Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"))
    } else if *epoch.borrow() != bound_epoch {
        Some((
            SyncCloseCode::Normal,
            "local identity changed; reconnect required",
        ))
    } else {
        None
    };
    let mut work = Box::pin(async {
        match pending.mode {
            SessionReplicaMode::BootstrapSource => {
                match runtime.replica.capture_bootstrap_source().await {
                    Ok(source) if source.inventory.replica_id == pending.replica_id => {
                        send_bootstrap_round(
                            &mut pending.channel,
                            &runtime.replica,
                            &source,
                            correlation_id,
                            pending.max_chunk_bytes,
                        )
                        .await
                        .map_err(round_error_to_sync)
                    }
                    Ok(_) => Err(SyncError::Protocol(
                        "local ReplicaId changed after bootstrap negotiation".to_owned(),
                    )),
                    Err(error) => Err(round_error_to_sync(RoundError::Replica(error))),
                }
            }
            SessionReplicaMode::BootstrapReceiver => {
                let received = pending.channel.receive(None).await;
                match received {
                    Ok(envelope)
                        if envelope.correlation_id == correlation_id
                            && envelope.reply_to.is_none() =>
                    {
                        match envelope.payload {
                            Some(sync_envelope::Payload::RoundStart(start)) => {
                                let claim = bootstrap_claim.as_ref().expect(
                                    "bootstrap receiver acquired its claim before SyncReady",
                                );
                                let guard = bootstrap_guard.as_ref().expect(
                                    "bootstrap receiver acquired its commit guard before SyncReady",
                                );
                                let writer_node_id = runtime.identities.node_id().await;
                                receive_bootstrap_round(
                                    &mut pending.channel,
                                    &runtime.replica,
                                    envelope.message_id,
                                    start,
                                    correlation_id,
                                    pending.max_chunk_bytes,
                                    claim.claim_id,
                                    pending.replica_id,
                                    guard,
                                    writer_node_id,
                                )
                                .await
                                .map_err(round_error_to_sync)
                            }
                            _ => Err(SyncError::Protocol(
                                "bootstrap source did not begin a bootstrap round".to_owned(),
                            )),
                        }
                    }
                    Ok(_) => Err(SyncError::Protocol(
                        "bootstrap round metadata differs from its inherited correlation"
                            .to_owned(),
                    )),
                    Err(error) => Err(SyncError::Unavailable(error.to_string())),
                }
            }
            SessionReplicaMode::Normal => {
                unreachable!("normal sessions use the ready-session loop")
            }
        }
    });
    let result = if cancellation.is_some() {
        Err(SyncError::Unavailable(
            "bootstrap session was cancelled".to_owned(),
        ))
    } else {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                cancellation = Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"));
                Err(SyncError::Unavailable("bootstrap session was cancelled by shutdown".to_owned()))
            }
            _ = async {
                if watch_identity_changes_during_transfer {
                    let _ = epoch.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                cancellation = Some((SyncCloseCode::Normal, "local identity changed; reconnect required"));
                Err(SyncError::Unavailable("bootstrap session was cancelled by an identity change".to_owned()))
            }
            result = &mut work => result,
        }
    };
    drop(work);

    if let Some(claim) = bootstrap_claim
        && runtime
            .replica
            .release_bootstrap_claim(claim.claim_id)
            .await
            .is_err()
    {
        runtime.logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "sync_bootstrap_claim_release_failed",
            correlation_id,
            json!({ "claim_id": claim.claim_id.to_string() }),
        );
    }
    drop(bootstrap_guard.take());

    if let Some((code, message)) = cancellation {
        runtime.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_bootstrap_cancelled",
            correlation_id,
            json!({
                "reason": if code == SyncCloseCode::ShuttingDown {
                    "shutdown"
                } else {
                    "identity_changed"
                },
            }),
        );
        pending
            .channel
            .close(code, message, correlation_id, None)
            .await;
        return;
    }

    match result {
        Ok(result) => {
            runtime.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_bootstrap_completed",
                correlation_id,
                json!({
                    "replica_id": pending.replica_id.to_string(),
                    "object_count": result.object_count,
                    "blob_count": result.blob_count,
                    "bytes": result.transferred_bytes,
                }),
            );
            pending
                .channel
                .close(
                    SyncCloseCode::Normal,
                    "bootstrap completed; reconnect for normal sync",
                    correlation_id,
                    None,
                )
                .await;
        }
        Err(error) => {
            runtime.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_bootstrap_failed",
                correlation_id,
                json!({ "error_code": sync_error_name(&error) }),
            );
            let close_code = match error {
                SyncError::Protocol(_) => SyncCloseCode::ProtocolViolation,
                _ => SyncCloseCode::InternalError,
            };
            pending
                .channel
                .close(
                    close_code,
                    "bootstrap did not complete",
                    correlation_id,
                    None,
                )
                .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ready_session(
    runtime: &SyncRuntime,
    mut channel: SessionChannel<TcpStream>,
    bound_epoch: u64,
    remote_node_id: Uuid,
    mode: SessionReplicaMode,
    max_chunk_bytes: u32,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut cancel: watch::Receiver<Option<SyncCloseCode>>,
) {
    if mode != SessionReplicaMode::Normal {
        let correlation_id = new_correlation_id();
        channel
            .close(
                SyncCloseCode::InternalError,
                "bootstrap session is not ready for transfer",
                &correlation_id,
                None,
            )
            .await;
        return;
    }
    let mut shutdown = runtime.shutdown.subscribe();
    let mut epoch = runtime.identities.subscribe_epoch();
    let mut pings = HashMap::<u64, PendingPing>::new();
    loop {
        let immediate_close = if *shutdown.borrow() {
            Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"))
        } else if let Some(code) = *cancel.borrow() {
            Some((code, "sync session was superseded"))
        } else if *epoch.borrow() != bound_epoch {
            Some((
                SyncCloseCode::Normal,
                "local identity changed; reconnect required",
            ))
        } else {
            None
        };
        if let Some((code, message)) = immediate_close {
            let correlation_id = new_correlation_id();
            channel.close(code, message, &correlation_id, None).await;
            break;
        }
        let next_ping_deadline = pings.values().map(|ping| ping.deadline).min();
        let ping_timeout = async move {
            match next_ping_deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let stopping = changed.is_err() || *shutdown.borrow_and_update();
                if stopping {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::ShuttingDown, "local daemon is shutting down", &correlation_id, None).await;
                    break;
                }
            }
            changed = cancel.changed() => {
                if changed.is_err() {
                    break;
                }
                let code = *cancel.borrow_and_update();
                if let Some(code) = code {
                    let correlation_id = new_correlation_id();
                    channel.close(code, "sync session was superseded", &correlation_id, None).await;
                    break;
                }
            }
            changed = epoch.changed() => {
                let changed_identity = changed.is_err() || *epoch.borrow_and_update() != bound_epoch;
                if changed_identity {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::Normal, "local identity changed; reconnect required", &correlation_id, None).await;
                    break;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    SessionCommand::Ping { correlation_id, response } => {
                        let mut nonce_bytes = [0_u8; 8];
                        if fill_random(&mut nonce_bytes).is_err() {
                            let _ = response.send(Err(SyncError::Internal("cannot generate sync ping nonce".to_owned())));
                            continue;
                        }
                        let nonce = u64::from_be_bytes(nonce_bytes);
                        let message_id = match channel.send(
                            sync_envelope::Payload::Ping(SyncPing {
                                nonce,
                                sent_at: Some(system_timestamp()),
                            }),
                            &correlation_id,
                            None,
                            None,
                        ).await {
                            Ok(message_id) => message_id,
                            Err(error) => {
                                let _ = response.send(Err(SyncError::Protocol(error.to_string())));
                                break;
                            }
                        };
                        if let Some(replaced) = pings.insert(nonce, PendingPing {
                            sent_message_id: message_id,
                            started: Instant::now(),
                            deadline: Instant::now() + PING_RESPONSE_DEADLINE,
                            response,
                        }) {
                            let _ = replaced.response.send(Err(SyncError::Internal("sync ping nonce collision".to_owned())));
                        }
                    }
                    SessionCommand::Synchronize { correlation_id, response } => {
                        if !pings.is_empty() {
                            let _ = response.send(Err(SyncError::Unavailable(
                                "another sync request is in flight on this session".to_owned(),
                            )));
                            continue;
                        }
                        let local_node_id = runtime.identities.node_id().await;
                        let mut round = Box::pin(request_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            local_node_id,
                            remote_node_id,
                            &correlation_id,
                            max_chunk_bytes,
                        ));
                        let (result, close) = tokio::select! {
                            biased;
                            _ = shutdown.changed() => (
                                None,
                                Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down")),
                            ),
                            changed = cancel.changed() => {
                                let code = if changed.is_err() {
                                    SyncCloseCode::Normal
                                } else {
                                    (*cancel.borrow_and_update()).unwrap_or(SyncCloseCode::Normal)
                                };
                                (None, Some((code, "sync session was superseded")))
                            }
                            _ = epoch.changed() => (
                                None,
                                Some((SyncCloseCode::Normal, "local identity changed; reconnect required")),
                            ),
                            result = &mut round => (Some(result), None),
                        };
                        drop(round);
                        if let Some((code, message)) = close {
                            let _ = response.send(Err(SyncError::Unavailable(message.to_owned())));
                            channel.close(code, message, &correlation_id, None).await;
                            break;
                        }
                        let result = result.expect("a completed round returns its result");
                        let protocol_failure = matches!(
                            result,
                            Err(SyncError::Protocol(_)) | Err(SyncError::Internal(_))
                        );
                        let _ = response.send(result);
                        if protocol_failure {
                            break;
                        }
                    }
                }
            }
            _ = ping_timeout => {
                let now = Instant::now();
                let expired = pings
                    .iter()
                    .filter_map(|(nonce, pending)| (pending.deadline <= now).then_some(*nonce))
                    .collect::<Vec<_>>();
                for nonce in expired {
                    if let Some(pending) = pings.remove(&nonce) {
                        let _ = pending.response.send(Err(SyncError::Unavailable(
                            "sync ping timed out".to_owned(),
                        )));
                    }
                }
            }
            received = channel.receive(None) => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(SessionError::RemoteClosed { .. }) => break,
                    Err(_) => {
                        let correlation_id = new_correlation_id();
                        channel.close(SyncCloseCode::ProtocolViolation, "invalid ready-session message", &correlation_id, None).await;
                        break;
                    }
                };
                match envelope.payload {
                    Some(sync_envelope::Payload::Ping(ping)) => {
                        if channel.send(
                            sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                            &envelope.correlation_id,
                            Some(envelope.message_id),
                            None,
                        ).await.is_err() {
                            break;
                        }
                    }
                    Some(sync_envelope::Payload::Pong(pong)) => {
                        let Some(pending) = pings.remove(&pong.nonce) else {
                            channel.close(SyncCloseCode::ProtocolViolation, "received an unknown SyncPong", &envelope.correlation_id, None).await;
                            break;
                        };
                        if envelope.reply_to != Some(pending.sent_message_id) {
                            let _ = pending.response.send(Err(SyncError::Protocol("SyncPong reply_to does not name its request".to_owned())));
                            channel.close(SyncCloseCode::ProtocolViolation, "SyncPong reply_to is invalid", &envelope.correlation_id, None).await;
                            break;
                        }
                        let _ = pending.response.send(Ok(pending.started.elapsed()));
                    }
                    Some(sync_envelope::Payload::RoundRequest(_)) => {
                        if !pings.is_empty() || envelope.reply_to.is_some() {
                            channel.close(SyncCloseCode::ProtocolViolation, "unexpected sync round request", &envelope.correlation_id, None).await;
                            break;
                        }
                        let mut round = Box::pin(source_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            &envelope.correlation_id,
                            envelope.message_id,
                            max_chunk_bytes,
                        ));
                        let (result, close) = tokio::select! {
                            biased;
                            _ = shutdown.changed() => (
                                None,
                                Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down")),
                            ),
                            changed = cancel.changed() => {
                                let code = if changed.is_err() {
                                    SyncCloseCode::Normal
                                } else {
                                    (*cancel.borrow_and_update()).unwrap_or(SyncCloseCode::Normal)
                                };
                                (None, Some((code, "sync session was superseded")))
                            }
                            _ = epoch.changed() => (
                                None,
                                Some((SyncCloseCode::Normal, "local identity changed; reconnect required")),
                            ),
                            result = &mut round => (Some(result), None),
                        };
                        drop(round);
                        if let Some((code, message)) = close {
                            channel.close(code, message, &envelope.correlation_id, None).await;
                            break;
                        }
                        if result
                            .expect("a completed reverse round returns its result")
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ => {
                        channel.close(SyncCloseCode::ProtocolViolation, "message is not valid in a ready idle session", &envelope.correlation_id, None).await;
                        break;
                    }
                }
            }
        }
    }
    for (_, pending) in pings {
        let _ = pending.response.send(Err(SyncError::Unavailable(
            "sync session closed during ping".to_owned(),
        )));
    }
}

async fn request_bidirectional_round(
    channel: &mut SessionChannel<TcpStream>,
    replica: &ReplicaRuntime,
    local_node_id: Uuid,
    remote_node_id: Uuid,
    correlation_id: &str,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let request_message_id = channel
        .send(
            sync_envelope::Payload::RoundRequest(SyncRoundRequest {}),
            correlation_id,
            None,
            None,
        )
        .await
        .map_err(|error| SyncError::Unavailable(error.to_string()))?;
    loop {
        let envelope = channel.receive(None).await.map_err(|error| {
            SyncError::Protocol(format!("cannot receive requested sync round: {error}"))
        })?;
        match envelope.payload {
            Some(sync_envelope::Payload::RoundStart(start)) => {
                if envelope.reply_to != Some(request_message_id)
                    || envelope.correlation_id != correlation_id
                {
                    return Err(SyncError::Protocol(
                        "requested sync round does not name its request".to_owned(),
                    ));
                }
                let received = receive_round(
                    channel,
                    replica,
                    envelope.message_id,
                    start,
                    correlation_id,
                    max_chunk_bytes,
                )
                .await
                .map_err(round_error_to_sync)?;
                let (_, sent) = send_round(
                    channel,
                    replica,
                    correlation_id,
                    Some(envelope.message_id),
                    max_chunk_bytes,
                )
                .await
                .map_err(round_error_to_sync)?;
                return Ok(received.combine(sent));
            }
            Some(sync_envelope::Payload::RoundRequest(_)) => {
                if envelope.reply_to.is_some() {
                    return Err(SyncError::Protocol(
                        "sync round request must not reply to another message".to_owned(),
                    ));
                }
                if local_node_id < remote_node_id {
                    continue;
                }
                return source_bidirectional_round(
                    channel,
                    replica,
                    &envelope.correlation_id,
                    envelope.message_id,
                    max_chunk_bytes,
                )
                .await;
            }
            Some(sync_envelope::Payload::Ping(ping)) => {
                channel
                    .send(
                        sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                        &envelope.correlation_id,
                        Some(envelope.message_id),
                        None,
                    )
                    .await
                    .map_err(|error| SyncError::Unavailable(error.to_string()))?;
            }
            _ => {
                return Err(SyncError::Protocol(
                    "message is invalid while waiting for a requested sync round".to_owned(),
                ));
            }
        }
    }
}

async fn source_bidirectional_round(
    channel: &mut SessionChannel<TcpStream>,
    replica: &ReplicaRuntime,
    correlation_id: &str,
    request_message_id: u64,
    max_chunk_bytes: u32,
) -> Result<RoundResult, SyncError> {
    let (start_message_id, sent) = send_round(
        channel,
        replica,
        correlation_id,
        Some(request_message_id),
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    let envelope = channel.receive(None).await.map_err(|error| {
        SyncError::Protocol(format!("cannot receive reverse sync round: {error}"))
    })?;
    let Some(sync_envelope::Payload::RoundStart(start)) = envelope.payload else {
        return Err(SyncError::Protocol(
            "peer did not begin the reverse sync round".to_owned(),
        ));
    };
    if envelope.reply_to != Some(start_message_id) || envelope.correlation_id != correlation_id {
        return Err(SyncError::Protocol(
            "reverse sync round does not match its initiating round".to_owned(),
        ));
    }
    let received = receive_round(
        channel,
        replica,
        envelope.message_id,
        start,
        correlation_id,
        max_chunk_bytes,
    )
    .await
    .map_err(round_error_to_sync)?;
    Ok(sent.combine(received))
}

fn round_error_to_sync(error: RoundError) -> SyncError {
    match error {
        RoundError::Session(error) => SyncError::Unavailable(error.to_string()),
        RoundError::Replica(ReplicaError::RevisionConflict(message)) => {
            SyncError::Unavailable(message)
        }
        RoundError::Replica(ReplicaError::Uninitialized) => {
            SyncError::FailedPrecondition("no local replica yet".to_owned())
        }
        RoundError::Replica(_) => SyncError::Store,
        RoundError::Protocol(message) => SyncError::Protocol(message.to_owned()),
        RoundError::Rejected(message) => SyncError::Unavailable(message),
    }
}

fn sync_error_code(error: &SyncError) -> ErrorCode {
    match error {
        SyncError::NotFound(_) => ErrorCode::NotFound,
        SyncError::FailedPrecondition(_) => ErrorCode::FailedPrecondition,
        SyncError::Unavailable(_) => ErrorCode::Unavailable,
        SyncError::Protocol(_) => ErrorCode::ProtocolMismatch,
        SyncError::Store | SyncError::Internal(_) => ErrorCode::Internal,
    }
}

fn sync_error_name(error: &SyncError) -> &'static str {
    match sync_error_code(error) {
        ErrorCode::NotFound => "not_found",
        ErrorCode::FailedPrecondition => "failed_precondition",
        ErrorCode::Unavailable => "unavailable",
        ErrorCode::ProtocolMismatch => "protocol",
        _ => "internal",
    }
}

fn system_timestamp() -> prost_types::Timestamp {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        },
    }
}

fn jittered(base: Duration) -> Duration {
    let mut bytes = [0_u8; 4];
    if fill_random(&mut bytes).is_err() {
        return base;
    }
    let upper_millis = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
    if upper_millis == 0 {
        return base;
    }
    base.saturating_add(Duration::from_millis(
        u64::from(u32::from_be_bytes(bytes)) % upper_millis,
    ))
}

fn session_error_code(error: &SessionError) -> &'static str {
    match error {
        SessionError::Transport(_) => "transport",
        SessionError::LocalProtocol { .. } => "protocol",
        SessionError::RemoteClosed { .. } => "remote_close",
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, net::SocketAddr, path::PathBuf, str::FromStr};

    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::{
        configuration::{NetworkKey, ReplicaStoreConfig},
        node::{NodeIdentity, logging::NodeLogger},
        protocol::oll::{
            CommitDocumentsRequest, CreateDocument, DeleteNode, DocumentMutation, DocumentPath,
            DocumentProjection, MoveNode, ReadDocumentRequest, ReplaceDocument, document_mutation,
        },
        replica::OperationSource,
    };

    use super::*;

    struct SyncDeployment {
        _directory: TempDir,
        root: PathBuf,
        config_root: PathBuf,
        store: ReplicaStoreConfig,
        log_dir: PathBuf,
        identity: NodeIdentity,
        identities: Arc<IdentityCoordinator>,
    }

    impl SyncDeployment {
        fn new(name: &str) -> Self {
            let directory = TempDir::new().unwrap();
            let root = directory.path().join("working");
            let config_root = directory.path().join("config");
            let log_dir = directory.path().join("logs");
            fs::create_dir(&root).unwrap();
            fs::create_dir(&config_root).unwrap();
            let identity = NodeIdentity::generate(name.parse().unwrap());
            Self {
                store: ReplicaStoreConfig::Sqlite {
                    path: directory.path().join("store/replica.sqlite3"),
                },
                identities: IdentityCoordinator::new(identity.clone()),
                identity,
                _directory: directory,
                root,
                config_root,
                log_dir,
            }
        }

        async fn start_replica(&self) -> (Arc<ReplicaRuntime>, Arc<NodeLogger>) {
            let logger = NodeLogger::open(&self.log_dir, self.identity.clone()).unwrap();
            let replica = ReplicaRuntime::start(
                self.config_root.clone(),
                self.root.clone(),
                &self.store,
                Arc::clone(&self.identities),
                Arc::clone(&logger),
            )
            .await
            .unwrap();
            (replica, logger)
        }

        fn sync_config(
            &self,
            listen: Option<SocketAddr>,
            connect: Vec<ConnectUrl>,
        ) -> ResolvedNodeConfig {
            ResolvedNodeConfig {
                replica_root: self.root.clone(),
                replica_store: self.store.clone(),
                log_dir: self.log_dir.clone(),
                listen,
                connect,
                network_key: Some(NetworkKey::new_for_test(vec![7; 32])),
            }
        }
    }

    fn unused_loopback_address() -> SocketAddr {
        std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
    }

    async fn read_text(replica: &ReplicaRuntime, path: &str) -> String {
        let response = replica
            .read_document(ReadDocumentRequest {
                path: Some(DocumentPath {
                    value: path.to_owned(),
                }),
                projection: DocumentProjection::Content as i32,
            })
            .await
            .unwrap();
        match response.document.unwrap().representation.unwrap() {
            crate::protocol::oll::document_snapshot::Representation::Content(content) => content,
            _ => panic!("expected content projection"),
        }
    }

    async fn create_text(replica: &ReplicaRuntime, operation_id: &str, path: &str, text: &str) {
        replica
            .commit_documents(
                CommitDocumentsRequest {
                    operation_id: operation_id.to_owned(),
                    preconditions: Vec::new(),
                    mutations: vec![DocumentMutation {
                        mutation: Some(document_mutation::Mutation::CreateDocument(
                            CreateDocument {
                                path: Some(DocumentPath {
                                    value: path.to_owned(),
                                }),
                                media_type: "text/plain".to_owned(),
                                content: text.to_owned(),
                            },
                        )),
                    }],
                },
                OperationSource::Plugin,
                operation_id,
            )
            .await
            .unwrap();
    }

    async fn replace_text(replica: &ReplicaRuntime, operation_id: &str, path: &str, text: &str) {
        replica
            .commit_documents(
                CommitDocumentsRequest {
                    operation_id: operation_id.to_owned(),
                    preconditions: Vec::new(),
                    mutations: vec![DocumentMutation {
                        mutation: Some(document_mutation::Mutation::ReplaceDocument(
                            ReplaceDocument {
                                path: Some(DocumentPath {
                                    value: path.to_owned(),
                                }),
                                content: text.to_owned(),
                                media_type: None,
                            },
                        )),
                    }],
                },
                OperationSource::Plugin,
                operation_id,
            )
            .await
            .unwrap();
    }

    async fn move_node(
        replica: &ReplicaRuntime,
        operation_id: &str,
        source: &str,
        destination: &str,
    ) {
        replica
            .commit_documents(
                CommitDocumentsRequest {
                    operation_id: operation_id.to_owned(),
                    preconditions: Vec::new(),
                    mutations: vec![DocumentMutation {
                        mutation: Some(document_mutation::Mutation::MoveNode(MoveNode {
                            source: Some(DocumentPath {
                                value: source.to_owned(),
                            }),
                            destination: Some(DocumentPath {
                                value: destination.to_owned(),
                            }),
                        })),
                    }],
                },
                OperationSource::Plugin,
                operation_id,
            )
            .await
            .unwrap();
    }

    async fn delete_node(replica: &ReplicaRuntime, operation_id: &str, path: &str) {
        replica
            .commit_documents(
                CommitDocumentsRequest {
                    operation_id: operation_id.to_owned(),
                    preconditions: Vec::new(),
                    mutations: vec![DocumentMutation {
                        mutation: Some(document_mutation::Mutation::DeleteNode(DeleteNode {
                            path: Some(DocumentPath {
                                value: path.to_owned(),
                            }),
                            recursive: false,
                        })),
                    }],
                },
                OperationSource::Plugin,
                operation_id,
            )
            .await
            .unwrap();
    }

    #[test]
    fn duplicate_arbitration_rank_prefers_the_canonical_initiator_then_hash() {
        let preferred = (false, [9_u8; 32]);
        let nonpreferred = (true, [0_u8; 32]);
        assert!(preferred < nonpreferred);
        assert!((false, [1_u8; 32]) < (false, [2_u8; 32]));
    }

    #[test]
    fn reconnect_backoff_is_bounded_even_after_saturation() {
        let mut backoff = INITIAL_BACKOFF;
        for _ in 0..100 {
            backoff = backoff.saturating_mul(2).min(MAXIMUM_BACKOFF);
        }
        assert_eq!(backoff, MAXIMUM_BACKOFF);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_aborts_a_silent_handshake_at_the_supplied_absolute_deadline() {
        let deployment = SyncDeployment::new("shutdown-listener");
        fs::write(deployment.root.join("ready.md"), "ready").unwrap();
        let (replica, logger) = deployment.start_replica().await;
        let listen = unused_loopback_address();
        let sync = SyncRuntime::start(
            &deployment.sync_config(Some(listen), Vec::new()),
            Arc::clone(&deployment.identities),
            Arc::clone(&replica),
            logger,
        )
        .await
        .unwrap();

        let mut silent = TcpStream::connect(listen).await.unwrap();
        silent.write_all(b"OLLSYNC\x01\x00\x20").await.unwrap();
        sleep(Duration::from_millis(25)).await;
        let started = Instant::now();
        let deadline = started + Duration::from_millis(100);
        sync.shutdown(deadline).await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        let mut byte = [0_u8; 1];
        let closed = tokio::time::timeout(Duration::from_millis(500), silent.read(&mut byte))
            .await
            .expect("shutdown did not close the in-progress handshake");
        assert!(matches!(closed, Ok(0) | Err(_)));

        replica
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn identity_epoch_change_closes_an_existing_ready_session() {
        let client = SyncDeployment::new("identity-change-client");
        fs::write(client.root.join("ready.md"), "ready").unwrap();
        let (client_replica, client_logger) = client.start_replica().await;
        let replica_id = match client_replica.status().await {
            ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
            state => panic!("unexpected client state: {state:?}"),
        };
        let server_identity = NodeIdentity::generate("identity-change-server".parse().unwrap());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let listen = listener.local_addr().unwrap();
        let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
        let server = tokio::spawn({
            let server_identity = server_identity.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let deadline = Instant::now() + HANDSHAKE_DEADLINE;
                let transport = NoiseTransport::accept(stream, &server_key, deadline)
                    .await
                    .unwrap();
                let mut session = PendingSession::begin(
                    transport,
                    &server_identity,
                    ReplicaStatus::InitializedPopulated { replica_id },
                    "identity-change-server-handshake",
                    deadline,
                )
                .await
                .unwrap();
                session
                    .exchange_ready("identity-change-server-handshake", deadline)
                    .await
                    .unwrap();
                assert!(matches!(
                    session.channel.receive(None).await,
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::Normal,
                        ..
                    })
                ));
            }
        });
        let client_sync = SyncRuntime::start(
            &client.sync_config(
                None,
                vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
            ),
            Arc::clone(&client.identities),
            Arc::clone(&client_replica),
            client_logger,
        )
        .await
        .unwrap();
        let ready_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if client_sync
                .status()
                .await
                .iter()
                .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            {
                break;
            }
            assert!(
                Instant::now() < ready_deadline,
                "sync session was not ready"
            );
            sleep(Duration::from_millis(10)).await;
        }

        client.identities.advance_epoch().unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("identity change did not close the ready session")
            .unwrap();
        let removed_deadline = Instant::now() + Duration::from_secs(2);
        while !client_sync.sessions.lock().await.is_empty() {
            assert!(
                Instant::now() < removed_deadline,
                "identity-invalidated session remained registered"
            );
            sleep(Duration::from_millis(10)).await;
        }

        client_sync
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        client_replica
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_cancels_an_inflight_round_and_unregisters_its_ready_session() {
        let client = SyncDeployment::new("shutdown-round-client");
        fs::write(client.root.join("ready.md"), "ready").unwrap();
        let (client_replica, client_logger) = client.start_replica().await;
        let replica_id = match client_replica.status().await {
            ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
            state => panic!("unexpected client state: {state:?}"),
        };
        let server_identity = NodeIdentity::generate("shutdown-round-server".parse().unwrap());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let listen = listener.local_addr().unwrap();
        let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
        let (round_started, round_started_rx) = oneshot::channel();
        let server = tokio::spawn({
            let server_identity = server_identity.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let deadline = Instant::now() + HANDSHAKE_DEADLINE;
                let transport = NoiseTransport::accept(stream, &server_key, deadline)
                    .await
                    .unwrap();
                let mut session = PendingSession::begin(
                    transport,
                    &server_identity,
                    ReplicaStatus::InitializedPopulated { replica_id },
                    "shutdown-server-handshake",
                    deadline,
                )
                .await
                .unwrap();
                session
                    .exchange_ready("shutdown-server-handshake", deadline)
                    .await
                    .unwrap();
                let request = session.channel.receive(None).await.unwrap();
                assert!(matches!(
                    request.payload,
                    Some(sync_envelope::Payload::RoundRequest(_))
                ));
                round_started.send(()).unwrap();
                assert!(matches!(
                    session.channel.receive(None).await,
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::ShuttingDown,
                        ..
                    })
                ));
            }
        });
        let client_sync = SyncRuntime::start(
            &client.sync_config(
                None,
                vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
            ),
            Arc::clone(&client.identities),
            Arc::clone(&client_replica),
            client_logger,
        )
        .await
        .unwrap();
        let ready_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if client_sync
                .status()
                .await
                .iter()
                .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            {
                break;
            }
            assert!(
                Instant::now() < ready_deadline,
                "sync session was not ready"
            );
            sleep(Duration::from_millis(10)).await;
        }
        let round_runtime = Arc::clone(&client_sync);
        let server_name = server_identity.node_name().clone();
        let round = tokio::spawn(async move {
            round_runtime
                .synchronize(Some(&server_name), 1, "shutdown-round-correlation")
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(2), round_started_rx)
            .await
            .unwrap()
            .unwrap();

        let started = Instant::now();
        client_sync
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(client_sync.sessions.lock().await.is_empty());
        let result = round.await.unwrap();
        assert_eq!(result[0].outcome, PeerSyncOutcome::Failed as i32);
        server.await.unwrap();

        client_replica
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_cancels_an_inflight_bootstrap_before_its_absolute_deadline() {
        let source = SyncDeployment::new("shutdown-bootstrap-source");
        fs::write(source.root.join("ready.md"), "ready").unwrap();
        let (source_replica, source_logger) = source.start_replica().await;
        let replica_id = match source_replica.status().await {
            ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
            state => panic!("unexpected source state: {state:?}"),
        };
        let receiver_identity =
            NodeIdentity::generate("shutdown-bootstrap-receiver".parse().unwrap());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let listen = listener.local_addr().unwrap();
        let receiver_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
        let (inventory_received, inventory_received_rx) = oneshot::channel();
        let receiver = tokio::spawn({
            let receiver_identity = receiver_identity.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let deadline = Instant::now() + HANDSHAKE_DEADLINE;
                let transport = NoiseTransport::accept(stream, &receiver_key, deadline)
                    .await
                    .unwrap();
                let mut session = PendingSession::begin(
                    transport,
                    &receiver_identity,
                    ReplicaStatus::Uninitialized,
                    "shutdown-bootstrap-receiver-handshake",
                    deadline,
                )
                .await
                .unwrap();
                assert_eq!(session.replica_id, replica_id);
                session
                    .exchange_ready("shutdown-bootstrap-receiver-handshake", deadline)
                    .await
                    .unwrap();
                loop {
                    let envelope = session.channel.receive(None).await.unwrap();
                    if matches!(
                        envelope.payload,
                        Some(sync_envelope::Payload::RoundInventoryComplete(_))
                    ) {
                        break;
                    }
                }
                inventory_received.send(()).unwrap();
                assert!(matches!(
                    session.channel.receive(None).await,
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::ShuttingDown,
                        ..
                    })
                ));
            }
        });
        let source_sync = SyncRuntime::start(
            &source.sync_config(
                None,
                vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
            ),
            Arc::clone(&source.identities),
            Arc::clone(&source_replica),
            source_logger,
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), inventory_received_rx)
            .await
            .unwrap()
            .unwrap();

        let started = Instant::now();
        source_sync
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        receiver.await.unwrap();

        source_replica
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unanswered_ping_expires_without_poisoning_the_session() {
        let client = SyncDeployment::new("ping-client");
        fs::write(client.root.join("shared.md"), "shared").unwrap();
        let (client_replica, client_logger) = client.start_replica().await;
        let replica_id = match client_replica.status().await {
            ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
            state => panic!("unexpected client state: {state:?}"),
        };
        let server_identity = NodeIdentity::generate("ping-server".parse().unwrap());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let listen = listener.local_addr().unwrap();
        let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
        let server = tokio::spawn({
            let server_identity = server_identity.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let deadline = Instant::now() + HANDSHAKE_DEADLINE;
                let transport = NoiseTransport::accept(stream, &server_key, deadline)
                    .await
                    .unwrap();
                let mut session = PendingSession::begin(
                    transport,
                    &server_identity,
                    ReplicaStatus::InitializedPopulated { replica_id },
                    "ping-server-handshake",
                    deadline,
                )
                .await
                .unwrap();
                session
                    .exchange_ready("ping-server-handshake", deadline)
                    .await
                    .unwrap();

                let first = session.channel.receive(None).await.unwrap();
                assert!(matches!(
                    first.payload,
                    Some(sync_envelope::Payload::Ping(_))
                ));
                let second = session.channel.receive(None).await.unwrap();
                let Some(sync_envelope::Payload::Ping(ping)) = second.payload else {
                    panic!("expected the second ping after the first timed out");
                };
                session
                    .channel
                    .send(
                        sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                        &second.correlation_id,
                        Some(second.message_id),
                        None,
                    )
                    .await
                    .unwrap();
            }
        });
        let client_sync = SyncRuntime::start(
            &client.sync_config(
                None,
                vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
            ),
            Arc::clone(&client.identities),
            Arc::clone(&client_replica),
            client_logger,
        )
        .await
        .unwrap();
        let ready_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if client_sync.status().await.iter().any(|peer| {
                peer.connection_state == PeerConnectionState::Ready as i32
                    && peer
                        .node
                        .as_ref()
                        .and_then(|node| node.node_name.as_ref())
                        .is_some_and(|name| name.value == "ping-server")
            }) {
                break;
            }
            assert!(
                Instant::now() < ready_deadline,
                "ping session was not ready"
            );
            sleep(Duration::from_millis(10)).await;
        }
        let server_name = server_identity.node_name().clone();
        assert!(matches!(
            client_sync
                .ping(&server_name, "unanswered-ping-correlation")
                .await,
            Err(SyncError::Unavailable(message)) if message == "sync ping timed out"
        ));
        client_sync
            .ping(&server_name, "answered-ping-correlation")
            .await
            .unwrap();
        server.await.unwrap();
        client_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        let events = fs::read_to_string(client.log_dir.join("sync.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(events.iter().any(|event| {
            event["event"] == "sync_ping_failed"
                && event["correlation_id"] == "unanswered-ping-correlation"
        }));
        assert!(events.iter().any(|event| {
            event["event"] == "sync_ping_completed"
                && event["correlation_id"] == "answered-ping-correlation"
        }));

        let shutdown_deadline = Instant::now() + Duration::from_secs(2);
        client_sync.shutdown(shutdown_deadline).await.unwrap();
        client_replica.shutdown(shutdown_deadline).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn noise_session_bootstraps_an_uninitialized_receiver_and_reconnects_normally() {
        let source = SyncDeployment::new("sync-source");
        fs::write(source.root.join("source.md"), "from source").unwrap();
        let source_binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff";
        fs::write(source.root.join("source.gif"), source_binary).unwrap();
        let (source_replica, source_logger) = source.start_replica().await;
        let source_replica_id = match source_replica.status().await {
            ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
            state => panic!("unexpected source state: {state:?}"),
        };
        let receiver = SyncDeployment::new("sync-receiver");
        let (receiver_replica, receiver_logger) = receiver.start_replica().await;
        assert_eq!(
            receiver_replica.status().await,
            ReplicaStatus::Uninitialized
        );

        let listen = unused_loopback_address();
        let source_sync = SyncRuntime::start(
            &source.sync_config(Some(listen), Vec::new()),
            Arc::clone(&source.identities),
            Arc::clone(&source_replica),
            source_logger,
        )
        .await
        .unwrap();
        let receiver_sync = SyncRuntime::start(
            &receiver.sync_config(
                None,
                vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
            ),
            Arc::clone(&receiver.identities),
            Arc::clone(&receiver_replica),
            receiver_logger,
        )
        .await
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if receiver_replica.status().await
                == (ReplicaStatus::InitializedPopulated {
                    replica_id: source_replica_id,
                })
                && receiver_sync
                    .status()
                    .await
                    .iter()
                    .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "bootstrap did not complete and reconnect"
            );
            sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            read_text(&receiver_replica, "/source.md").await,
            "from source"
        );
        assert_eq!(
            fs::read(receiver.root.join("source.gif")).unwrap(),
            source_binary
        );
        source_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        receiver_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        let source_events = fs::read_to_string(source.log_dir.join("sync.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let receiver_events = fs::read_to_string(receiver.log_dir.join("sync.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let bootstrap_correlation = source_events
            .iter()
            .find(|event| event["event"] == "sync_bootstrap_started")
            .and_then(|event| event["correlation_id"].as_str())
            .unwrap();
        assert!(receiver_events.iter().any(|event| {
            event["event"] == "sync_bootstrap_started"
                && event["correlation_id"] == bootstrap_correlation
        }));
        assert!(source_events.iter().any(|event| {
            event["event"] == "sync_replica_transfer_completed"
                && event["correlation_id"] == bootstrap_correlation
        }));
        assert!(receiver_events.iter().any(|event| {
            event["event"] == "sync_candidate_committed"
                && event["correlation_id"] == bootstrap_correlation
        }));

        let shutdown_deadline = Instant::now() + Duration::from_secs(5);
        receiver_sync.shutdown(shutdown_deadline).await.unwrap();
        source_sync.shutdown(shutdown_deadline).await.unwrap();
        receiver_replica.shutdown(shutdown_deadline).await.unwrap();
        source_replica.shutdown(shutdown_deadline).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn finite_bidirectional_round_converges_offline_catalog_changes() {
        let first = SyncDeployment::new("sync-first");
        fs::create_dir(first.root.join("folder")).unwrap();
        fs::write(first.root.join("shared.md"), "base").unwrap();
        fs::write(first.root.join("move.md"), "move me").unwrap();
        fs::write(first.root.join("rename.md"), "rename me").unwrap();
        fs::write(first.root.join("delete.md"), "delete me").unwrap();
        let (first_replica, first_logger) = first.start_replica().await;
        let snapshot = first._directory.path().join("seed.ollsnap");
        first_replica
            .export_snapshot(&snapshot, "sync-test-snapshot")
            .await
            .unwrap();

        let second = SyncDeployment::new("sync-second");
        let (second_replica, second_logger) = second.start_replica().await;
        second_replica
            .import_snapshot(&snapshot, "sync-test-import")
            .await
            .unwrap();
        create_text(&first_replica, "first-create", "/first.md", "first").await;
        create_text(&second_replica, "second-create", "/second.md", "second").await;
        replace_text(
            &first_replica,
            "first-offline-edit",
            "/shared.md",
            "first offline edit",
        )
        .await;
        replace_text(
            &second_replica,
            "second-offline-edit",
            "/shared.md",
            "second offline edit",
        )
        .await;
        move_node(
            &first_replica,
            "first-offline-move",
            "/move.md",
            "/folder/move.md",
        )
        .await;
        move_node(
            &second_replica,
            "second-offline-rename",
            "/rename.md",
            "/renamed.md",
        )
        .await;
        delete_node(&second_replica, "second-offline-delete", "/delete.md").await;
        let mut binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
        binary.resize(200_000, 0x5a);
        fs::write(first.root.join("image.gif"), &binary).unwrap();
        let binary_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let present = first_replica
                .state
                .read()
                .await
                .as_ref()
                .and_then(|replica| replica.entry_at_path("/image.gif").ok().flatten())
                .is_some();
            if present {
                break;
            }
            assert!(
                Instant::now() < binary_deadline,
                "binary was not reconciled"
            );
            sleep(Duration::from_millis(25)).await;
        }

        let first_listen = unused_loopback_address();
        let second_listen = unused_loopback_address();
        let first_sync = SyncRuntime::start(
            &first.sync_config(
                Some(first_listen),
                vec![ConnectUrl::from_str(&format!("oll://{second_listen}")).unwrap()],
            ),
            Arc::clone(&first.identities),
            Arc::clone(&first_replica),
            first_logger,
        )
        .await
        .unwrap();
        let second_sync = SyncRuntime::start(
            &second.sync_config(
                Some(second_listen),
                vec![ConnectUrl::from_str(&format!("oll://{first_listen}")).unwrap()],
            ),
            Arc::clone(&second.identities),
            Arc::clone(&second_replica),
            second_logger,
        )
        .await
        .unwrap();

        let ready_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if first_sync
                .status()
                .await
                .iter()
                .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
                && second_sync
                    .status()
                    .await
                    .iter()
                    .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
            {
                break;
            }
            assert!(
                Instant::now() < ready_deadline,
                "normal session was not ready"
            );
            sleep(Duration::from_millis(25)).await;
        }
        let (expected_first_direction, expected_second_direction) =
            if first.identity.node_id() < second.identity.node_id() {
                (
                    PeerConnectionDirection::Outbound as i32,
                    PeerConnectionDirection::Inbound as i32,
                )
            } else {
                (
                    PeerConnectionDirection::Inbound as i32,
                    PeerConnectionDirection::Outbound as i32,
                )
            };
        loop {
            let first_direction = first_sync.status().await[0].direction;
            let second_direction = second_sync.status().await[0].direction;
            if first_direction == expected_first_direction
                && second_direction == expected_second_direction
            {
                break;
            }
            assert!(
                Instant::now() < ready_deadline,
                "duplicate-session arbitration did not stabilize"
            );
            sleep(Duration::from_millis(25)).await;
        }
        let second_name = second.identity.node_name().clone();
        let (first_result, result) = tokio::join!(
            first_sync.synchronize(Some(&second_name), 3, "simultaneous-first-correlation"),
            second_sync.synchronize(None, 3, "simultaneous-second-correlation"),
        );
        let first_result = first_result.unwrap();
        let result = result.unwrap();
        assert_eq!(first_result.len(), 1);
        assert_ne!(
            first_result[0].outcome,
            PeerSyncOutcome::Failed as i32,
            "{first_result:?}"
        );
        assert_eq!(result.len(), 1);
        assert_ne!(
            result[0].outcome,
            PeerSyncOutcome::Failed as i32,
            "{result:?}"
        );
        assert_eq!(read_text(&first_replica, "/first.md").await, "first");
        assert_eq!(read_text(&first_replica, "/second.md").await, "second");
        assert_eq!(read_text(&second_replica, "/first.md").await, "first");
        assert_eq!(read_text(&second_replica, "/second.md").await, "second");
        assert_eq!(
            read_text(&first_replica, "/shared.md").await,
            read_text(&second_replica, "/shared.md").await
        );
        for replica in [&first_replica, &second_replica] {
            assert_eq!(read_text(replica, "/folder/move.md").await, "move me");
            assert_eq!(read_text(replica, "/renamed.md").await, "rename me");
        }
        for root in [&first.root, &second.root] {
            assert!(!root.join("move.md").exists());
            assert!(!root.join("rename.md").exists());
            assert!(!root.join("delete.md").exists());
        }
        assert_eq!(fs::read(second.root.join("image.gif")).unwrap(), binary);
        first_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        second_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        let logs = [&first.log_dir, &second.log_dir].map(|log_dir| {
            fs::read_to_string(log_dir.join("sync.log"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>()
        });
        let round_correlation = [
            "simultaneous-first-correlation",
            "simultaneous-second-correlation",
        ]
        .into_iter()
        .find(|correlation_id| {
            logs.iter().all(|events| {
                events.iter().any(|event| {
                    event["event"] == "sync_replica_transfer_staged"
                        && event["correlation_id"] == *correlation_id
                }) && events.iter().any(|event| {
                    event["event"] == "sync_candidate_committed"
                        && event["correlation_id"] == *correlation_id
                })
            })
        })
        .expect("one inherited correlation must span both directions of the finite round");
        assert!(!round_correlation.is_empty());

        let second_result = second_sync
            .synchronize(None, 1, "already-converged-correlation")
            .await
            .unwrap();
        assert_eq!(
            second_result[0].outcome,
            PeerSyncOutcome::AlreadySatisfied as i32
        );

        let shutdown_deadline = Instant::now() + Duration::from_secs(5);
        second_sync.shutdown(shutdown_deadline).await.unwrap();
        first_sync.shutdown(shutdown_deadline).await.unwrap();
        second_replica.shutdown(shutdown_deadline).await.unwrap();
        first_replica.shutdown(shutdown_deadline).await.unwrap();
    }
}
