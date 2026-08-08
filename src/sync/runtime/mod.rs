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
    HANDSHAKE_DEADLINE, NoiseTransport, PendingSession, ROUND_PROGRESS_DEADLINE, RoundError,
    RoundResult, SessionChannel, SessionError, SessionReplicaMode, SyncObservation,
    derive_noise_psk, receive_bootstrap_round, receive_round, security::NoisePsk,
    send_bootstrap_round, send_round, transport::TransportError,
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
    SessionLost(SessionError),
    ProgressTimeout { failure_stage: &'static str },
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
            Self::SessionLost(error) => error.fmt(formatter),
            Self::ProgressTimeout { failure_stage } => {
                write!(
                    formatter,
                    "sync round made no progress during {failure_stage}"
                )
            }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionDisposition {
    RetryWithBackoff,
    ReconnectImmediately,
    SuppressedByActiveSession(Uuid),
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
    handshake_hash: [u8; 32],
    connection_state: PeerConnectionState,
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

mod bidirectional;
mod bootstrap;
mod connection;
mod control;
mod lifecycle;
mod listener;
mod liveness;
mod outbound;
mod ready;
mod registry;
mod shutdown;
mod support;

#[cfg(test)]
mod tests;

use bidirectional::*;
use bootstrap::*;
use connection::*;
use listener::*;
use liveness::*;
use outbound::*;
use ready::*;
use support::*;
