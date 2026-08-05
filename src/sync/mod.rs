//! Authenticated peer transport and replica synchronization.

use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) struct SyncObservation<'a> {
    pub(crate) connection_id: Uuid,
    pub(crate) peer_node_id: Uuid,
    pub(crate) direction: &'static str,
    pub(crate) correlation_id: &'a str,
}

mod round;
mod runtime;
mod security;
mod session;
mod transport;

pub(crate) use round::{
    RoundError, RoundResult, receive_bootstrap_round, receive_round, send_bootstrap_round,
    send_round,
};
pub(crate) use runtime::{SyncError, SyncRuntime};
pub(crate) use security::derive_noise_psk;
pub(crate) use session::{
    PendingSession, ROUND_PROGRESS_DEADLINE, SessionChannel, SessionError, SessionReplicaMode,
};
pub(crate) use transport::{HANDSHAKE_DEADLINE, NoiseTransport};
