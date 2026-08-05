mod artifacts;
mod cleanup;
mod handshake;
mod incoming;
mod jobs;
mod outbound;
mod outcome;
mod router;
mod service;

#[cfg(test)]
pub(super) use handshake::{validate_envelope, validate_handshake_trace};
#[cfg(test)]
pub(super) use incoming::quiescing_allows;
#[cfg(test)]
pub(super) use outbound::{send_payload, try_send_payload};
pub(super) use outcome::SessionOutcome;
pub(super) use router::run_session;
pub(super) use service::{ConnectedSession, InstanceService};

const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const SESSION_FAILURE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const SESSION_FAILURE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
const ARTIFACT_COMMAND_CAPACITY: usize = 32;
const SESSION_WORK_CAPACITY: usize = 64;
