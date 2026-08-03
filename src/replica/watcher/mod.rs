mod events;
mod filesystem;
mod projection;
mod query;
mod shutdown;
mod start;
mod state;
mod support;
mod types;

use std::time::Duration;

#[cfg(test)]
pub(super) use types::DocumentInspection;
pub use types::ReplicaRuntime;

pub(super) const WATCH_DEBOUNCE: Duration = Duration::from_millis(200);
pub(super) const PROJECTION_ATTEMPTS: u32 = 3;
pub(super) const PROJECTION_RETRY_DELAY: Duration = Duration::from_millis(100);
