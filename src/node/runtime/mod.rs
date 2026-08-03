mod blocking;
mod client;
mod daemon;
mod dispatch;
mod error;
mod identity_watch;
mod launcher;
mod socket;

#[cfg(test)]
mod tests;

pub use dispatch::execute;
pub use error::NodeError;

use std::time::Duration;

const STARTUP_DEADLINE: Duration = Duration::from_secs(10);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const LAUNCHER_TERMINATION_GRACE: Duration = Duration::from_secs(2);
const IDENTITY_WATCH_DEBOUNCE: Duration = Duration::from_millis(200);
