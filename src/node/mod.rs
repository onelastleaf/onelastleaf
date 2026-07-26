//! Unix-only node lifecycle and local administration runtime.

mod admin;
mod identity;
mod init;
mod liveness;
mod lock;
mod logging;
mod runtime;

pub use liveness::{ParentLivenessPipe, wait_for_parent_exit};
pub use runtime::{NodeError, execute};
