//! Unix-only node lifecycle and local administration runtime.

mod admin;
pub(crate) mod identity;
mod init;
mod liveness;
mod lock;
pub(crate) mod logging;
mod runtime;

#[cfg(test)]
pub(crate) use identity::NodeIdentity;
pub use liveness::{ParentLivenessPipe, wait_for_parent_exit};
pub use runtime::{NodeError, execute};
