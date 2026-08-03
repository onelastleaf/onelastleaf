mod active;
mod blob;
mod commit;
mod generation;
mod identity;
mod load;
mod open;
mod operations;
mod peers;
mod projection;
mod schema;
mod state;
mod support;
mod write;

#[cfg(test)]
mod tests;

use sqlx::AnyPool;

pub use blob::{NewBlob, NewBlobSource};
pub use commit::RetainedCommit;
pub(crate) use identity::{IdentityTransition, IdentityTransitionKind};
pub(crate) use peers::{BootstrapClaim, PeerBinding};

#[derive(Debug)]
pub struct ReplicaStore {
    pub(super) pool: AnyPool,
}
