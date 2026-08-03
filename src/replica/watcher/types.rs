use std::{
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
};

use notify_debouncer_full::{Debouncer, RecommendedCache, notify::RecommendedWatcher};
use tokio::{
    sync::{Mutex, RwLock, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::node::{identity::IdentityCoordinator, logging::NodeLogger};

use super::super::{store::ReplicaStore, types::ActiveReplica};

#[derive(Clone, Debug)]
pub struct DocumentInspection {
    pub catalog_node_id: Uuid,
    pub catalog_revision: [u8; 32],
    pub document_id: Uuid,
    pub document_revision: [u8; 32],
    pub path: String,
    pub media_type: String,
    pub encoding: String,
    pub has_byte_order_mark: bool,
    pub size_bytes: u64,
}

pub struct ReplicaRuntime {
    pub(crate) config_root: PathBuf,
    pub(crate) root: PathBuf,
    pub(crate) store: Arc<ReplicaStore>,
    pub(crate) state: RwLock<Option<ActiveReplica>>,
    pub(crate) identities: Arc<IdentityCoordinator>,
    pub(crate) logger: Arc<NodeLogger>,
    pub(super) watcher: StdMutex<Option<Debouncer<RecommendedWatcher, RecommendedCache>>>,
    pub(super) event_shutdown: watch::Sender<bool>,
    pub(super) event_task: Mutex<Option<JoinHandle<()>>>,
}
