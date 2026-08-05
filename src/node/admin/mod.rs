mod client;
mod plugin;
mod plugin_client;
mod plugin_server;
mod server;

#[cfg(test)]
mod tests;

pub use client::{
    export_replica, get_status, import_replica, inspect_replica_document, list_replica_operations,
    ping_peer, request_shutdown, set_log_filter, synchronize_peers,
};
pub use plugin_client::{
    get_plugin, get_plugin_job, list_plugin_jobs, list_plugin_releases, list_plugins,
    reconcile_plugin_installations, remove_plugin, restart_plugin, set_plugin_desired_state,
    start_plugin_job, stop_plugin_job,
};
pub use server::serve;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, SystemTime},
};

use tokio::{sync::watch, time::Instant};

use crate::{
    configuration::ResolvedNodeConfig, plugin::PluginRuntime, protocol::oll::NodeLifecycleState,
    replica::ReplicaRuntime, sync::SyncRuntime,
};

use super::{identity::IdentityCoordinator, logging::NodeLogger};

const LIFECYCLE_STARTING: u8 = 1;
const LIFECYCLE_RUNNING: u8 = 2;
const LIFECYCLE_STOPPING: u8 = 3;
const ADMIN_CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const ADMIN_SHORT_CALL_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default)]
pub struct ShutdownNotice {
    correlation_id: Option<String>,
    requested_at: Option<Instant>,
}

impl ShutdownNotice {
    pub fn requested(correlation_id: String) -> Self {
        Self {
            correlation_id: Some(correlation_id),
            requested_at: Some(Instant::now()),
        }
    }

    pub fn is_requested(&self) -> bool {
        self.correlation_id.is_some()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    pub fn requested_at(&self) -> Option<Instant> {
        self.requested_at
    }
}

pub struct AdminState {
    identities: Arc<IdentityCoordinator>,
    config: ResolvedNodeConfig,
    started_at: SystemTime,
    lifecycle: AtomicU8,
    logger: Arc<NodeLogger>,
    replica: Arc<ReplicaRuntime>,
    sync: Arc<SyncRuntime>,
    plugins: Arc<PluginRuntime>,
    shutdown: watch::Sender<ShutdownNotice>,
}

impl AdminState {
    pub fn new(
        identities: Arc<IdentityCoordinator>,
        config: ResolvedNodeConfig,
        logger: Arc<NodeLogger>,
        replica: Arc<ReplicaRuntime>,
        sync: Arc<SyncRuntime>,
        plugins: Arc<PluginRuntime>,
        shutdown: watch::Sender<ShutdownNotice>,
    ) -> Self {
        Self {
            identities,
            config,
            started_at: SystemTime::now(),
            lifecycle: AtomicU8::new(LIFECYCLE_STARTING),
            logger,
            replica,
            sync,
            plugins,
            shutdown,
        }
    }

    pub fn mark_running(&self) {
        self.lifecycle.store(LIFECYCLE_RUNNING, Ordering::Release);
    }

    pub fn lifecycle(&self) -> NodeLifecycleState {
        match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_STARTING => NodeLifecycleState::Starting,
            LIFECYCLE_RUNNING => NodeLifecycleState::Running,
            LIFECYCLE_STOPPING => NodeLifecycleState::Stopping,
            _ => NodeLifecycleState::Unspecified,
        }
    }

    pub fn begin_shutdown(&self) -> bool {
        self.lifecycle
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_STOPPING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn is_stopping(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_STOPPING
    }
}
