use std::sync::Arc;

use tokio::{sync::watch, time::Instant};

use crate::node::logging::NodeLogger;

use super::{PluginStore, package::PackageManager, runtime::PluginSupervisor};

mod jobs;
mod lifecycle;
mod operations;
mod package;
mod queries;

pub use jobs::{PluginJobInspection, PluginJobListEntry};
pub use queries::{PluginInspection, PluginListEntry};

/// Deployment-local owner of package, process, job, and artifact state.
pub struct PluginRuntime {
    store: PluginStore,
    packages: PackageManager,
    supervisor: Arc<PluginSupervisor>,
    package_shutdown: watch::Sender<Option<Instant>>,
    operations: Arc<operations::OperationTracker>,
    logger: Arc<NodeLogger>,
}
