//! Plugin process supervision and the instance-owned gRPC runtime session.

mod host;
mod jobs;
mod plugin_log;
mod process;
mod session;
mod supervisor;
mod trace;
mod value;

#[cfg(test)]
mod e2e_tests;
#[cfg(test)]
mod tests;

use std::time::Duration;
use std::{path::PathBuf, sync::Arc};

use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

use crate::{
    configuration::ConfigRuntime,
    node::{ParentLivenessPipe, identity::IdentityCoordinator, logging::NodeLogger},
    replica::ReplicaRuntime,
};

use super::{
    ArtifactPublisher, JobCancellationReason, PluginError, PluginId, PluginInstanceId, PluginJob,
    PluginStore,
    package::{PackageLayout, PluginPackageGates},
};

#[cfg(test)]
pub(crate) use supervisor::SaturatedInstanceWorkQueue;
pub use supervisor::{PluginAction, PluginSessionSnapshot, PluginSupervisor};
pub(crate) use value::{
    MAXIMUM_VALUE_DEPTH, valid_duration, valid_timestamp, validate_serializable_config_value,
};

pub const MAXIMUM_CALL_DEPTH: u32 = 10;
pub const MAXIMUM_CAUSAL_DEPTH: u32 = 10;

const INSTANCE_COMMAND_CAPACITY: usize = 64;
const OUTBOUND_ENVELOPE_CAPACITY: usize = 128;
pub(super) const JOB_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct RuntimeDependencies {
    store: PluginStore,
    packages: PackageLayout,
    package_gates: Arc<PluginPackageGates>,
    config_root: PathBuf,
    config: ConfigRuntime,
    replica: Arc<ReplicaRuntime>,
    identities: Arc<IdentityCoordinator>,
    logger: Arc<NodeLogger>,
    artifacts: ArtifactPublisher,
    parent_liveness: Arc<ParentLivenessPipe>,
}

enum InstanceCommand {
    StartJob {
        job: PluginJob,
        response: oneshot::Sender<Result<PluginJob, PluginError>>,
    },
    CancelJob {
        job: PluginJob,
        reason: JobCancellationReason,
        dispatched: oneshot::Sender<Result<(), PluginError>>,
    },
}

#[derive(Clone)]
struct InstanceShutdown {
    reason: String,
    correlation_id: String,
    deadline: tokio::time::Instant,
}

#[derive(Clone)]
struct InstanceSender {
    work: mpsc::Sender<InstanceCommand>,
    shutdown: tokio::sync::watch::Sender<Option<InstanceShutdown>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InstanceWorkClosed;

impl InstanceSender {
    async fn send_work(&self, command: InstanceCommand) -> Result<(), InstanceWorkClosed> {
        self.work
            .send(command)
            .await
            .map_err(|_| InstanceWorkClosed)
    }

    fn shutdown(&self, request: InstanceShutdown) {
        self.shutdown.send_if_modified(|current| match current {
            Some(current) if request.deadline < current.deadline => {
                current.deadline = request.deadline;
                true
            }
            Some(_) => false,
            slot @ None => {
                *slot = Some(request);
                true
            }
        });
    }
}

enum InstanceNotice {
    Ready {
        plugin_id: PluginId,
        instance_id: PluginInstanceId,
        actions: Vec<PluginAction>,
        correlation_id: String,
    },
    Spawned {
        plugin_id: PluginId,
        instance_id: PluginInstanceId,
        process_id: u32,
        started_at: OffsetDateTime,
    },
    Ended {
        plugin_id: PluginId,
        instance_id: PluginInstanceId,
        failure: Option<String>,
        correlation_id: String,
        ended_at: OffsetDateTime,
    },
}
