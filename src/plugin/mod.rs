//! Deployment-local plugin identities and durable SQL state.

mod artifact;
mod error;
pub mod package;
pub(crate) mod protocol;
pub mod runtime;
mod store;
mod system;
mod types;

#[cfg(test)]
mod artifact_tests;
#[cfg(test)]
mod protocol_tests;

pub(crate) use artifact::ArtifactPublisher;
#[cfg(test)]
pub(crate) use artifact::{ArtifactSession, MAX_ARTIFACT_CHUNK_BYTES};
pub use error::PluginError;
pub use store::PluginStore;
pub use system::{
    PluginInspection, PluginJobInspection, PluginJobListEntry, PluginListEntry, PluginRuntime,
};
pub use types::{
    ArtifactPublishIntent, DesiredPluginState, InstallMode, InstalledPlugin, JobAdmission,
    JobCancellation, JobCancellationReason, JobDeadline, JobState, NormalizedJobPayload,
    ObservedPluginState, PackagePublishIntent, PluginArtifact, PluginArtifactId, PluginId,
    PluginInstanceId, PluginJob, PluginJobCounts, PluginJobId, PluginName, PluginOperationId,
    PluginSelector, RemovalIntent, RemovalPhase,
};
