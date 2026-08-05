use std::path::PathBuf;

use time::OffsetDateTime;

use super::{PluginArtifactId, PluginId, PluginJobId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginArtifact {
    pub artifact_id: PluginArtifactId,
    pub job_id: PluginJobId,
    pub plugin_id: PluginId,
    pub file_name: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
    pub destination: PathBuf,
    pub stored_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPublishIntent {
    pub artifact_id: PluginArtifactId,
    pub job_id: PluginJobId,
    pub plugin_id: PluginId,
    pub file_name: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
    pub staging_path: PathBuf,
    pub destination: PathBuf,
    pub correlation_id: String,
}
