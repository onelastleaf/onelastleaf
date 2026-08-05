use std::path::PathBuf;

use time::OffsetDateTime;
use uuid::Uuid;

use super::super::PluginError;
use super::{DesiredPluginState, InstallMode, PluginId, PluginInstanceId, PluginName};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledPlugin {
    pub plugin_id: PluginId,
    pub plugin_name: PluginName,
    pub normalized_declaration: Vec<u8>,
    pub declaration_sha256: [u8; 32],
    pub effective_manifest: Vec<u8>,
    pub selected_commit: Option<String>,
    pub install_mode: InstallMode,
    pub release_id: Option<String>,
    pub current_generation: Uuid,
    pub running_generation: Option<Uuid>,
    pub running_instance_id: Option<PluginInstanceId>,
    pub desired_state: DesiredPluginState,
    pub restart_sequence: u64,
    pub consumed_restart_sequence: u64,
    pub restart_attempt: u32,
    pub restart_not_before: Option<OffsetDateTime>,
    pub last_lifecycle_failure: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackagePublishIntent {
    pub plugin_id: PluginId,
    pub plugin_name: PluginName,
    pub operation_id: String,
    pub expected_current_generation: Option<Uuid>,
    pub candidate_generation: Uuid,
    pub normalized_declaration: Vec<u8>,
    pub declaration_sha256: [u8; 32],
    pub effective_manifest: Vec<u8>,
    pub selected_commit: Option<String>,
    pub install_mode: InstallMode,
    pub release_id: Option<String>,
    pub correlation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalPhase {
    Prepared,
    DeclarationPublished,
    PackageTrashed,
}

impl RemovalPhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::DeclarationPublished => "declaration_published",
            Self::PackageTrashed => "package_trashed",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PluginError> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "declaration_published" => Ok(Self::DeclarationPublished),
            "package_trashed" => Ok(Self::PackageTrashed),
            _ => Err(PluginError::CorruptStore(
                "plugin removal phase is invalid".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemovalIntent {
    pub plugin_id: PluginId,
    pub operation_id: String,
    pub plugins_lua_sha256: [u8; 32],
    pub prepared_plugins_lua: Vec<u8>,
    pub trash_path: PathBuf,
    pub phase: RemovalPhase,
    pub correlation_id: String,
}
