use super::super::PluginError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DesiredPluginState {
    Running,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservedPluginState {
    Starting,
    Ready,
    Stopping,
    Exited,
    Failed,
}

impl DesiredPluginState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PluginError> {
        match value {
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            _ => Err(PluginError::CorruptStore(
                "plugin desired state is invalid".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallMode {
    Source,
    Release,
}

impl InstallMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Release => "release",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PluginError> {
        match value {
            "source" => Ok(Self::Source),
            "release" => Ok(Self::Release),
            _ => Err(PluginError::CorruptStore(
                "plugin install mode is invalid".to_owned(),
            )),
        }
    }
}
