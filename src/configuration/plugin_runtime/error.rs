use std::{fmt, io, path::PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginConfigErrorKind {
    InvalidArgument,
    NotFound,
    AlreadyExists,
    FailedPrecondition,
    Internal,
}

#[derive(Debug)]
pub enum PluginConfigError {
    InvalidSessionId,
    InvalidPluginId,
    SessionAlreadyExists,
    SessionNotActive,
    ConfigNotFound { path: PathBuf },
    Read { path: PathBuf, kind: io::ErrorKind },
    InvalidUtf8 { path: PathBuf },
    Evaluation { path: PathBuf },
    InvalidPath(&'static str),
    ValueNotFound,
    UnsupportedValue(&'static str),
    CyclicValue,
    FunctionSessionMismatch,
    FunctionNotFound,
    Invocation,
    RuntimeUnavailable,
}

impl PluginConfigError {
    pub fn kind(&self) -> PluginConfigErrorKind {
        match self {
            Self::InvalidSessionId
            | Self::InvalidPluginId
            | Self::InvalidPath(_)
            | Self::UnsupportedValue(_)
            | Self::CyclicValue
            | Self::Evaluation { .. }
            | Self::InvalidUtf8 { .. }
            | Self::Invocation => PluginConfigErrorKind::InvalidArgument,
            Self::ConfigNotFound { .. } | Self::ValueNotFound | Self::FunctionNotFound => {
                PluginConfigErrorKind::NotFound
            }
            Self::SessionAlreadyExists => PluginConfigErrorKind::AlreadyExists,
            Self::SessionNotActive | Self::FunctionSessionMismatch => {
                PluginConfigErrorKind::FailedPrecondition
            }
            Self::Read { .. } | Self::RuntimeUnavailable => PluginConfigErrorKind::Internal,
        }
    }
}

impl fmt::Display for PluginConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionId => formatter.write_str("plugin session ID must not be empty"),
            Self::InvalidPluginId => formatter.write_str("plugin ID is invalid"),
            Self::SessionAlreadyExists => formatter.write_str("plugin session already exists"),
            Self::SessionNotActive => formatter.write_str("plugin session is not active"),
            Self::ConfigNotFound { .. } => {
                formatter.write_str("plugin configuration does not exist")
            }
            Self::Read { kind, .. } => {
                write!(formatter, "cannot read plugin configuration: {kind}")
            }
            Self::InvalidUtf8 { .. } => {
                formatter.write_str("plugin configuration is not valid UTF-8 Lua source")
            }
            Self::Evaluation { .. } => formatter.write_str(
                "cannot evaluate plugin configuration; check its Lua syntax and runtime operations",
            ),
            Self::InvalidPath(problem) => {
                write!(formatter, "invalid plugin config path: {problem}")
            }
            Self::ValueNotFound => formatter.write_str("plugin configuration value was not found"),
            Self::UnsupportedValue(kind) => {
                write!(
                    formatter,
                    "plugin configuration contains unsupported {kind}"
                )
            }
            Self::CyclicValue => {
                formatter.write_str("plugin configuration contains a cyclic table")
            }
            Self::FunctionSessionMismatch => {
                formatter.write_str("configuration function belongs to another plugin session")
            }
            Self::FunctionNotFound => {
                formatter.write_str("configuration function does not exist in the active session")
            }
            Self::Invocation => formatter.write_str("configuration function invocation failed"),
            Self::RuntimeUnavailable => formatter.write_str("configuration runtime is unavailable"),
        }
    }
}

impl std::error::Error for PluginConfigError {}
