use std::{fmt, io};

#[derive(Debug)]
pub enum PluginError {
    InvalidArgument(String),
    NotFound(String),
    AlreadyExists(String),
    Aborted(String),
    FailedPrecondition(String),
    CorruptStore(String),
    Store(String),
    Io {
        operation: &'static str,
        source: io::Error,
    },
}

impl PluginError {
    pub fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "invalid_argument",
            Self::NotFound(_) => "not_found",
            Self::AlreadyExists(_) => "already_exists",
            Self::Aborted(_) => "aborted",
            Self::FailedPrecondition(_) => "failed_precondition",
            Self::CorruptStore(_) => "corrupt_store",
            Self::Store(_) => "store",
            Self::Io { .. } => "io",
        }
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message)
            | Self::NotFound(message)
            | Self::AlreadyExists(message)
            | Self::Aborted(message)
            | Self::FailedPrecondition(message)
            | Self::CorruptStore(message)
            | Self::Store(message) => formatter.write_str(message),
            Self::Io { operation, source } => write!(formatter, "cannot {operation}: {source}"),
        }
    }
}

impl std::error::Error for PluginError {}
