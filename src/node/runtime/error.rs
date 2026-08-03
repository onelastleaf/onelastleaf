use std::{fmt, io, path::PathBuf};

#[derive(Debug)]
pub enum NodeError {
    Config(String),
    Unavailable(String),
    Operation(String),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Internal(String),
    NotImplemented,
}

impl NodeError {
    pub fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }

    pub fn config_io(operation: &'static str, path: PathBuf, source: io::Error) -> Self {
        Self::ConfigIo {
            operation,
            path,
            source,
        }
    }

    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_) | Self::ConfigIo { .. } => crate::cli::EXIT_CONFIG,
            Self::Unavailable(_) | Self::NotImplemented => crate::cli::EXIT_UNAVAILABLE,
            Self::Operation(_) | Self::Io { .. } | Self::Internal(_) => 1,
        }
    }
}

impl fmt::Display for NodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message)
            | Self::Unavailable(message)
            | Self::Operation(message)
            | Self::Internal(message) => formatter.write_str(message),
            Self::Io { operation, source } => write!(formatter, "cannot {operation}: {source}"),
            Self::ConfigIo {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "cannot {operation} at {}: {source}",
                path.display()
            ),
            Self::NotImplemented => formatter.write_str("command is not implemented"),
        }
    }
}
