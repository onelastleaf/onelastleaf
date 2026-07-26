use std::{
    env,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;

use super::Cli;

pub const EXIT_UNAVAILABLE: u8 = 69;
pub const EXIT_CONFIG: u8 = 78;

pub(super) const DEFAULT_CONFIG_SUFFIX: &str = ".config/oll";
pub(super) const DEFAULT_REPLICA_SUFFIX: &str = ".local/share/oll";
pub(super) const DEFAULT_LOG_SUFFIX: &str = ".local/state/oll";
pub(super) const XDG_LOG_SUFFIX: &str = "oll";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Environment {
    pub home: Option<PathBuf>,
    pub state_home: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub replica: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
}

impl Environment {
    pub fn from_process() -> Self {
        Self {
            home: env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            state_home: env::var_os("XDG_STATE_HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            config: env::var_os("OLL_CONFIG")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            replica: env::var_os("OLL_REPLICA")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            log_dir: env::var_os("OLL_LOG_DIR")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
        }
    }

    pub(super) fn config_root(&self) -> Result<PathBuf, CliError> {
        resolve_path(
            None,
            self.config.as_ref(),
            self.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
    }

    pub(super) fn log_dir(&self) -> Result<PathBuf, CliError> {
        resolve_log_dir(None, self)
    }
}

pub(super) fn resolve_log_dir(
    cli: Option<&PathBuf>,
    environment: &Environment,
) -> Result<PathBuf, CliError> {
    if let Some(path) = cli.or(environment.log_dir.as_ref()) {
        return Ok(path.clone());
    }
    if let Some(state_home) = &environment.state_home {
        return Ok(state_home.join(XDG_LOG_SUFFIX));
    }

    environment
        .home
        .as_ref()
        .map(|home| home.join(DEFAULT_LOG_SUFFIX))
        .ok_or(CliError::MissingHome {
            name: "log directory",
        })
}

pub(super) fn resolve_path(
    cli: Option<&PathBuf>,
    environment: Option<&PathBuf>,
    home: Option<&PathBuf>,
    default_suffix: &str,
    name: &'static str,
) -> Result<PathBuf, CliError> {
    if let Some(path) = cli.or(environment) {
        return Ok(path.clone());
    }

    home.map(|path| path.join(default_suffix))
        .ok_or(CliError::MissingHome { name })
}

pub(super) fn ensure_persistable_path(path: &Path, name: &'static str) -> Result<(), CliError> {
    if path.to_str().is_none() {
        return Err(CliError::NonUtf8PersistentPath { name });
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
pub enum CliError {
    MissingHome { name: &'static str },
    NonUtf8PersistentPath { name: &'static str },
}

impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::MissingHome { .. } | Self::NonUtf8PersistentPath { .. } => {
                ExitCode::from(EXIT_CONFIG)
            }
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHome { name } => write!(
                formatter,
                "cannot determine {name}: pass an explicit path or set HOME"
            ),
            Self::NonUtf8PersistentPath { name } => {
                write!(formatter, "cannot persist {name}: path is not valid UTF-8")
            }
        }
    }
}

/// Resolve a client-provided OS path against the client's launch directory.
/// Absolute paths are returned unchanged, including `.` and `..` segments.
pub fn resolve_client_path(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        cwd.join(path)
    }
}

pub fn parse_from<I, T>(arguments: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(arguments)?;
    cli.validate()?;
    Ok(cli)
}
