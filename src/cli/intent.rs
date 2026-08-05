use std::{
    fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
};

use clap::{CommandFactory, error::ErrorKind};

use crate::configuration::ResolvedNodeConfig;

use super::environment::{ensure_persistable_path, resolve_log_dir};
use super::{
    Cli, CliError, Command, ConnectUrl, Environment, GitRemote, JobCommand, LogCommand,
    LogFilterLevel, LogTarget, LoopbackAddr, NodeName, OutputFormat, PluginArgs, PluginCommand,
    ReplicaCommand, SnapshotCommand, resolve_client_path,
};

impl Cli {
    pub fn into_intent(self) -> Result<CliIntent, clap::Error> {
        match self.command {
            Command::Init(args) => Ok(CliIntent::Init(InitIntent {
                node_name: args.node_name,
                connect: args.connect,
                listen: args.listen,
                replica_root: args.replica,
                config_root: args.config,
                log_dir: args.log_dir,
            })),
            Command::Run(args) => Ok(CliIntent::Run(RunIntent {
                replica_root: args.replica,
                config_root: args.config,
                log_dir: args.log_dir,
                listen: args.listen,
                connect: args.connect,
                pingback: args.pingback,
            })),
            Command::Start => Ok(CliIntent::Start),
            Command::Stop => Ok(CliIntent::Stop),
            Command::Status(args) => Ok(CliIntent::Status { json: args.json }),
            Command::Log(args) => Ok(CliIntent::Log(args.command.into())),
            Command::Replica(args) => Ok(CliIntent::Replica(args.command.into())),
            Command::Sync(args) => match (args.log, args.node_name, args.retries) {
                (true, None, None) => Ok(CliIntent::Sync(SyncIntent::ViewLog)),
                (false, node_name, retries) => Ok(CliIntent::Sync(SyncIntent::Synchronize {
                    node_name,
                    max_attempts: retries,
                })),
                _ => Err(intent_error(
                    ErrorKind::ArgumentConflict,
                    "--log cannot be combined with a node name or --retries",
                )),
            },
            Command::Ping(args) => Ok(CliIntent::Ping {
                node_name: args.node_name,
            }),
            Command::Psk => Ok(CliIntent::Psk),
            Command::Plugin(args) => plugin_intent(args).map(CliIntent::Plugin),
            Command::Job(args) => job_intent(args.command).map(CliIntent::Job),
        }
    }
}

fn intent_error(kind: ErrorKind, message: impl fmt::Display) -> clap::Error {
    Cli::command().error(kind, message)
}

#[derive(Debug, PartialEq)]
pub enum CliIntent {
    Init(InitIntent),
    Run(RunIntent),
    Start,
    Stop,
    Status { json: bool },
    Log(LogIntent),
    Replica(ReplicaIntent),
    Sync(SyncIntent),
    Ping { node_name: NodeName },
    Psk,
    Plugin(PluginIntent),
    Job(JobIntent),
}

#[derive(Debug, PartialEq)]
pub struct InitIntent {
    pub node_name: NodeName,
    pub connect: Vec<ConnectUrl>,
    pub listen: Option<SocketAddr>,
    pub replica_root: Option<PathBuf>,
    pub config_root: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
}

impl InitIntent {
    pub(super) fn replica_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        environment.replica_root(self.replica_root.as_ref())
    }

    pub(super) fn config_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        environment.config_root_with_cli(self.config_root.as_ref())
    }

    pub(super) fn log_dir(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_log_dir(self.log_dir.as_ref(), environment)
    }
}

#[derive(Debug, PartialEq)]
pub struct RunIntent {
    pub replica_root: Option<PathBuf>,
    pub config_root: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub connect: Vec<ConnectUrl>,
    pub pingback: Option<LoopbackAddr>,
}

impl RunIntent {
    pub(super) fn config_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        environment.config_root_with_cli(self.config_root.as_ref())
    }
}

#[derive(Debug)]
pub enum PreparedCliIntent {
    Init(PreparedInitIntent),
    Run(PreparedRunIntent),
    Client(PreparedClientIntent),
}

#[derive(Debug)]
pub struct PreparedInitIntent {
    pub node_name: NodeName,
    pub connect: Vec<ConnectUrl>,
    pub listen: Option<SocketAddr>,
    pub replica_root: PathBuf,
    pub replica_store_base: PathBuf,
    pub config_root: PathBuf,
    pub log_dir: PathBuf,
    pub artifact_download_dir: PathBuf,
}

#[derive(Debug)]
pub struct PreparedRunIntent {
    pub config_root: PathBuf,
    pub platform_data_dir: Option<PathBuf>,
    pub overrides: RunOverrides,
    pub pingback: Option<LoopbackAddr>,
}

/// Runtime-only values resolved from CLI and environment inputs. The node
/// handler applies them after it owns the deployment lock and loads config.lua.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunOverrides {
    pub replica_root: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub connect: Option<Vec<ConnectUrl>>,
}

impl RunOverrides {
    pub fn apply_to(&self, config: &mut ResolvedNodeConfig) {
        if let Some(replica_root) = &self.replica_root {
            config.replica_root = replica_root.clone();
        }
        if let Some(log_dir) = &self.log_dir {
            config.log_dir = log_dir.clone();
        }
        if let Some(listen) = self.listen {
            config.listen = Some(listen);
        }
        if let Some(connect) = &self.connect {
            config.connect = connect.clone();
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PreparedClientIntent {
    pub intent: CliIntent,
    pub dependency: ClientDependency,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ClientDependency {
    None,
    ConfigRoot(PathBuf),
    LogDir(PathBuf),
}

#[derive(Debug, PartialEq)]
pub enum ReplicaIntent {
    Inspect {
        document: PathBuf,
    },
    Ops {
        document: PathBuf,
        limit: Option<NonZeroUsize>,
        format: OutputFormat,
    },
    Export {
        output: PathBuf,
    },
    Import {
        snapshot: PathBuf,
    },
    SnapshotInspect {
        snapshot: PathBuf,
        json: bool,
    },
    SnapshotVerify {
        snapshot: PathBuf,
    },
}

impl From<ReplicaCommand> for ReplicaIntent {
    fn from(command: ReplicaCommand) -> Self {
        match command {
            ReplicaCommand::Inspect { document } => Self::Inspect { document },
            ReplicaCommand::Ops {
                document,
                limit,
                format,
            } => Self::Ops {
                document,
                limit,
                format,
            },
            ReplicaCommand::Export { output } => Self::Export { output },
            ReplicaCommand::Import { snapshot } => Self::Import { snapshot },
            ReplicaCommand::Snapshot(snapshot) => match snapshot.command {
                SnapshotCommand::Inspect { snapshot, json } => {
                    Self::SnapshotInspect { snapshot, json }
                }
                SnapshotCommand::Verify { snapshot } => Self::SnapshotVerify { snapshot },
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SyncIntent {
    Synchronize {
        node_name: Option<NodeName>,
        max_attempts: Option<NonZeroU32>,
    },
    ViewLog,
}

#[derive(Debug, PartialEq)]
pub enum LogIntent {
    Set {
        target: LogTarget,
        level: LogFilterLevel,
    },
}

impl From<LogCommand> for LogIntent {
    fn from(command: LogCommand) -> Self {
        match command {
            LogCommand::Set { directive } => Self::Set {
                target: directive.target,
                level: directive.level,
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum GitSelector {
    Default,
    Revision(String),
    Branch(String),
}

#[derive(Debug, PartialEq)]
pub enum PluginLogTarget {
    All,
    Plugin(String),
}

#[derive(Debug, PartialEq)]
pub enum PluginIntent {
    Install(PluginInstallIntent),
    Reconcile {
        json: bool,
    },
    Validate,
    List {
        json: bool,
    },
    Info {
        selector: String,
        json: bool,
    },
    Releases {
        selector: String,
        json: bool,
    },
    Start {
        selector: String,
    },
    Stop {
        selector: String,
    },
    Restart {
        selector: String,
    },
    Update {
        selector: String,
        json: bool,
    },
    Remove {
        selector: String,
        json: bool,
    },
    ViewLog {
        target: PluginLogTarget,
    },
    Call {
        selector: String,
        action: String,
        arguments: Vec<String>,
        operation_id: Option<String>,
        json: bool,
    },
}

#[derive(Debug, PartialEq)]
pub enum PluginInstallIntent {
    Declared {
        json: bool,
    },
    Remote {
        remote: Box<GitRemote>,
        selector: GitSelector,
        mode: PluginInstallMode,
        json: bool,
    },
}

#[derive(Debug, PartialEq)]
pub enum PluginInstallMode {
    Source,
    Release { release_id: String },
}

fn plugin_intent(args: PluginArgs) -> Result<PluginIntent, clap::Error> {
    match args.command {
        PluginCommand::Install {
            repository,
            rev,
            branch,
            release,
            source,
            json,
        } => {
            let selector = match (rev, branch) {
                (None, None) => GitSelector::Default,
                (Some(revision), None) if !revision.is_empty() => GitSelector::Revision(revision),
                (None, Some(branch)) if !branch.is_empty() => GitSelector::Branch(branch),
                (Some(_), None) => {
                    return Err(intent_error(
                        ErrorKind::InvalidValue,
                        "--rev must not be empty",
                    ));
                }
                (None, Some(_)) => {
                    return Err(intent_error(
                        ErrorKind::InvalidValue,
                        "--branch must not be empty",
                    ));
                }
                (Some(_), Some(_)) => {
                    return Err(intent_error(
                        ErrorKind::ArgumentConflict,
                        "--rev cannot be combined with --branch",
                    ));
                }
            };
            let install = match repository {
                None if !matches!(&selector, GitSelector::Default)
                    || release.is_some()
                    || source =>
                {
                    return Err(intent_error(
                        ErrorKind::MissingRequiredArgument,
                        "installation options require a Git remote",
                    ));
                }
                None => PluginInstallIntent::Declared { json },
                Some(_) if release.is_some() && source => {
                    return Err(intent_error(
                        ErrorKind::ArgumentConflict,
                        "--release cannot be combined with --source",
                    ));
                }
                Some(_) if release.as_ref().is_some_and(String::is_empty) => {
                    return Err(intent_error(
                        ErrorKind::InvalidValue,
                        "--release must not be empty",
                    ));
                }
                Some(remote) => PluginInstallIntent::Remote {
                    remote: Box::new(remote),
                    selector,
                    mode: release.map_or(PluginInstallMode::Source, |release_id| {
                        PluginInstallMode::Release { release_id }
                    }),
                    json,
                },
            };
            Ok(PluginIntent::Install(install))
        }
        PluginCommand::Reconcile { json } => Ok(PluginIntent::Reconcile { json }),
        PluginCommand::Validate => Ok(PluginIntent::Validate),
        PluginCommand::List { json } => Ok(PluginIntent::List { json }),
        PluginCommand::Info { selector, json } => Ok(PluginIntent::Info { selector, json }),
        PluginCommand::Releases { selector, json } => Ok(PluginIntent::Releases { selector, json }),
        PluginCommand::Start { selector } => Ok(PluginIntent::Start { selector }),
        PluginCommand::Stop { selector } => Ok(PluginIntent::Stop { selector }),
        PluginCommand::Restart { selector } => Ok(PluginIntent::Restart { selector }),
        PluginCommand::Update { selector, json } => Ok(PluginIntent::Update { selector, json }),
        PluginCommand::Remove { selector, json } => Ok(PluginIntent::Remove { selector, json }),
        PluginCommand::Log { selector: None } => Ok(PluginIntent::ViewLog {
            target: PluginLogTarget::All,
        }),
        PluginCommand::Log {
            selector: Some(selector),
        } => Ok(PluginIntent::ViewLog {
            target: PluginLogTarget::Plugin(selector),
        }),
        PluginCommand::Call {
            selector,
            action,
            arguments,
            operation_id,
            json,
        } => {
            if operation_id.as_ref().is_some_and(String::is_empty) {
                return Err(intent_error(
                    ErrorKind::InvalidValue,
                    "--operation-id must not be empty",
                ));
            }
            if action.is_empty() {
                return Err(intent_error(
                    ErrorKind::InvalidValue,
                    "plugin action must not be empty",
                ));
            }
            Ok(PluginIntent::Call {
                selector,
                action,
                arguments,
                operation_id,
                json,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfirmationRequirement {
    ReplicaBackupCreated,
    ReplicaReplacementApproved,
}

impl ConfirmationRequirement {
    pub fn prompt(self) -> &'static str {
        match self {
            Self::ReplicaBackupCreated => {
                "Have you exported the current replica to a backup snapshot?"
            }
            Self::ReplicaReplacementApproved => {
                "Import replaces the entire current replica. Continue?"
            }
        }
    }
}

impl CliIntent {
    pub fn prepare(
        self,
        environment: &Environment,
        cwd: &Path,
    ) -> Result<PreparedCliIntent, CliError> {
        match self {
            Self::Init(args) => {
                let replica_root = resolve_client_path(&args.replica_root(environment)?, cwd);
                let replica_store_base =
                    resolve_client_path(&environment.replica_store_base()?, cwd);
                let config_root = resolve_client_path(&args.config_root(environment)?, cwd);
                let log_dir = resolve_client_path(&args.log_dir(environment)?, cwd);
                let artifact_download_dir =
                    resolve_client_path(&environment.artifact_download_dir()?, cwd);
                ensure_persistable_path(&replica_root, "replica root")?;
                ensure_persistable_path(&replica_store_base, "replica store")?;
                ensure_persistable_path(&log_dir, "log directory")?;
                ensure_persistable_path(&artifact_download_dir, "artifact download directory")?;
                Ok(PreparedCliIntent::Init(PreparedInitIntent {
                    node_name: args.node_name,
                    connect: args.connect,
                    listen: args.listen,
                    replica_root,
                    replica_store_base,
                    config_root,
                    log_dir,
                    artifact_download_dir,
                }))
            }
            Self::Run(args) => {
                let config_root = resolve_client_path(&args.config_root(environment)?, cwd);
                let overrides = RunOverrides {
                    replica_root: args
                        .replica_root
                        .as_ref()
                        .or(environment.replica.as_ref())
                        .map(|path| resolve_client_path(path, cwd)),
                    log_dir: args
                        .log_dir
                        .as_ref()
                        .or(environment.log_dir.as_ref())
                        .map(|path| resolve_client_path(path, cwd)),
                    listen: args.listen,
                    connect: if args.connect.is_empty() {
                        None
                    } else {
                        Some(args.connect)
                    },
                };

                Ok(PreparedCliIntent::Run(PreparedRunIntent {
                    config_root,
                    platform_data_dir: environment
                        .replica_store_base()
                        .ok()
                        .map(|path| resolve_client_path(&path, cwd)),
                    overrides,
                    pingback: args.pingback,
                }))
            }
            mut client => {
                client.resolve_client_paths(cwd);
                let dependency = match &client {
                    Self::Replica(
                        ReplicaIntent::SnapshotInspect { .. }
                        | ReplicaIntent::SnapshotVerify { .. },
                    )
                    | Self::Psk => ClientDependency::None,
                    Self::Sync(SyncIntent::ViewLog)
                    | Self::Plugin(PluginIntent::ViewLog { .. }) => {
                        ClientDependency::LogDir(resolve_client_path(&environment.log_dir()?, cwd))
                    }
                    Self::Init(_) | Self::Run(_) => unreachable!(),
                    _ => ClientDependency::ConfigRoot(resolve_client_path(
                        &environment.config_root()?,
                        cwd,
                    )),
                };
                Ok(PreparedCliIntent::Client(PreparedClientIntent {
                    intent: client,
                    dependency,
                }))
            }
        }
    }

    pub fn confirmation_requirements(&self) -> &'static [ConfirmationRequirement] {
        match self {
            Self::Replica(ReplicaIntent::Import { .. }) => &[
                ConfirmationRequirement::ReplicaBackupCreated,
                ConfirmationRequirement::ReplicaReplacementApproved,
            ],
            _ => &[],
        }
    }

    /// Resolve client OS paths without checking the filesystem or normalizing
    /// `.` and `..` segments. Operation handlers apply their own validation.
    pub(super) fn resolve_client_paths(&mut self, cwd: &Path) {
        let Self::Replica(intent) = self else {
            return;
        };

        let path = match intent {
            ReplicaIntent::Inspect { document } | ReplicaIntent::Ops { document, .. } => document,
            ReplicaIntent::Export { output } => output,
            ReplicaIntent::Import { snapshot }
            | ReplicaIntent::SnapshotInspect { snapshot, .. }
            | ReplicaIntent::SnapshotVerify { snapshot } => snapshot,
        };
        *path = resolve_client_path(path, cwd);
    }
}

#[derive(Debug, PartialEq)]
pub enum JobIntent {
    List { limit: u16, json: bool },
    Info { job_id: String, json: bool },
    Stop { job_id: String },
}

fn job_intent(command: JobCommand) -> Result<JobIntent, clap::Error> {
    match command {
        JobCommand::List { limit, json } if (1..=1000).contains(&limit) => {
            Ok(JobIntent::List { limit, json })
        }
        JobCommand::List { .. } => Err(intent_error(
            ErrorKind::InvalidValue,
            "--limit must be between 1 and 1000",
        )),
        JobCommand::Info { job_id, json } => Ok(JobIntent::Info { job_id, json }),
        JobCommand::Stop { job_id } => Ok(JobIntent::Stop { job_id }),
    }
}
