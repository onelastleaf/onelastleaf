use std::{
    fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
};

use clap::{CommandFactory, error::ErrorKind};

use crate::configuration::ResolvedNodeConfig;

use super::environment::{
    DEFAULT_CONFIG_SUFFIX, DEFAULT_REPLICA_SUFFIX, ensure_persistable_path, resolve_log_dir,
    resolve_path,
};
use super::{
    Cli, CliError, Command, ConnectUrl, Environment, GitRemote, JobCommand, LogCommand,
    LogFilterLevel, LogTarget, LoopbackAddr, NodeName, OutputFormat, PluginArgs, PluginCommand,
    ReplicaCommand, SnapshotCommand, resolve_client_path,
};

impl Cli {
    pub fn validate(&self) -> Result<(), clap::Error> {
        if let Command::Plugin(args) = &self.command
            && args.command.is_none()
            && args.log.is_none()
        {
            return Err(Self::command().error(
                ErrorKind::MissingSubcommand,
                "either a plugin subcommand or --log is required",
            ));
        }

        Ok(())
    }

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
            Command::Plugin(args) => plugin_intent(args).map(CliIntent::Plugin),
            Command::Job(args) => Ok(CliIntent::Job(args.command.into())),
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
        resolve_path(
            self.replica_root.as_ref(),
            environment.replica.as_ref(),
            environment.home.as_ref(),
            DEFAULT_REPLICA_SUFFIX,
            "replica root",
        )
    }

    pub(super) fn config_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.config_root.as_ref(),
            environment.config.as_ref(),
            environment.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
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
        resolve_path(
            self.config_root.as_ref(),
            environment.config.as_ref(),
            environment.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
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
    pub config_root: PathBuf,
    pub log_dir: PathBuf,
}

#[derive(Debug)]
pub struct PreparedRunIntent {
    pub config_root: PathBuf,
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
    Validate,
    List,
    Info {
        plugin_id: String,
    },
    Start {
        plugin_id: String,
    },
    Stop {
        plugin_id: String,
    },
    Restart {
        plugin_id: String,
    },
    Update {
        plugin_id: String,
    },
    Remove {
        plugin_id: String,
    },
    ViewLog {
        target: PluginLogTarget,
    },
    Call {
        plugin_id: String,
        action: String,
        arguments: Vec<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum PluginInstallIntent {
    Declared,
    Source {
        remote: GitRemote,
        selector: GitSelector,
    },
    Release {
        remote: GitRemote,
        selector: GitSelector,
    },
}

fn plugin_intent(args: PluginArgs) -> Result<PluginIntent, clap::Error> {
    match (args.log, args.command) {
        (Some(None), None) => Ok(PluginIntent::ViewLog {
            target: PluginLogTarget::All,
        }),
        (Some(Some(plugin_id)), None) => Ok(PluginIntent::ViewLog {
            target: PluginLogTarget::Plugin(plugin_id),
        }),
        (None, Some(command)) => match command {
            PluginCommand::Install {
                repository,
                rev,
                branch,
                release,
                source,
            } => {
                let selector = match (rev, branch) {
                    (None, None) => GitSelector::Default,
                    (Some(revision), None) => GitSelector::Revision(revision),
                    (None, Some(branch)) => GitSelector::Branch(branch),
                    (Some(_), Some(_)) => {
                        return Err(intent_error(
                            ErrorKind::ArgumentConflict,
                            "--rev cannot be combined with --branch",
                        ));
                    }
                };
                let install = match repository {
                    None if !matches!(&selector, GitSelector::Default) || release || source => {
                        return Err(intent_error(
                            ErrorKind::MissingRequiredArgument,
                            "installation options require a Git remote",
                        ));
                    }
                    None => PluginInstallIntent::Declared,
                    Some(_) if release && source => {
                        return Err(intent_error(
                            ErrorKind::ArgumentConflict,
                            "--release cannot be combined with --source",
                        ));
                    }
                    Some(remote) if release => PluginInstallIntent::Release { remote, selector },
                    Some(remote) => PluginInstallIntent::Source { remote, selector },
                };
                Ok(PluginIntent::Install(install))
            }
            PluginCommand::Validate => Ok(PluginIntent::Validate),
            PluginCommand::List => Ok(PluginIntent::List),
            PluginCommand::Info { plugin_id } => Ok(PluginIntent::Info { plugin_id }),
            PluginCommand::Start { plugin_id } => Ok(PluginIntent::Start { plugin_id }),
            PluginCommand::Stop { plugin_id } => Ok(PluginIntent::Stop { plugin_id }),
            PluginCommand::Restart { plugin_id } => Ok(PluginIntent::Restart { plugin_id }),
            PluginCommand::Update { plugin_id } => Ok(PluginIntent::Update { plugin_id }),
            PluginCommand::Remove { plugin_id } => Ok(PluginIntent::Remove { plugin_id }),
            PluginCommand::Call {
                plugin_id,
                action,
                arguments,
            } => Ok(PluginIntent::Call {
                plugin_id,
                action,
                arguments,
            }),
        },
        (None, None) => Err(intent_error(
            ErrorKind::MissingSubcommand,
            "either a plugin subcommand or --log is required",
        )),
        (Some(_), Some(_)) => Err(intent_error(
            ErrorKind::ArgumentConflict,
            "--log cannot be combined with a plugin subcommand",
        )),
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
                let config_root = resolve_client_path(&args.config_root(environment)?, cwd);
                let log_dir = resolve_client_path(&args.log_dir(environment)?, cwd);
                ensure_persistable_path(&replica_root, "replica root")?;
                ensure_persistable_path(&log_dir, "log directory")?;
                Ok(PreparedCliIntent::Init(PreparedInitIntent {
                    node_name: args.node_name,
                    connect: args.connect,
                    listen: args.listen,
                    replica_root,
                    config_root,
                    log_dir,
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
                    ) => ClientDependency::None,
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
    List,
    Info { job_id: String },
    Stop { job_id: String },
}

impl From<JobCommand> for JobIntent {
    fn from(command: JobCommand) -> Self {
        match command {
            JobCommand::List => Self::List,
            JobCommand::Info { job_id } => Self::Info { job_id },
            JobCommand::Stop { job_id } => Self::Stop { job_id },
        }
    }
}
