use std::{
    env,
    ffi::OsString,
    fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use url::Url;

pub const EXIT_UNAVAILABLE: u8 = 69;
pub const EXIT_CONFIG: u8 = 78;

const DEFAULT_CONFIG_SUFFIX: &str = ".config/oll";
const CONFIG_FILENAME: &str = "config.lua";
const DEFAULT_REPLICA_SUFFIX: &str = ".local/share/oll";
const DEFAULT_LOG_SUFFIX: &str = ".local/state/oll";
const XDG_LOG_SUFFIX: &str = "oll";

#[derive(Debug, Parser)]
#[command(
    name = "oll",
    version,
    about = "CRDT-powered document library daemon",
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

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
                profile: args.profile,
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

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize the Lua configuration and the node's replica.
    Init(InitArgs),
    /// Run oll in the foreground.
    Run(RunArgs),
    /// Start oll in the background.
    Start,
    /// Gracefully stop oll and all child processes through the admin API.
    Stop,
    /// Show node and configured peer status.
    Status(StatusArgs),
    /// Inspect, import, and export replica state.
    Replica(ReplicaArgs),
    /// Synchronize with configured peers or inspect the sync log.
    Sync(SyncArgs),
    /// Test connectivity to a protocol-named node.
    Ping(PingArgs),
    /// Install and manage plugins.
    Plugin(PluginArgs),
    /// Inspect and stop plugin jobs.
    Job(JobArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Profile {
    Client,
    Server,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeName(String);

impl NodeName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for NodeName {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let bytes = input.as_bytes();
        if !(1..=63).contains(&bytes.len()) {
            return Err("node name must be between 1 and 63 bytes".to_owned());
        }

        let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if !is_alphanumeric(bytes[0]) || !is_alphanumeric(bytes[bytes.len() - 1]) {
            return Err(
                "node name must start and end with a lowercase ASCII letter or digit".to_owned(),
            );
        }
        if !bytes
            .iter()
            .all(|byte| is_alphanumeric(*byte) || *byte == b'-')
        {
            return Err(
                "node name may contain only lowercase ASCII letters, digits, and hyphens"
                    .to_owned(),
            );
        }

        Ok(Self(input.to_owned()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectUrl(Url);

impl ConnectUrl {
    pub fn as_url(&self) -> &Url {
        &self.0
    }
}

impl fmt::Display for ConnectUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConnectUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(input).map_err(|error| error.to_string())?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("connect URL scheme must be http or https".to_owned());
        }
        if url.host().is_none() {
            return Err("connect URL must include a host".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Debug, Args)]
pub struct InitArgs {
    /// Durable human-readable name paired with this node's generated NodeId.
    #[arg(value_name = "NODE_NAME")]
    pub node_name: NodeName,
    /// Optional initialization profile; it does not grant node authority.
    #[arg(long, value_enum)]
    pub profile: Option<Profile>,
    /// Peer URL to connect to. May be repeated.
    #[arg(long, value_name = "URL")]
    pub connect: Vec<ConnectUrl>,
    /// Socket address to listen on.
    #[arg(long, value_name = "ADDRESS")]
    pub listen: Option<SocketAddr>,
    /// Replica root. Overrides OLL_REPLICA and the default path.
    #[arg(long, value_name = "PATH")]
    pub replica: Option<PathBuf>,
    /// Configuration root containing config.lua. Overrides OLL_CONFIG.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// Log directory. Overrides OLL_LOG_DIR and the user-state default.
    #[arg(long, value_name = "PATH")]
    pub log_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Temporary replica root override. Takes precedence over OLL_REPLICA.
    #[arg(long, value_name = "PATH")]
    pub replica: Option<PathBuf>,
    /// Configuration root containing config.lua. Overrides OLL_CONFIG.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// Temporary log directory override. Takes precedence over OLL_LOG_DIR.
    #[arg(long, value_name = "PATH")]
    pub log_dir: Option<PathBuf>,
    /// Temporary socket address to listen on.
    #[arg(long, value_name = "ADDRESS")]
    pub listen: Option<SocketAddr>,
    /// Temporary peer URL to connect to. May be repeated.
    #[arg(long, value_name = "URL")]
    pub connect: Vec<ConnectUrl>,
    /// Internal readiness callback used by `oll start`.
    #[arg(long, value_name = "ADDRESS", hide = true)]
    pub pingback: Option<LoopbackAddr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoopbackAddr(SocketAddr);

impl LoopbackAddr {
    pub fn as_socket_addr(self) -> SocketAddr {
        self.0
    }
}

impl FromStr for LoopbackAddr {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let address = input
            .parse::<SocketAddr>()
            .map_err(|error| error.to_string())?;
        if !address.ip().is_loopback() {
            return Err("pingback address must use a loopback IP".to_owned());
        }
        Ok(Self(address))
    }
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReplicaArgs {
    #[command(subcommand)]
    pub command: ReplicaCommand,
}

#[derive(Debug, Subcommand)]
pub enum ReplicaCommand {
    /// Inspect one document's current state.
    Inspect { document: PathBuf },
    /// Show one document's CRDT operation history.
    Ops {
        document: PathBuf,
        #[arg(long)]
        limit: Option<NonZeroUsize>,
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,
    },
    /// Export the complete replica snapshot.
    Export {
        #[arg(short = 'o', long, value_name = "PATH")]
        output: PathBuf,
    },
    /// Import a complete replica snapshot.
    Import { snapshot: PathBuf },
    /// Inspect or verify a replica snapshot file.
    Snapshot(SnapshotArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Args)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub command: SnapshotCommand,
}

#[derive(Debug, Subcommand)]
pub enum SnapshotCommand {
    /// Show snapshot metadata without importing it.
    Inspect {
        snapshot: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Verify snapshot structure and checksums.
    Verify { snapshot: PathBuf },
}

#[derive(Debug, Args)]
pub struct SyncArgs {
    /// Synchronize only with this protocol-named node.
    #[arg(value_name = "NODE_NAME", conflicts_with = "log")]
    pub node_name: Option<NodeName>,
    /// Maximum total attempts, including the initial synchronization attempt.
    #[arg(short = 'n', long, conflicts_with = "log")]
    pub retries: Option<NonZeroU32>,
    /// View sync.log from the user log directory instead of synchronizing.
    #[arg(long)]
    pub log: bool,
}

#[derive(Debug, Args)]
pub struct PingArgs {
    /// Protocol-declared name of the node to ping.
    #[arg(value_name = "NODE_NAME")]
    pub node_name: NodeName,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct PluginArgs {
    /// View all plugin logs, or only one plugin when an ID is supplied.
    #[arg(
        long,
        value_name = "PLUGIN_ID",
        num_args = 0..=1
    )]
    pub log: Option<Option<String>>,
    #[command(subcommand)]
    pub command: Option<PluginCommand>,
}

#[derive(Debug, Subcommand)]
pub enum PluginCommand {
    /// Install a plugin from a Git repository.
    Install {
        /// Git repository URL.
        repository: Url,
        /// Checkout this exact Git revision or tag.
        #[arg(long, conflicts_with = "branch")]
        rev: Option<String>,
        /// Checkout the head of this branch.
        #[arg(long, conflicts_with = "rev")]
        branch: Option<String>,
        /// Download a release binary instead of building from source.
        #[arg(long, conflicts_with = "source")]
        release: bool,
        /// Build from source. This is the default installation mode.
        #[arg(long, conflicts_with = "release")]
        source: bool,
    },
    /// List installed plugins.
    List,
    /// Show one plugin's metadata and state.
    Info { plugin_id: String },
    /// Start a plugin process.
    Start { plugin_id: String },
    /// Gracefully stop a plugin process.
    Stop { plugin_id: String },
    /// Gracefully stop and start a plugin process.
    Restart { plugin_id: String },
    /// Update an installed plugin.
    Update { plugin_id: String },
    /// Remove an installed plugin.
    Remove { plugin_id: String },
    /// Invoke a plugin action and return a job ID.
    #[command(trailing_var_arg = true)]
    Call {
        /// Plugin to invoke.
        plugin_id: String,
        /// Plugin-defined action name.
        action: String,
        /// Shell-style UTF-8 arguments forwarded in order to the plugin action.
        #[arg(allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
}

#[derive(Debug, Args)]
pub struct JobArgs {
    #[command(subcommand)]
    pub command: JobCommand,
}

#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List plugin jobs.
    List,
    /// Show one plugin job.
    Info { job_id: String },
    /// Gracefully stop the plugin process that owns a job.
    Stop { job_id: String },
}

#[derive(Debug, PartialEq)]
pub enum CliIntent {
    Init(InitIntent),
    Run(RunIntent),
    Start,
    Stop,
    Status { json: bool },
    Replica(ReplicaIntent),
    Sync(SyncIntent),
    Ping { node_name: NodeName },
    Plugin(PluginIntent),
    Job(JobIntent),
}

#[derive(Debug, PartialEq)]
pub struct InitIntent {
    pub node_name: NodeName,
    pub profile: Option<Profile>,
    pub connect: Vec<ConnectUrl>,
    pub listen: Option<SocketAddr>,
    pub replica_root: Option<PathBuf>,
    pub config_root: Option<PathBuf>,
    pub log_dir: Option<PathBuf>,
}

impl InitIntent {
    pub fn replica_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.replica_root.as_ref(),
            environment.replica.as_ref(),
            environment.home.as_ref(),
            DEFAULT_REPLICA_SUFFIX,
            "replica root",
        )
    }

    pub fn config_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.config_root.as_ref(),
            environment.config.as_ref(),
            environment.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
    }

    pub fn config_path(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        self.config_root(environment)
            .map(|root| root.join(CONFIG_FILENAME))
    }

    pub fn log_dir(&self, environment: &Environment) -> Result<PathBuf, CliError> {
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
    pub fn replica_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.replica_root.as_ref(),
            environment.replica.as_ref(),
            environment.home.as_ref(),
            DEFAULT_REPLICA_SUFFIX,
            "replica root",
        )
    }

    pub fn config_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.config_root.as_ref(),
            environment.config.as_ref(),
            environment.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
    }

    pub fn config_path(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        self.config_root(environment)
            .map(|root| root.join(CONFIG_FILENAME))
    }

    pub fn log_dir(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_log_dir(self.log_dir.as_ref(), environment)
    }
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
pub enum GitSelector {
    Default,
    Revision(String),
    Branch(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallMode {
    Source,
    Release,
}

#[derive(Debug, PartialEq)]
pub enum PluginLogTarget {
    All,
    Plugin(String),
}

#[derive(Debug, PartialEq)]
pub enum PluginIntent {
    Install {
        repository: Url,
        selector: GitSelector,
        mode: InstallMode,
    },
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
                let mode = match (release, source) {
                    (false, _) => InstallMode::Source,
                    (true, false) => InstallMode::Release,
                    (true, true) => {
                        return Err(intent_error(
                            ErrorKind::ArgumentConflict,
                            "--release cannot be combined with --source",
                        ));
                    }
                };
                Ok(PluginIntent::Install {
                    repository,
                    selector,
                    mode,
                })
            }
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
    pub fn confirmation_requirements(&self) -> &'static [ConfirmationRequirement] {
        match self {
            Self::Replica(ReplicaIntent::Import { .. }) => &[
                ConfirmationRequirement::ReplicaBackupCreated,
                ConfirmationRequirement::ReplicaReplacementApproved,
            ],
            _ => &[],
        }
    }

    pub fn resolve_client_paths_from_process(&mut self) -> Result<(), CliError> {
        if !matches!(self, Self::Replica(_)) {
            return Ok(());
        }

        let cwd =
            env::current_dir().map_err(|error| CliError::CurrentDirectory(error.to_string()))?;
        self.resolve_client_paths(&cwd);
        Ok(())
    }

    /// Resolve client OS paths without checking the filesystem or normalizing
    /// `.` and `..` segments. Operation handlers apply their own validation.
    pub fn resolve_client_paths(&mut self, cwd: &Path) {
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

    pub fn validate_environment(&self, environment: &Environment) -> Result<(), CliError> {
        match self {
            Self::Init(args) => {
                let _ = args.replica_root(environment)?;
                let _ = args.config_path(environment)?;
                let _ = args.log_dir(environment)?;
            }
            Self::Run(args) => {
                let _ = args.replica_root(environment)?;
                let _ = args.config_path(environment)?;
                let _ = args.log_dir(environment)?;
            }
            Self::Start | Self::Stop | Self::Status { .. } | Self::Ping { .. } | Self::Job(_) => {
                let _ = environment.config_path()?;
            }
            Self::Replica(
                ReplicaIntent::Inspect { .. }
                | ReplicaIntent::Ops { .. }
                | ReplicaIntent::Export { .. }
                | ReplicaIntent::Import { .. },
            )
            | Self::Sync(SyncIntent::Synchronize { .. }) => {
                let _ = environment.config_path()?;
            }
            Self::Replica(
                ReplicaIntent::SnapshotInspect { .. } | ReplicaIntent::SnapshotVerify { .. },
            ) => {}
            Self::Sync(SyncIntent::ViewLog) | Self::Plugin(PluginIntent::ViewLog { .. }) => {
                let _ = environment.log_dir()?;
            }
            Self::Plugin(_) => {
                let _ = environment.config_path()?;
            }
        }

        Ok(())
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

    pub fn config_root(&self) -> Result<PathBuf, CliError> {
        resolve_path(
            None,
            self.config.as_ref(),
            self.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config root",
        )
    }

    pub fn config_path(&self) -> Result<PathBuf, CliError> {
        self.config_root().map(|root| root.join(CONFIG_FILENAME))
    }

    pub fn replica_root(&self) -> Result<PathBuf, CliError> {
        resolve_path(
            None,
            self.replica.as_ref(),
            self.home.as_ref(),
            DEFAULT_REPLICA_SUFFIX,
            "replica root",
        )
    }

    pub fn log_dir(&self) -> Result<PathBuf, CliError> {
        resolve_log_dir(None, self)
    }
}

fn resolve_log_dir(cli: Option<&PathBuf>, environment: &Environment) -> Result<PathBuf, CliError> {
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

fn resolve_path(
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

#[derive(Debug, Eq, PartialEq)]
pub enum CliError {
    MissingHome { name: &'static str },
    CurrentDirectory(String),
}

impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::MissingHome { .. } => ExitCode::from(EXIT_CONFIG),
            Self::CurrentDirectory(_) => ExitCode::from(EXIT_CONFIG),
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
            Self::CurrentDirectory(error) => {
                write!(
                    formatter,
                    "cannot determine client working directory: {error}"
                )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Cli {
        parse_from(arguments).unwrap()
    }

    fn intent(arguments: &[&str]) -> CliIntent {
        parse(arguments).into_intent().unwrap()
    }

    #[test]
    fn clap_schema_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn resolves_path_precedence() {
        let environment = Environment {
            home: Some("/home/test".into()),
            state_home: Some("/state/test".into()),
            config: Some("/env/config".into()),
            replica: Some("/env/replica".into()),
            log_dir: Some("/env/log".into()),
        };
        let CliIntent::Init(init) = intent(&[
            "oll",
            "init",
            "test-node",
            "--replica",
            "/cli/replica",
            "--config",
            "/cli/config",
            "--log-dir",
            "/cli/log",
        ]) else {
            panic!()
        };

        assert_eq!(
            init.replica_root(&environment).unwrap(),
            Path::new("/cli/replica")
        );
        assert_eq!(
            init.config_root(&environment).unwrap(),
            Path::new("/cli/config")
        );
        assert_eq!(
            init.config_path(&environment).unwrap(),
            Path::new("/cli/config/config.lua")
        );
        assert_eq!(init.log_dir(&environment).unwrap(), Path::new("/cli/log"));

        let CliIntent::Run(run) = intent(&["oll", "run"]) else {
            panic!()
        };
        assert_eq!(
            run.replica_root(&environment).unwrap(),
            Path::new("/env/replica")
        );
        assert_eq!(
            run.config_root(&environment).unwrap(),
            Path::new("/env/config")
        );
        assert_eq!(
            run.config_path(&environment).unwrap(),
            Path::new("/env/config/config.lua")
        );
        assert_eq!(run.log_dir(&environment).unwrap(), Path::new("/env/log"));

        let environment = Environment {
            home: Some("/home/test".into()),
            state_home: Some("/state/test".into()),
            ..Environment::default()
        };
        assert_eq!(
            run.replica_root(&environment).unwrap(),
            Path::new("/home/test/.local/share/oll")
        );
        assert_eq!(
            run.config_root(&environment).unwrap(),
            Path::new("/home/test/.config/oll")
        );
        assert_eq!(
            run.config_path(&environment).unwrap(),
            Path::new("/home/test/.config/oll/config.lua")
        );
        assert_eq!(
            run.log_dir(&environment).unwrap(),
            Path::new("/state/test/oll")
        );

        let environment = Environment {
            home: Some("/home/test".into()),
            ..Environment::default()
        };
        assert_eq!(
            run.log_dir(&environment).unwrap(),
            Path::new("/home/test/.local/state/oll")
        );
    }

    #[test]
    fn parses_init_and_run_topologies() {
        for arguments in [
            vec!["oll", "init", "test-node"],
            vec!["oll", "init", "test-node", "--profile", "server"],
            vec![
                "oll",
                "init",
                "test-node",
                "--profile",
                "server",
                "--listen",
                "127.0.0.1:7443",
            ],
            vec![
                "oll",
                "init",
                "test-node",
                "--replica",
                "/path/to/replica/root",
            ],
            vec![
                "oll",
                "init",
                "test-node",
                "--config",
                "/path/to/config/root",
            ],
            vec!["oll", "init", "test-node", "--log-dir", "/path/to/log/dir"],
            vec![
                "oll",
                "init",
                "test-node",
                "--listen",
                "127.0.0.1:7443",
                "--connect",
                "https://oll.example.com",
            ],
            vec!["oll", "run"],
            vec!["oll", "run", "--replica", "/path/to/replica/root"],
            vec!["oll", "run", "--config", "/path/to/config/root"],
            vec!["oll", "run", "--log-dir", "/path/to/log/dir"],
            vec!["oll", "run", "--listen", "127.0.0.1:7443"],
            vec!["oll", "run", "--connect", "https://oll.example.com"],
            vec![
                "oll",
                "run",
                "--listen",
                "127.0.0.1:7443",
                "--connect",
                "https://oll.example.com",
            ],
            vec!["oll", "start"],
            vec!["oll", "stop"],
            vec!["oll", "status"],
            vec!["oll", "status", "--json"],
        ] {
            parse(&arguments);
        }

        let cli = parse(&[
            "oll",
            "init",
            "test-node",
            "--profile",
            "client",
            "--connect",
            "https://oll.example.com",
            "--listen",
            "127.0.0.1:7443",
        ]);
        let Command::Init(args) = cli.command else {
            panic!()
        };
        assert_eq!(args.profile, Some(Profile::Client));
        assert_eq!(args.node_name.as_str(), "test-node");
        assert_eq!(args.connect.len(), 1);
        assert_eq!(args.listen, Some("127.0.0.1:7443".parse().unwrap()));

        let cli = parse(&[
            "oll",
            "run",
            "--listen",
            "127.0.0.1:7443",
            "--connect",
            "https://node-a.example.com",
            "--connect",
            "https://node-b.example.com",
        ]);
        let Command::Run(args) = cli.command else {
            panic!()
        };
        assert_eq!(args.connect.len(), 2);
        assert!(parse_from(["oll", "run", "--profile", "server"]).is_err());
    }

    #[test]
    fn validates_node_names_as_lowercase_dns_labels() {
        let name: NodeName = "home-server-2".parse().unwrap();
        assert_eq!(name.as_str(), "home-server-2");
        assert_eq!(name.to_string(), "home-server-2");

        for invalid in [
            "",
            "Home",
            "-home",
            "home-",
            "home_server",
            "home.example",
            "node name",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(invalid.parse::<NodeName>().is_err(), "accepted {invalid:?}");
        }

        assert!(parse_from(["oll", "init"]).is_err());
        assert!(parse_from(["oll", "init", "Home"]).is_err());
        assert!(parse_from(["oll", "sync", "home.example"]).is_err());
        assert!(parse_from(["oll", "ping", "node name"]).is_err());
    }

    #[test]
    fn parses_replica_commands() {
        for arguments in [
            vec!["oll", "replica", "inspect", "/note.md"],
            vec!["oll", "replica", "ops", "/note.md", "--limit", "50"],
            vec!["oll", "replica", "ops", "/note.md", "--format", "json"],
            vec!["oll", "replica", "export", "-o", "replica.snapshot"],
            vec!["oll", "replica", "import", "replica.snapshot"],
            vec![
                "oll",
                "replica",
                "snapshot",
                "inspect",
                "replica.snapshot",
                "--json",
            ],
            vec!["oll", "replica", "snapshot", "verify", "replica.snapshot"],
        ] {
            parse(&arguments);
        }
    }

    #[test]
    fn resolves_replica_paths_against_client_cwd_without_canonicalizing() {
        let mut intent = parse(&[
            "oll",
            "replica",
            "snapshot",
            "inspect",
            "nested/../backup.ollsnap",
            "--json",
        ])
        .into_intent()
        .unwrap();
        intent.resolve_client_paths(Path::new("/client/work"));
        let CliIntent::Replica(ReplicaIntent::SnapshotInspect { snapshot, .. }) = intent else {
            panic!()
        };
        assert_eq!(
            snapshot,
            PathBuf::from("/client/work/nested/../backup.ollsnap")
        );

        let mut intent = parse(&["oll", "replica", "inspect", "/replica/../note.md"])
            .into_intent()
            .unwrap();
        intent.resolve_client_paths(Path::new("/different/client"));
        let CliIntent::Replica(ReplicaIntent::Inspect { document }) = intent else {
            panic!()
        };
        assert_eq!(document, PathBuf::from("/replica/../note.md"));
    }

    #[test]
    fn resolves_every_snapshot_and_document_path_kind() {
        for (arguments, expected) in [
            (
                vec!["oll", "replica", "inspect", "note.md"],
                PathBuf::from("/client/note.md"),
            ),
            (
                vec!["oll", "replica", "ops", "history.md"],
                PathBuf::from("/client/history.md"),
            ),
            (
                vec!["oll", "replica", "export", "-o", "out.ollsnap"],
                PathBuf::from("/client/out.ollsnap"),
            ),
            (
                vec!["oll", "replica", "import", "in.ollsnap"],
                PathBuf::from("/client/in.ollsnap"),
            ),
            (
                vec!["oll", "replica", "snapshot", "verify", "verify.ollsnap"],
                PathBuf::from("/client/verify.ollsnap"),
            ),
        ] {
            let mut intent = parse(&arguments).into_intent().unwrap();
            intent.resolve_client_paths(Path::new("/client"));
            let CliIntent::Replica(intent) = intent else {
                panic!()
            };
            let path = match intent {
                ReplicaIntent::Inspect { document } | ReplicaIntent::Ops { document, .. } => {
                    document
                }
                ReplicaIntent::Export { output } => output,
                ReplicaIntent::Import { snapshot }
                | ReplicaIntent::SnapshotInspect { snapshot, .. }
                | ReplicaIntent::SnapshotVerify { snapshot } => snapshot,
            };
            assert_eq!(path, expected);
        }
    }

    #[test]
    fn parses_sync_commands() {
        for arguments in [
            vec!["oll", "sync"],
            vec!["oll", "sync", "node-a"],
            vec!["oll", "sync", "-n", "3"],
            vec!["oll", "sync", "node-a", "-n", "3"],
            vec!["oll", "sync", "--log"],
            vec!["oll", "ping", "node-a"],
        ] {
            parse(&arguments);
        }

        let cli = parse(&["oll", "sync", "node-a", "-n", "3"]);
        let Command::Sync(args) = cli.command else {
            panic!()
        };
        assert_eq!(args.node_name.unwrap().as_str(), "node-a");

        let cli = parse(&["oll", "ping", "node-a"]);
        let Command::Ping(args) = cli.command else {
            panic!()
        };
        assert_eq!(args.node_name.as_str(), "node-a");
    }

    #[test]
    fn converts_sync_modes_to_distinct_intents() {
        assert_eq!(
            intent(&["oll", "sync", "--log"]),
            CliIntent::Sync(SyncIntent::ViewLog)
        );

        let CliIntent::Sync(SyncIntent::Synchronize {
            node_name,
            max_attempts,
        }) = intent(&["oll", "sync", "node-a", "--retries", "3"])
        else {
            panic!()
        };
        assert_eq!(node_name.unwrap().as_str(), "node-a");
        assert_eq!(max_attempts.unwrap().get(), 3);
    }

    #[test]
    fn parses_plugin_and_job_commands() {
        for arguments in [
            vec![
                "oll",
                "plugin",
                "install",
                "https://github.com/example/oll-anki.git",
            ],
            vec![
                "oll",
                "plugin",
                "install",
                "https://github.com/example/oll-anki.git",
                "--rev",
                "v0.3.1",
            ],
            vec![
                "oll",
                "plugin",
                "install",
                "https://github.com/example/oll-anki.git",
                "--branch",
                "main",
                "--source",
            ],
            vec![
                "oll",
                "plugin",
                "install",
                "https://github.com/example/oll-anki.git",
                "--release",
            ],
            vec![
                "oll",
                "plugin",
                "install",
                "https://github.com/example/oll-anki.git",
                "--source",
            ],
            vec!["oll", "plugin", "list"],
            vec!["oll", "plugin", "info", "oll.anki"],
            vec!["oll", "plugin", "start", "oll.anki"],
            vec!["oll", "plugin", "stop", "oll.anki"],
            vec!["oll", "plugin", "restart", "oll.anki"],
            vec!["oll", "plugin", "update", "oll.anki"],
            vec!["oll", "plugin", "remove", "oll.anki"],
            vec!["oll", "plugin", "--log"],
            vec!["oll", "plugin", "--log", "oll.anki"],
            vec![
                "oll", "plugin", "call", "oll.anki", "create", "--deck", "default",
            ],
            vec!["oll", "job", "list"],
            vec!["oll", "job", "info", "job-1"],
            vec!["oll", "job", "stop", "job-1"],
        ] {
            parse(&arguments);
        }
    }

    #[test]
    fn preserves_plugin_action_argv_verbatim() {
        let call_intent = intent(&[
            "oll",
            "plugin",
            "call",
            "oll.example",
            "publish",
            "",
            "--flag",
            "--flag",
            "value",
        ]);
        let CliIntent::Plugin(PluginIntent::Call {
            plugin_id,
            action,
            arguments,
        }) = call_intent
        else {
            panic!()
        };
        assert_eq!(plugin_id, "oll.example");
        assert_eq!(action, "publish");
        assert_eq!(arguments, vec!["", "--flag", "--flag", "value"]);

        let health_intent = intent(&["oll", "plugin", "call", "oll.example", "health"]);
        let CliIntent::Plugin(PluginIntent::Call { arguments, .. }) = health_intent else {
            panic!()
        };
        assert!(arguments.is_empty());
    }

    #[test]
    fn converts_plugin_modes_without_sentinels_or_boolean_state() {
        assert_eq!(
            intent(&["oll", "plugin", "--log"]),
            CliIntent::Plugin(PluginIntent::ViewLog {
                target: PluginLogTarget::All,
            })
        );
        assert_eq!(
            intent(&["oll", "plugin", "--log", "__all__"]),
            CliIntent::Plugin(PluginIntent::ViewLog {
                target: PluginLogTarget::Plugin("__all__".to_owned()),
            })
        );

        let CliIntent::Plugin(PluginIntent::Install { selector, mode, .. }) = intent(&[
            "oll",
            "plugin",
            "install",
            "https://example.com/plugin.git",
            "--branch",
            "main",
            "--release",
        ]) else {
            panic!()
        };
        assert_eq!(selector, GitSelector::Branch("main".to_owned()));
        assert_eq!(mode, InstallMode::Release);

        let CliIntent::Plugin(PluginIntent::Install { selector, mode, .. }) =
            intent(&["oll", "plugin", "install", "https://example.com/plugin.git"])
        else {
            panic!()
        };
        assert_eq!(selector, GitSelector::Default);
        assert_eq!(mode, InstallMode::Source);
    }

    #[test]
    fn intent_whitelist_rejects_invalid_programmatic_states() {
        let sync = Cli {
            command: Command::Sync(SyncArgs {
                node_name: Some("node-a".parse().unwrap()),
                retries: None,
                log: true,
            }),
        };
        assert!(sync.into_intent().is_err());

        let install = Cli {
            command: Command::Plugin(PluginArgs {
                log: None,
                command: Some(PluginCommand::Install {
                    repository: "https://example.com/plugin.git".parse().unwrap(),
                    rev: Some("v1".to_owned()),
                    branch: Some("main".to_owned()),
                    release: true,
                    source: true,
                }),
            }),
        };
        assert!(install.into_intent().is_err());

        let plugin = Cli {
            command: Command::Plugin(PluginArgs {
                log: Some(None),
                command: Some(PluginCommand::List),
            }),
        };
        assert!(plugin.into_intent().is_err());
    }

    #[test]
    fn environment_dependencies_follow_the_concrete_intent() {
        let empty = Environment::default();
        for arguments in [
            vec!["oll", "replica", "snapshot", "inspect", "file.ollsnap"],
            vec!["oll", "replica", "snapshot", "verify", "file.ollsnap"],
        ] {
            intent(&arguments).validate_environment(&empty).unwrap();
        }

        let log_environment = Environment {
            log_dir: Some("/logs".into()),
            ..Environment::default()
        };
        for arguments in [vec!["oll", "sync", "--log"], vec!["oll", "plugin", "--log"]] {
            intent(&arguments)
                .validate_environment(&log_environment)
                .unwrap();
        }

        for arguments in [
            vec!["oll", "replica", "inspect", "/note.md"],
            vec!["oll", "replica", "export", "--output", "file.ollsnap"],
            vec!["oll", "replica", "import", "file.ollsnap"],
            vec!["oll", "sync", "node-a"],
            vec!["oll", "sync", "--log"],
            vec!["oll", "plugin", "list"],
            vec!["oll", "plugin", "--log"],
        ] {
            assert!(intent(&arguments).validate_environment(&empty).is_err());
        }
    }

    #[test]
    fn replica_import_requires_backup_and_replacement_confirmations() {
        let import_intent = intent(&["oll", "replica", "import", "file.ollsnap"]);
        assert_eq!(
            import_intent.confirmation_requirements(),
            [
                ConfirmationRequirement::ReplicaBackupCreated,
                ConfirmationRequirement::ReplicaReplacementApproved,
            ]
        );
        assert_eq!(
            import_intent.confirmation_requirements()[0].prompt(),
            "Have you exported the current replica to a backup snapshot?"
        );
        assert_eq!(
            import_intent.confirmation_requirements()[1].prompt(),
            "Import replaces the entire current replica. Continue?"
        );
        assert!(
            intent(&["oll", "replica", "snapshot", "verify", "file.ollsnap"])
                .confirmation_requirements()
                .is_empty()
        );
    }

    #[test]
    fn rejects_conflicting_modes() {
        assert!(parse_from(["oll", "sync", "node-a", "--log"]).is_err());
        assert!(
            parse_from([
                "oll",
                "plugin",
                "install",
                "https://example.com/plugin.git",
                "--release",
                "--source",
            ])
            .is_err()
        );
        assert!(
            parse_from([
                "oll",
                "plugin",
                "install",
                "https://example.com/plugin.git",
                "--rev",
                "v1",
                "--branch",
                "main",
            ])
            .is_err()
        );
        assert!(parse_from(["oll", "plugin"]).is_err());
    }
}
