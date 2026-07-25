use std::{
    env,
    ffi::OsString,
    fmt,
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    process::ExitCode,
    str::FromStr,
};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use url::Url;

pub const EXIT_UNAVAILABLE: u8 = 69;
pub const EXIT_CONFIG: u8 = 78;

const DEFAULT_CONFIG_SUFFIX: &str = ".config/oll/config.lua";
const DEFAULT_REPLICA_SUFFIX: &str = ".local/share/oll";
const ALL_PLUGIN_LOGS: &str = "__all__";

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

    pub fn execute_stage_gate(self, environment: &Environment) -> Result<(), CliError> {
        let (command, stage) = match self.command {
            Command::Init(args) => {
                let _ = args.replica_root(environment)?;
                let _ = environment.config_path()?;
                ("oll init", ImplementationStage::Replica)
            }
            Command::Run(args) => {
                let _ = args.config_path(environment)?;
                ("oll run", ImplementationStage::Node)
            }
            Command::Start => {
                let _ = environment.config_path()?;
                ("oll start", ImplementationStage::Node)
            }
            Command::Stop => {
                let _ = environment.config_path()?;
                ("oll stop", ImplementationStage::Node)
            }
            Command::Status(_) => {
                let _ = environment.config_path()?;
                ("oll status", ImplementationStage::Node)
            }
            Command::Replica(_) => {
                let _ = environment.replica_root()?;
                ("oll replica", ImplementationStage::Replica)
            }
            Command::Sync(_) => {
                let _ = environment.config_path()?;
                ("oll sync", ImplementationStage::Sync)
            }
            Command::Ping(_) => {
                let _ = environment.config_path()?;
                ("oll ping", ImplementationStage::Sync)
            }
            Command::Plugin(_) => {
                let _ = environment.config_path()?;
                ("oll plugin", ImplementationStage::PluginSystem)
            }
            Command::Job(_) => {
                let _ = environment.config_path()?;
                ("oll job", ImplementationStage::PluginSystem)
            }
        };

        Err(CliError::StageUnavailable { command, stage })
    }
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
    /// Test connectivity to a configured node.
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
}

impl InitArgs {
    pub fn replica_root(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.replica.as_ref(),
            environment.replica.as_ref(),
            environment.home.as_ref(),
            DEFAULT_REPLICA_SUFFIX,
            "replica root",
        )
    }
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Lua configuration file. Overrides OLL_CONFIG and the default path.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// Temporary socket address to listen on.
    #[arg(long, value_name = "ADDRESS")]
    pub listen: Option<SocketAddr>,
    /// Temporary peer URL to connect to. May be repeated.
    #[arg(long, value_name = "URL")]
    pub connect: Vec<ConnectUrl>,
}

impl RunArgs {
    pub fn config_path(&self, environment: &Environment) -> Result<PathBuf, CliError> {
        resolve_path(
            self.config.as_ref(),
            environment.config.as_ref(),
            environment.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config path",
        )
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
    /// Synchronize only with this configured node.
    #[arg(value_name = "NODE_NAME", conflicts_with = "log")]
    pub node_name: Option<String>,
    /// Maximum synchronization attempts.
    #[arg(short = 'n', long, conflicts_with = "log")]
    pub retries: Option<NonZeroU32>,
    /// View /var/log/oll/sync.log instead of synchronizing.
    #[arg(long)]
    pub log: bool,
}

#[derive(Debug, Args)]
pub struct PingArgs {
    pub node_name: String,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct PluginArgs {
    /// View all plugin logs, or only one plugin when an ID is supplied.
    #[arg(
        long,
        value_name = "PLUGIN_ID",
        num_args = 0..=1,
        default_missing_value = ALL_PLUGIN_LOGS
    )]
    pub log: Option<String>,
    #[command(subcommand)]
    pub command: Option<PluginCommand>,
}

impl PluginArgs {
    pub fn log_target(&self) -> Option<PluginLogTarget<'_>> {
        self.log.as_deref().map(|value| {
            if value == ALL_PLUGIN_LOGS {
                PluginLogTarget::All
            } else {
                PluginLogTarget::Plugin(value)
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginLogTarget<'a> {
    All,
    Plugin(&'a str),
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
    /// Invoke a plugin method and return a job ID.
    #[command(trailing_var_arg = true)]
    Call {
        /// Plugin to invoke.
        plugin_id: String,
        /// Plugin-defined method name.
        method: String,
        /// Opaque arguments forwarded to the plugin method.
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Environment {
    pub home: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub replica: Option<PathBuf>,
}

impl Environment {
    pub fn from_process() -> Self {
        Self {
            home: env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            config: env::var_os("OLL_CONFIG")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            replica: env::var_os("OLL_REPLICA")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
        }
    }

    pub fn config_path(&self) -> Result<PathBuf, CliError> {
        resolve_path(
            None,
            self.config.as_ref(),
            self.home.as_ref(),
            DEFAULT_CONFIG_SUFFIX,
            "config path",
        )
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImplementationStage {
    Node,
    Replica,
    Sync,
    PluginSystem,
}

impl fmt::Display for ImplementationStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Node => "node",
            Self::Replica => "replica",
            Self::Sync => "sync",
            Self::PluginSystem => "plugin system",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum CliError {
    MissingHome {
        name: &'static str,
    },
    StageUnavailable {
        command: &'static str,
        stage: ImplementationStage,
    },
}

impl CliError {
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Self::MissingHome { .. } => ExitCode::from(EXIT_CONFIG),
            Self::StageUnavailable { .. } => ExitCode::from(EXIT_UNAVAILABLE),
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
            Self::StageUnavailable { command, stage } => write!(
                formatter,
                "command `{command}` is unavailable until the {stage} stage is implemented"
            ),
        }
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

    #[test]
    fn clap_schema_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn resolves_path_precedence() {
        let environment = Environment {
            home: Some("/home/test".into()),
            config: Some("/env/config.lua".into()),
            replica: Some("/env/replica".into()),
        };
        let init = InitArgs {
            profile: None,
            connect: Vec::new(),
            listen: None,
            replica: Some("/cli/replica".into()),
        };
        let run = RunArgs {
            config: Some("/cli/config.lua".into()),
            listen: None,
            connect: Vec::new(),
        };

        assert_eq!(
            init.replica_root(&environment).unwrap(),
            PathBuf::from("/cli/replica")
        );
        assert_eq!(
            run.config_path(&environment).unwrap(),
            PathBuf::from("/cli/config.lua")
        );

        let init = InitArgs {
            replica: None,
            ..init
        };
        let run = RunArgs {
            config: None,
            ..run
        };
        assert_eq!(
            init.replica_root(&environment).unwrap(),
            PathBuf::from("/env/replica")
        );
        assert_eq!(
            run.config_path(&environment).unwrap(),
            PathBuf::from("/env/config.lua")
        );

        let environment = Environment {
            home: Some("/home/test".into()),
            ..Environment::default()
        };
        assert_eq!(
            init.replica_root(&environment).unwrap(),
            PathBuf::from("/home/test/.local/share/oll")
        );
        assert_eq!(
            run.config_path(&environment).unwrap(),
            PathBuf::from("/home/test/.config/oll/config.lua")
        );
    }

    #[test]
    fn parses_init_and_run_topologies() {
        for arguments in [
            vec!["oll", "init"],
            vec!["oll", "init", "--profile", "server"],
            vec![
                "oll",
                "init",
                "--profile",
                "server",
                "--listen",
                "127.0.0.1:7443",
            ],
            vec!["oll", "init", "--replica", "/path/to/replica/root"],
            vec!["oll", "run"],
            vec![
                "oll",
                "run",
                "--config",
                "/home/test/.config/oll/config.lua",
            ],
            vec!["oll", "run", "--listen", "127.0.0.1:7443"],
            vec!["oll", "run", "--connect", "https://oll.example.com"],
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
