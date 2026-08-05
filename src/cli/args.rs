use std::{
    net::{IpAddr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand};
use url::Url;

use super::{
    ColorMode, ConnectUrl, GitRemote, InitStore, LogFilterDirective, LoopbackAddr, NodeName,
    OutputFormat,
};

const DEFAULT_SYNC_PORT: u16 = 17_384;

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

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize Lua configuration, node identity, and the replica slot.
    Init(InitArgs),
    /// Run oll in the foreground.
    Run(RunArgs),
    /// Start oll in the background.
    Start,
    /// Gracefully stop oll and all child processes through the admin API.
    Stop,
    /// Show node and configured peer status.
    Status(StatusArgs),
    /// Inspect and adjust local daemon log filters.
    Log(LogArgs),
    /// Inspect, import, and export replica state.
    Replica(ReplicaArgs),
    /// Synchronize with configured peers or inspect the sync log.
    Sync(SyncArgs),
    /// Test connectivity to a protocol-named node.
    Ping(PingArgs),
    /// Generate a printable network key on stdout.
    Psk,
    /// Install and manage plugins.
    Plugin(PluginArgs),
    /// Inspect and stop plugin jobs.
    Job(JobArgs),
}

#[derive(Debug, Args)]
pub struct InitArgs {
    /// Durable human-readable name paired with this node's generated NodeId.
    #[arg(value_name = "NODE_NAME")]
    pub node_name: NodeName,
    /// Peer URL to connect to. May be repeated.
    #[arg(long, value_name = "HOST[:PORT]", value_parser = parse_init_connect_url)]
    pub connect: Vec<ConnectUrl>,
    /// Socket address to listen on.
    #[arg(long, value_name = "ADDRESS", value_parser = parse_listen_address)]
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
    /// SQL store backend written to config.lua.
    #[arg(long, value_enum, default_value = "sqlite")]
    pub store: InitStore,
}

fn parse_init_connect_url(input: &str) -> Result<ConnectUrl, String> {
    let normalized = if input.contains("://") {
        input.to_owned()
    } else {
        let authority = match input.parse::<IpAddr>() {
            Ok(IpAddr::V6(_)) => format!("[{input}]"),
            _ => input.to_owned(),
        };
        format!("oll://{authority}")
    };
    let mut url = Url::parse(&normalized).map_err(|error| error.to_string())?;
    if url.port().is_none() {
        url.set_port(Some(DEFAULT_SYNC_PORT))
            .map_err(|()| "connect URL cannot accept a port".to_owned())?;
    }
    url.as_str().parse()
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Colorize the foreground operator view when supported.
    #[arg(long, value_enum, default_value = "auto")]
    pub color: ColorMode,
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
    #[arg(long, value_name = "ADDRESS", value_parser = parse_listen_address)]
    pub listen: Option<SocketAddr>,
    /// Temporary peer URL to connect to. May be repeated.
    #[arg(long, value_name = "URL")]
    pub connect: Vec<ConnectUrl>,
    /// Internal readiness callback used by `oll start`.
    #[arg(long, value_name = "ADDRESS", hide = true)]
    pub pingback: Option<LoopbackAddr>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct LogArgs {
    #[command(subcommand)]
    pub command: LogCommand,
}

#[derive(Debug, Subcommand)]
pub enum LogCommand {
    /// Set one live log target filter.
    Set {
        #[arg(value_name = "TARGET=LEVEL")]
        directive: LogFilterDirective,
    },
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
    /// Show one document's high-level operation history.
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

fn parse_listen_address(value: &str) -> Result<SocketAddr, String> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| "listen address must be a socket address".to_owned())?;
    if address.port() == 0 {
        return Err("listen address must use a nonzero port".to_owned());
    }
    Ok(address)
}

#[derive(Debug, Args)]
pub struct PluginArgs {
    #[command(subcommand)]
    pub command: PluginCommand,
}

#[derive(Debug, Subcommand)]
pub enum PluginCommand {
    /// Install a plugin from a Git repository.
    Install {
        /// Git remote. Omit it to install declarations from plugins.lua.
        #[arg(value_name = "GIT_REMOTE")]
        repository: Option<GitRemote>,
        /// Checkout this exact Git revision.
        #[arg(long, conflicts_with = "branch", requires = "repository")]
        rev: Option<String>,
        /// Checkout the head of this branch.
        #[arg(long, conflicts_with = "rev", requires = "repository")]
        branch: Option<String>,
        /// Install this exact opaque release ID from oll-release.json.
        #[arg(
            long,
            value_name = "RELEASE_ID",
            conflicts_with = "source",
            requires = "repository"
        )]
        release: Option<String>,
        /// Build from source. This is the default installation mode.
        #[arg(long, conflicts_with = "release", requires = "repository")]
        source: bool,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Make installed plugins exactly match plugins.lua.
    Reconcile {
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Validate the data-only plugins.lua configuration.
    Validate,
    /// List installed plugins.
    List {
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Show one plugin's metadata and state.
    Info {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// List publisher-defined opaque release IDs.
    Releases {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Start a plugin process.
    Start {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
    },
    /// Gracefully stop a plugin process.
    Stop {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
    },
    /// Gracefully stop and start a plugin process.
    Restart {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
    },
    /// Update an installed plugin.
    Update {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Remove an installed plugin.
    Remove {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// View all plugin logs, or only one plugin when a selector is supplied.
    Log {
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: Option<String>,
    },
    /// Invoke a plugin action and return a job ID.
    #[command(trailing_var_arg = true)]
    Call {
        /// Idempotency key. Omit it to generate one locally.
        #[arg(long, value_name = "OPERATION_ID")]
        operation_id: Option<String>,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
        /// Plugin to invoke.
        #[arg(value_name = "PLUGIN_ID_OR_NAME")]
        selector: String,
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
    List {
        /// Maximum newest-first rows to return.
        #[arg(
            long,
            default_value_t = 100,
            value_parser = clap::value_parser!(u16).range(1..=1000)
        )]
        limit: u16,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Show one plugin job.
    Info {
        job_id: String,
        /// Emit one stable machine-readable JSON document.
        #[arg(long)]
        json: bool,
    },
    /// Cancel one job without stopping its plugin process.
    Stop { job_id: String },
}
