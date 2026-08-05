use std::{
    io::{self, IsTerminal, Write},
    path::Path,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use getrandom::fill as fill_random;

use crate::{
    cli::{
        CliIntent, ClientDependency, JobIntent, LogIntent, PreparedCliIntent, PreparedClientIntent,
        ReplicaIntent, SyncIntent,
    },
    node::init::{self, InitResult},
};

use super::{
    NodeError,
    blocking::in_runtime,
    client::{
        confirm_replica_import, export_replica, import_replica, inspect_replica_document,
        ping_peer, set_log_filter, show_replica_operations, show_snapshot_inspection, show_status,
        synchronize_peers, verify_local_snapshot,
    },
    daemon::run_daemon,
    launcher::{start, stop},
    plugin_cli,
};

pub fn execute(intent: PreparedCliIntent) -> Result<(), NodeError> {
    match intent {
        PreparedCliIntent::Init(intent) => match init::initialize(intent)? {
            InitResult::Initialized(identity) => {
                println!(
                    "initialized node {} ({})",
                    identity.node_name(),
                    identity.node_id()
                );
                Ok(())
            }
            InitResult::Cancelled => Ok(()),
        },
        PreparedCliIntent::Run(intent) => run_daemon(intent),
        PreparedCliIntent::Client(intent) => execute_client(intent),
    }
}

fn execute_client(intent: PreparedClientIntent) -> Result<(), NodeError> {
    match (intent.intent, intent.dependency) {
        (CliIntent::Psk, ClientDependency::None) => {
            let mut key = [0_u8; 32];
            fill_random(&mut key).map_err(|error| {
                NodeError::Internal(format!("cannot generate network key: {error}"))
            })?;
            let encoded = URL_SAFE_NO_PAD.encode(key);
            let stdout = io::stdout();
            let terminal = stdout.is_terminal();
            let mut stdout = stdout.lock();
            stdout
                .write_all(encoded.as_bytes())
                .and_then(|()| {
                    if terminal {
                        stdout.write_all(b"\n")
                    } else {
                        Ok(())
                    }
                })
                .and_then(|()| stdout.flush())
                .map_err(|error| NodeError::io("write network key", error))
        }
        (
            CliIntent::Replica(ReplicaIntent::SnapshotInspect { snapshot, json }),
            ClientDependency::None,
        ) => show_snapshot_inspection(&snapshot, json),
        (
            CliIntent::Replica(ReplicaIntent::SnapshotVerify { snapshot }),
            ClientDependency::None,
        ) => verify_local_snapshot(&snapshot),
        (CliIntent::Start, ClientDependency::ConfigRoot(config_root)) => start(&config_root),
        (CliIntent::Stop, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(stop(&config_root))
        }
        (CliIntent::Status { json }, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(show_status(&config_root, json))
        }
        (
            CliIntent::Log(LogIntent::Set { target, level }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(set_log_filter(&config_root, target, level)),
        (
            CliIntent::Replica(ReplicaIntent::Inspect { document }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(inspect_replica_document(&config_root, &document)),
        (
            CliIntent::Replica(ReplicaIntent::Ops {
                document,
                limit,
                format,
            }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(show_replica_operations(
            &config_root,
            &document,
            limit.map(|limit| limit.get()),
            format,
        )),
        (
            CliIntent::Replica(ReplicaIntent::Export { output }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(export_replica(&config_root, &output)),
        (
            CliIntent::Replica(ReplicaIntent::Import { snapshot }),
            ClientDependency::ConfigRoot(config_root),
        ) => {
            if !confirm_replica_import()? {
                return Ok(());
            }
            in_runtime(import_replica(&config_root, &snapshot))
        }
        (CliIntent::Ping { node_name }, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(ping_peer(&config_root, &node_name))
        }
        (
            CliIntent::Sync(SyncIntent::Synchronize {
                node_name,
                max_attempts,
            }),
            ClientDependency::ConfigRoot(config_root),
        ) => in_runtime(synchronize_peers(
            &config_root,
            node_name.as_ref(),
            max_attempts.map_or(1, std::num::NonZeroU32::get),
        )),
        (CliIntent::Sync(SyncIntent::ViewLog), ClientDependency::LogDir(log_dir)) => {
            show_log_file(&log_dir.join("sync.log"))
        }
        (CliIntent::Plugin(intent), dependency) => plugin_cli::execute_plugin(intent, dependency),
        (CliIntent::Job(intent), ClientDependency::ConfigRoot(config_root)) => {
            plugin_cli::execute_job(intent, &config_root)
        }
        (
            CliIntent::Job(
                JobIntent::List { .. } | JobIntent::Info { .. } | JobIntent::Stop { .. },
            ),
            _,
        ) => Err(NodeError::Internal(
            "job command was prepared with an invalid dependency".to_owned(),
        )),
        _ => Err(NodeError::NotImplemented),
    }
}

fn show_log_file(path: &Path) -> Result<(), NodeError> {
    let mut file =
        std::fs::File::open(path).map_err(|error| NodeError::io("open log file", error))?;
    let mut stdout = io::stdout().lock();
    io::copy(&mut file, &mut stdout)
        .and_then(|_| stdout.flush())
        .map_err(|error| NodeError::io("write log output", error))
}
