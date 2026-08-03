use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use notify_debouncer_full::{
    DebounceEventResult, Debouncer, RecommendedCache, new_debouncer,
    notify::{EventKind, RecommendedWatcher, RecursiveMode},
};
use serde_json::json;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{
    node::{
        identity::{self, IdentityCoordinator, NodeIdentity},
        logging::{LogLevel, NodeLogger, new_correlation_id},
    },
    replica::ReplicaRuntime,
};

use super::{IDENTITY_WATCH_DEBOUNCE, NodeError};

pub(super) struct IdentityWatch {
    watcher: Option<Debouncer<RecommendedWatcher, RecommendedCache>>,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl IdentityWatch {
    pub(super) async fn start(
        config_root: &Path,
        identities: Arc<IdentityCoordinator>,
        replica: Arc<ReplicaRuntime>,
        logger: Arc<NodeLogger>,
    ) -> Result<Self, NodeError> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let mut watcher = new_debouncer(
            IDENTITY_WATCH_DEBOUNCE,
            None,
            move |result: DebounceEventResult| {
                let _ = event_tx.send(result);
            },
        )
        .map_err(|error| {
            NodeError::Unavailable(format!("cannot initialize identity watcher: {error}"))
        })?;
        watcher
            .watch(config_root, RecursiveMode::NonRecursive)
            .map_err(|error| {
                NodeError::Unavailable(format!("cannot watch node identity files: {error}"))
            })?;
        let node_path = identity::identity_path(config_root);
        let replica_path = crate::replica::identity::identity_path(config_root);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (initial_tx, initial_rx) = oneshot::channel();
        let task = tokio::spawn(run_identity_watch(
            event_rx,
            shutdown_rx,
            node_path,
            replica_path,
            identities,
            replica,
            logger,
            initial_tx,
        ));
        initial_rx.await.map_err(|_| {
            NodeError::Internal("identity watcher stopped during its initial reload".to_owned())
        })?;
        Ok(Self {
            watcher: Some(watcher),
            shutdown,
            task: Some(task),
        })
    }

    pub(super) async fn shutdown(&mut self, deadline: Instant) -> Result<(), NodeError> {
        self.watcher.take();
        let _ = self.shutdown.send(true);
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match timeout_at(deadline, &mut task).await {
            Ok(result) => result.map_err(|error| {
                NodeError::Internal(format!("identity watcher task failed: {error}"))
            }),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(NodeError::Unavailable(
                    "identity watcher exceeded the graceful shutdown deadline".to_owned(),
                ))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_identity_watch(
    mut events: mpsc::UnboundedReceiver<DebounceEventResult>,
    mut shutdown: watch::Receiver<bool>,
    node_path: PathBuf,
    replica_path: PathBuf,
    identities: Arc<IdentityCoordinator>,
    replica: Arc<ReplicaRuntime>,
    logger: Arc<NodeLogger>,
    initial: oneshot::Sender<()>,
) {
    let mut initial = Some(initial);
    loop {
        let correlation_id = new_correlation_id();
        let (reload_node, reload_replica) = if initial.is_some() {
            (true, replica_path.exists())
        } else {
            let result = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        break;
                    }
                    continue;
                }
                result = events.recv() => {
                    let Some(result) = result else {
                        break;
                    };
                    result
                }
            };
            match result {
                Ok(events) => {
                    let mut reload_node = false;
                    let mut reload_replica = false;
                    for event in events {
                        if matches!(event.kind, EventKind::Access(_)) {
                            continue;
                        }
                        reload_node |= event.paths.iter().any(|path| path == &node_path);
                        reload_replica |= event.paths.iter().any(|path| path == &replica_path);
                    }
                    (reload_node, reload_replica)
                }
                Err(errors) => {
                    logger.emit(
                        LogLevel::Warn,
                        "oll::node",
                        "identity_watcher_error",
                        &correlation_id,
                        json!({ "error_count": errors.len() }),
                    );
                    (false, false)
                }
            }
        };
        if reload_node {
            let replacement = NodeIdentity::load(
                node_path
                    .parent()
                    .expect("node identity path always has its config-root parent"),
            );
            match replacement {
                Ok(replacement) => {
                    let _gate = identities.commit_guard().await;
                    let previous = identities.node().await;
                    if previous != replacement {
                        if logger.set_identity(replacement.clone()).is_err() {
                            logger.emit(
                                LogLevel::Error,
                                "oll::node",
                                "node_identity_reload_failed",
                                &correlation_id,
                                json!({ "error_code": "logger_identity_update_failed" }),
                            );
                        } else {
                            identities
                                .replace_node_while_commits_paused(replacement.clone())
                                .await;
                            match identities.advance_epoch() {
                                Ok(epoch) => logger.emit(
                                    LogLevel::Info,
                                    "oll::node",
                                    "node_identity_updated",
                                    &correlation_id,
                                    json!({
                                        "previous_node_id": previous.node_id().to_string(),
                                        "previous_node_name": previous.node_name().as_str(),
                                        "node_id": replacement.node_id().to_string(),
                                        "node_name": replacement.node_name().as_str(),
                                        "identity_epoch": epoch,
                                    }),
                                ),
                                Err(_) => logger.emit(
                                    LogLevel::Error,
                                    "oll::node",
                                    "node_identity_reload_failed",
                                    &correlation_id,
                                    json!({ "error_code": "identity_epoch_overflow" }),
                                ),
                            }
                        }
                    }
                }
                Err(_) => logger.emit(
                    LogLevel::Error,
                    "oll::node",
                    "node_identity_reload_failed",
                    &correlation_id,
                    json!({ "error_code": "invalid_node_identity" }),
                ),
            }
        }
        if reload_replica && let Err(error) = replica.reload_replica_identity(&correlation_id).await
        {
            logger.emit(
                LogLevel::Error,
                "oll::replica",
                "replica_identity_reload_failed",
                &correlation_id,
                json!({ "error_code": error.code() }),
            );
        }
        if let Some(initial) = initial.take() {
            let _ = initial.send(());
        }
    }
}
