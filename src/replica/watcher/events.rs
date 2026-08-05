use std::{path::Path, sync::Arc};

use notify_debouncer_full::{
    DebounceEventResult,
    notify::{
        EventKind,
        event::{ModifyKind, RenameMode},
    },
};
use serde_json::json;
use tokio::sync::{mpsc, watch};

use crate::node::logging::{LogLevel, new_correlation_id};

use super::{
    super::{
        ReplicaError,
        identity::{self, ReplicaIdentity},
        model::{
            absolute_to_namespace, apply_reliable_rename, initialize_from_disk, reconcile_disk,
            scan_working_tree,
        },
        store::IdentityTransitionKind,
    },
    filesystem::event_requires_reconciliation,
    support::elapsed_ms,
    types::ReplicaRuntime,
};

impl ReplicaRuntime {
    pub(super) async fn event_loop(
        self: Arc<Self>,
        mut receiver: mpsc::UnboundedReceiver<DebounceEventResult>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            let result = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        break;
                    }
                    continue;
                }
                result = receiver.recv() => {
                    let Some(result) = result else {
                        break;
                    };
                    result
                }
            };
            let correlation_id = new_correlation_id();
            match result {
                Ok(events) => {
                    let mut reconcile_final_state = false;
                    for event in &events {
                        reconcile_final_state |= event_requires_reconciliation(event);
                        let rename_error = if matches!(
                            event.kind,
                            EventKind::Modify(ModifyKind::Name(RenameMode::Both))
                        ) && event.paths.len() == 2
                        {
                            self.reconcile_rename(&event.paths[0], &event.paths[1], &correlation_id)
                                .await
                                .err()
                        } else {
                            None
                        };
                        if let Some(error) = rename_error {
                            self.log_failure("working_tree_rename_failed", &correlation_id, &error);
                        }
                    }
                    if !reconcile_final_state {
                        continue;
                    }
                }
                Err(errors) => {
                    self.logger.emit(
                        LogLevel::Warn,
                        "oll::replica",
                        "working_tree_watcher_error",
                        &correlation_id,
                        json!({ "error_count": errors.len() }),
                    );
                }
            }
            if let Err(error) = self.reconcile(&correlation_id).await {
                self.log_failure(
                    "working_tree_reconciliation_failed",
                    &correlation_id,
                    &error,
                );
            }
        }
    }

    pub(super) async fn reconcile(&self, correlation_id: &str) -> Result<(), ReplicaError> {
        let started = std::time::Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "working_tree_reconciliation_started",
            correlation_id,
            json!({}),
        );
        let _coordinator = self.identities.commit_guard().await;
        self.recover_projection(Some(correlation_id)).await?;
        let root = self.root.clone();
        let disk = tokio::task::spawn_blocking(move || scan_working_tree(&root))
            .await
            .map_err(|error| {
                ReplicaError::Internal(format!("working-tree scan task failed: {error}"))
            })??;
        let current = self.state.read().await.clone();
        let was_uninitialized = current.is_none();
        let writer_node_id = self.identities.node_id().await;
        let change = match current.as_ref() {
            None if disk.is_empty() => {
                self.logger.emit(
                    LogLevel::Info,
                    "oll::replica",
                    "working_tree_reconciliation_completed",
                    correlation_id,
                    json!({
                        "replica_state": "uninitialized",
                        "duration_ms": elapsed_ms(started),
                        "changed": false,
                    }),
                );
                return Ok(());
            }
            None => initialize_from_disk(&disk, writer_node_id, correlation_id)?,
            Some(replica) => reconcile_disk(replica, &disk, writer_node_id, correlation_id)?,
        };
        if !change.changed {
            self.logger.emit(
                LogLevel::Info,
                "oll::replica",
                "working_tree_reconciliation_completed",
                correlation_id,
                json!({
                    "replica_id": change.replica.replica_id.to_string(),
                    "duration_ms": elapsed_ms(started),
                    "changed": false,
                }),
            );
            return Ok(());
        }
        if was_uninitialized {
            self.store
                .build_inactive_generation(
                    &change.replica,
                    &change.blobs,
                    &change.operations,
                    &change.projection_paths,
                )
                .await?;
            identity::activate_candidate(
                &self.store,
                &self.config_root,
                None,
                &change.replica,
                IdentityTransitionKind::Initialize,
                false,
            )
            .await?;
        } else {
            self.store
                .save_active(
                    &change.replica,
                    &change.blobs,
                    &change.operations,
                    &change.projection_paths,
                )
                .await?;
        }
        self.replace_state(change.replica.clone()).await;
        if !change.projection_paths.is_empty() {
            self.project_targeted(&change.replica, &change.projection_paths, correlation_id)
                .await?;
        }
        self.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "working_tree_reconciliation_completed",
            correlation_id,
            json!({
                "replica_id": change.replica.replica_id.to_string(),
                "object_count": change.replica.visible_count(),
                "duration_ms": elapsed_ms(started),
                "changed": true,
            }),
        );
        Ok(())
    }

    pub(crate) async fn reload_replica_identity(
        &self,
        correlation_id: &str,
    ) -> Result<bool, ReplicaError> {
        let replacement = ReplicaIdentity::load(&self.config_root)?;
        let _coordinator = self.identities.commit_guard().await;
        let mut replica = self.state.read().await.clone().ok_or_else(|| {
            ReplicaError::Configuration(
                "replica.json cannot identify an uninitialized replica".to_owned(),
            )
        })?;
        if replica.replica_id == replacement.replica_id() {
            return Ok(false);
        }
        let previous = replica.replica_id;
        self.store
            .update_active_replica_id(replica.generation_id, previous, replacement.replica_id())
            .await?;
        replica.replica_id = replacement.replica_id();
        self.replace_state(replica).await;
        let epoch = self
            .identities
            .advance_epoch()
            .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        self.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "replica_identity_updated",
            correlation_id,
            json!({
                "previous_replica_id": previous.to_string(),
                "replica_id": replacement.replica_id().to_string(),
                "identity_epoch": epoch,
            }),
        );
        Ok(true)
    }

    async fn reconcile_rename(
        &self,
        source: &Path,
        destination: &Path,
        correlation_id: &str,
    ) -> Result<(), ReplicaError> {
        let source = absolute_to_namespace(&self.root, source)?;
        let destination = absolute_to_namespace(&self.root, destination)?;
        let _coordinator = self.identities.commit_guard().await;
        let current = self.state.read().await.clone();
        let Some(current) = current else {
            return Ok(());
        };
        let Some(change) = apply_reliable_rename(&current, &source, &destination, correlation_id)?
        else {
            return Ok(());
        };
        self.store
            .save_active(
                &change.replica,
                &change.blobs,
                &change.operations,
                &change.projection_paths,
            )
            .await?;
        self.replace_state(change.replica.clone()).await;
        if !change.projection_paths.is_empty() {
            self.project_targeted(&change.replica, &change.projection_paths, correlation_id)
                .await?;
        }
        self.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "working_tree_entry_moved",
            correlation_id,
            json!({
                "path_before": source,
                "path_after": destination,
                "replica_id": change.replica.replica_id.to_string(),
            }),
        );
        Ok(())
    }
}
