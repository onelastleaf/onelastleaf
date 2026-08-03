use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use serde_json::json;
use uuid::Uuid;

use crate::node::logging::{LogLevel, new_correlation_id};

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        types::{ActiveReplica, CatalogEntry, EntryData},
    },
    PROJECTION_ATTEMPTS, PROJECTION_RETRY_DELAY,
    filesystem::{
        atomic_project_blob, atomic_project_file, ensure_projected_directory,
        ensure_projection_ancestors, remove_path,
    },
    support::elapsed_ms,
    types::ReplicaRuntime,
};

impl ReplicaRuntime {
    pub(crate) async fn recover_projection(
        &self,
        inherited_correlation_id: Option<&str>,
    ) -> Result<(), ReplicaError> {
        let active = self.state.read().await.clone();
        let whole_pending = self.store.projection_pending().await?;
        let Some(replica) = active else {
            if whole_pending {
                return Err(ReplicaError::CorruptStore(
                    "projection is pending without an active replica".to_owned(),
                ));
            }
            return Ok(());
        };
        let generated_correlation_id;
        let correlation_id = if let Some(correlation_id) = inherited_correlation_id {
            correlation_id
        } else {
            generated_correlation_id = new_correlation_id();
            &generated_correlation_id
        };
        if whole_pending {
            self.logger.emit(
                LogLevel::Warn,
                "oll::replica",
                "replica_projection_recovery_started",
                correlation_id,
                json!({
                    "replica_id": replica.replica_id.to_string(),
                    "scope": "complete",
                }),
            );
            let started = std::time::Instant::now();
            let recovery = async {
                self.project_complete(&replica).await?;
                self.store
                    .clear_projection_pending(replica.generation_id)
                    .await
            }
            .await;
            if let Err(error) = recovery {
                self.logger.emit(
                    LogLevel::Error,
                    "oll::replica",
                    "replica_projection_recovery_failed",
                    correlation_id,
                    json!({
                        "replica_id": replica.replica_id.to_string(),
                        "scope": "complete",
                        "error_code": error.code(),
                        "duration_ms": elapsed_ms(started),
                    }),
                );
                return Err(error);
            }
            self.logger.emit(
                LogLevel::Info,
                "oll::replica",
                "replica_projection_recovery_completed",
                correlation_id,
                json!({
                    "replica_id": replica.replica_id.to_string(),
                    "scope": "complete",
                    "duration_ms": elapsed_ms(started),
                }),
            );
        } else {
            let paths = self.store.projection_paths(replica.generation_id).await?;
            if !paths.is_empty() {
                let started = std::time::Instant::now();
                self.logger.emit(
                    LogLevel::Warn,
                    "oll::replica",
                    "replica_projection_recovery_started",
                    correlation_id,
                    json!({
                        "replica_id": replica.replica_id.to_string(),
                        "scope": "targeted",
                        "path_count": paths.len(),
                    }),
                );
                let recovery = async {
                    self.project_targeted(&replica, &paths, correlation_id)
                        .await
                }
                .await;
                if let Err(error) = recovery {
                    self.logger.emit(
                        LogLevel::Error,
                        "oll::replica",
                        "replica_projection_recovery_failed",
                        correlation_id,
                        json!({
                            "replica_id": replica.replica_id.to_string(),
                            "scope": "targeted",
                            "path_count": paths.len(),
                            "error_code": error.code(),
                            "duration_ms": elapsed_ms(started),
                        }),
                    );
                    return Err(error);
                }
                self.logger.emit(
                    LogLevel::Info,
                    "oll::replica",
                    "replica_projection_recovery_completed",
                    correlation_id,
                    json!({
                        "replica_id": replica.replica_id.to_string(),
                        "scope": "targeted",
                        "path_count": paths.len(),
                        "duration_ms": elapsed_ms(started),
                    }),
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn project_targeted(
        &self,
        replica: &ActiveReplica,
        paths: &[String],
        correlation_id: &str,
    ) -> Result<(), ReplicaError> {
        let desired = replica.projected_paths()?;
        let by_path = desired
            .iter()
            .map(|(id, path)| (path.as_str(), *id))
            .collect::<BTreeMap<_, _>>();
        let mut removals = Vec::new();
        let mut materializations = Vec::new();
        for path in paths.iter().cloned().collect::<BTreeSet<_>>() {
            if let Some(id) = by_path.get(path.as_str()) {
                materializations.push((path, *id));
            } else {
                removals.push(path);
            }
        }
        removals.sort_by(|left, right| {
            right
                .matches('/')
                .count()
                .cmp(&left.matches('/').count())
                .then_with(|| left.cmp(right))
        });
        materializations.sort_by(|(left, _), (right, _)| {
            left.matches('/')
                .count()
                .cmp(&right.matches('/').count())
                .then_with(|| left.cmp(right))
        });
        let targets = removals.into_iter().map(|path| (path, None)).chain(
            materializations
                .into_iter()
                .map(|(path, id)| (path, Some(id))),
        );
        let mut first_failure = None;
        for (path, id) in targets {
            let projection = 'attempts: {
                for attempt in 1..=PROJECTION_ATTEMPTS {
                    let result = if let Some(id) = id {
                        match replica.entries.get(&id) {
                            Some(entry) => match self.native_path(&path) {
                                Ok(native_path) => {
                                    self.materialize_entry(replica, entry, &native_path).await
                                }
                                Err(error) => Err(error),
                            },
                            None => Err(ReplicaError::CorruptStore(
                                "projection entry is missing".to_owned(),
                            )),
                        }
                    } else {
                        match self.native_path(&path) {
                            Ok(native_path) => remove_path(&native_path),
                            Err(error) => Err(error),
                        }
                    };
                    match result {
                        Ok(()) => break 'attempts Ok(()),
                        Err(error) if attempt < PROJECTION_ATTEMPTS => {
                            self.logger.emit(
                                LogLevel::Warn,
                                "oll::replica",
                                "working_tree_projection_retrying",
                                correlation_id,
                                json!({
                                    "path": &path,
                                    "error_code": error.code(),
                                    "retryable": true,
                                    "attempt": attempt,
                                    "backoff_ms": u64::try_from(
                                        PROJECTION_RETRY_DELAY.as_millis()
                                    )
                                    .unwrap_or(u64::MAX),
                                }),
                            );
                            tokio::time::sleep(PROJECTION_RETRY_DELAY).await;
                        }
                        Err(error) => break 'attempts Err(error),
                    }
                }
                unreachable!("projection attempt loop always returns")
            };
            let result = match projection {
                Ok(()) => {
                    self.store
                        .clear_projection_path(replica.generation_id, &path)
                        .await
                }
                Err(error) => Err(error),
            };
            if first_failure.is_none() {
                first_failure = result.err();
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    pub(super) async fn materialize_paths(
        &self,
        replica: &ActiveReplica,
        ids: impl IntoIterator<Item = Uuid>,
    ) -> Result<(), ReplicaError> {
        let paths = replica.projected_paths()?;
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_by_key(|id| {
            paths
                .get(id)
                .map_or(usize::MAX, |path| path.matches('/').count())
        });
        for id in ids {
            let entry = replica.entries.get(&id).ok_or_else(|| {
                ReplicaError::CorruptStore("projection entry is missing".to_owned())
            })?;
            if entry.deleted {
                continue;
            }
            let path = paths.get(&id).ok_or_else(|| {
                ReplicaError::CorruptStore("projection path is missing".to_owned())
            })?;
            self.materialize_entry(replica, entry, &self.native_path(path)?)
                .await?;
        }
        Ok(())
    }

    async fn materialize_entry(
        &self,
        replica: &ActiveReplica,
        entry: &CatalogEntry,
        path: &Path,
    ) -> Result<(), ReplicaError> {
        ensure_projection_ancestors(&self.root, path)?;
        match &entry.data {
            EntryData::Directory => ensure_projected_directory(path),
            EntryData::Document(document) => {
                let object = replica
                    .documents
                    .get(&document.document_id)
                    .ok_or_else(|| {
                        ReplicaError::CorruptStore(
                            "document object is missing during projection".to_owned(),
                        )
                    })?;
                let doc = loro::LoroDoc::new();
                doc.import(&object.loro).map_err(|error| {
                    ReplicaError::CorruptStore(format!(
                        "cannot decode document during projection: {error}"
                    ))
                })?;
                let text = doc.get_text("content").to_string();
                let (bytes, promoted) =
                    encode_text(&text, &document.encoding, document.has_byte_order_mark)?;
                if promoted {
                    return Err(ReplicaError::CorruptStore(
                        "document requires a durable UTF-8 encoding promotion before projection"
                            .to_owned(),
                    ));
                }
                atomic_project_file(path, &bytes)
            }
            EntryData::Binary(binary) => {
                let (_, version) = binary.winning_version().ok_or_else(|| {
                    ReplicaError::CorruptStore("binary has no winning version".to_owned())
                })?;
                atomic_project_blob(&self.store, path, &version.sha256).await
            }
        }
    }

    fn native_path(&self, namespace: &str) -> Result<PathBuf, ReplicaError> {
        if namespace == "/" {
            return Ok(self.root.clone());
        }
        let relative = namespace.strip_prefix('/').ok_or_else(|| {
            ReplicaError::CorruptStore("projected path is not absolute".to_owned())
        })?;
        if relative
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        {
            return Err(ReplicaError::CorruptStore(
                "projected path has an invalid segment".to_owned(),
            ));
        }
        Ok(self.root.join(relative))
    }
}
