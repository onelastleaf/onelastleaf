use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use notify_debouncer_full::{
    DebounceEventResult, Debouncer, RecommendedCache, new_debouncer,
    notify::{
        EventKind, RecommendedWatcher, RecursiveMode,
        event::{AccessKind, AccessMode, MetadataKind, ModifyKind, RenameMode},
    },
};
use serde_json::json;
use tokio::{
    sync::{Mutex, RwLock, mpsc},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    configuration::ReplicaStoreConfig,
    node::logging::{LogLevel, NodeLogger, new_correlation_id},
};

use super::{
    ReplicaError,
    classification::encode_text,
    model::{
        absolute_to_namespace, apply_reliable_rename, initialize_from_disk, reconcile_disk,
        scan_working_tree,
    },
    store::ReplicaStore,
    types::{ActiveReplica, CatalogEntry, EntryData, OperationRecord, ReplicaStatus},
};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(200);

#[derive(Clone, Debug)]
pub struct DocumentInspection {
    pub catalog_node_id: Uuid,
    pub catalog_revision: [u8; 32],
    pub document_id: Uuid,
    pub document_revision: [u8; 32],
    pub path: String,
    pub media_type: String,
    pub encoding: String,
    pub has_byte_order_mark: bool,
    pub size_bytes: u64,
}

pub struct ReplicaRuntime {
    pub(crate) root: PathBuf,
    pub(crate) writer_node_id: Uuid,
    pub(crate) store: Arc<ReplicaStore>,
    pub(crate) state: RwLock<Option<ActiveReplica>>,
    pub(crate) coordinator: Mutex<()>,
    pub(crate) logger: Arc<NodeLogger>,
    watcher: StdMutex<Option<Debouncer<RecommendedWatcher, RecommendedCache>>>,
    event_task: Mutex<Option<JoinHandle<()>>>,
}

impl ReplicaRuntime {
    pub async fn start(
        root: PathBuf,
        store_config: &ReplicaStoreConfig,
        writer_node_id: Uuid,
        logger: Arc<NodeLogger>,
    ) -> Result<Arc<Self>, ReplicaError> {
        let store = Arc::new(ReplicaStore::open(store_config).await?);
        let active = store.load_active().await?;
        let runtime = Arc::new(Self {
            root,
            writer_node_id,
            store,
            state: RwLock::new(active),
            coordinator: Mutex::new(()),
            logger,
            watcher: StdMutex::new(None),
            event_task: Mutex::new(None),
        });
        runtime.recover_projection(None).await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        let mut watcher =
            new_debouncer(WATCH_DEBOUNCE, None, move |result: DebounceEventResult| {
                let _ = sender.send(result);
            })
            .map_err(|error| {
                ReplicaError::Internal(format!("cannot initialize recursive watcher: {error}"))
            })?;
        watcher
            .watch(&runtime.root, RecursiveMode::Recursive)
            .map_err(|error| {
                ReplicaError::Internal(format!("cannot watch replica_root recursively: {error}"))
            })?;
        *runtime
            .watcher
            .lock()
            .map_err(|_| ReplicaError::Internal("watcher lock is poisoned".to_owned()))? =
            Some(watcher);

        let startup_correlation = new_correlation_id();
        if let Err(error) = runtime.reconcile(&startup_correlation).await {
            runtime.take_watcher();
            return Err(error);
        }
        let task_runtime = Arc::clone(&runtime);
        let task = tokio::spawn(async move {
            task_runtime.event_loop(receiver).await;
        });
        *runtime.event_task.lock().await = Some(task);
        Ok(runtime)
    }

    pub async fn status(&self) -> ReplicaStatus {
        self.state
            .read()
            .await
            .as_ref()
            .map_or(ReplicaStatus::Uninitialized, ActiveReplica::status)
    }

    pub async fn inspect_document(
        &self,
        native_path: &Path,
    ) -> Result<DocumentInspection, ReplicaError> {
        let namespace = absolute_to_namespace(&self.root, native_path)?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let entry = replica
            .entry_at_path(&namespace)?
            .ok_or_else(|| ReplicaError::NotFound("managed document was not found".to_owned()))?;
        let document = entry.document().ok_or_else(|| {
            ReplicaError::InvalidArgument(
                "replica inspect path must name a text document".to_owned(),
            )
        })?;
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("document Loro object is missing".to_owned())
            })?;
        Ok(DocumentInspection {
            catalog_node_id: entry.catalog_node_id,
            catalog_revision: entry.catalog_revision,
            document_id: document.document_id,
            document_revision: object.revision,
            path: namespace,
            media_type: document.media_type.clone(),
            encoding: document.encoding.clone(),
            has_byte_order_mark: document.has_byte_order_mark,
            size_bytes: document.size_bytes,
        })
    }

    pub async fn list_operations(
        &self,
        native_path: &Path,
        limit: usize,
    ) -> Result<Vec<OperationRecord>, ReplicaError> {
        if limit == 0 {
            return Err(ReplicaError::InvalidArgument(
                "operation limit must be greater than zero".to_owned(),
            ));
        }
        let inspection = self.inspect_document(native_path).await?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        self.store
            .list_operations(replica.generation_id, inspection.document_id, limit)
            .await
    }

    pub async fn export_snapshot(&self, destination: &Path) -> Result<(Uuid, Uuid), ReplicaError> {
        super::snapshot::export_runtime(self, destination).await
    }

    pub async fn import_snapshot(&self, source: &Path) -> Result<(Uuid, Uuid), ReplicaError> {
        super::snapshot::import_runtime(self, source).await
    }

    pub async fn shutdown(&self) -> Result<(), ReplicaError> {
        self.take_watcher();
        if let Some(task) = self.event_task.lock().await.take() {
            task.await.map_err(|error| {
                ReplicaError::Internal(format!("filesystem watcher task failed: {error}"))
            })?;
        }
        Ok(())
    }

    pub(crate) async fn replace_state(&self, replica: ActiveReplica) {
        *self.state.write().await = Some(replica);
    }

    pub(crate) async fn project_complete(
        &self,
        replica: &ActiveReplica,
    ) -> Result<(), ReplicaError> {
        let desired = replica.projected_paths()?;
        let desired_paths = desired.values().cloned().collect::<BTreeSet<_>>();
        let mut actual = Vec::new();
        for item in walkdir::WalkDir::new(&self.root)
            .follow_links(false)
            .min_depth(1)
        {
            let item = item.map_err(|error| {
                ReplicaError::InvalidArgument(format!(
                    "cannot inspect working tree during projection: {error}"
                ))
            })?;
            actual.push((
                absolute_to_namespace(&self.root, item.path())?,
                item.path().to_owned(),
            ));
        }
        actual.sort_by_key(|(path, _)| std::cmp::Reverse(path.matches('/').count()));
        for (namespace, path) in actual {
            if !desired_paths.contains(&namespace) {
                remove_path(&path)?;
            }
        }
        self.materialize_paths(replica, desired.keys().copied())
            .await
    }

    async fn event_loop(
        self: Arc<Self>,
        mut receiver: mpsc::UnboundedReceiver<DebounceEventResult>,
    ) {
        while let Some(result) = receiver.recv().await {
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
                            let _ = self.log_failure(
                                "working_tree_rename_failed",
                                &correlation_id,
                                &error,
                            );
                        }
                    }
                    if !reconcile_final_state {
                        continue;
                    }
                }
                Err(errors) => {
                    let _ = self.logger.emit(
                        LogLevel::Warn,
                        "oll::replica",
                        "working_tree_watcher_error",
                        &correlation_id,
                        json!({ "error_count": errors.len() }),
                    );
                }
            }
            if let Err(error) = self.reconcile(&correlation_id).await {
                let _ = self.log_failure(
                    "working_tree_reconciliation_failed",
                    &correlation_id,
                    &error,
                );
            }
        }
    }

    async fn reconcile(&self, correlation_id: &str) -> Result<(), ReplicaError> {
        let started = std::time::Instant::now();
        self.logger
            .emit(
                LogLevel::Info,
                "oll::replica",
                "working_tree_reconciliation_started",
                correlation_id,
                json!({}),
            )
            .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        let _coordinator = self.coordinator.lock().await;
        self.recover_projection(Some(correlation_id)).await?;
        let root = self.root.clone();
        let disk = tokio::task::spawn_blocking(move || scan_working_tree(&root))
            .await
            .map_err(|error| {
                ReplicaError::Internal(format!("working-tree scan task failed: {error}"))
            })??;
        let current = self.state.read().await.clone();
        let was_uninitialized = current.is_none();
        let change = match current.as_ref() {
            None if disk.is_empty() => {
                self.logger
                    .emit(
                        LogLevel::Info,
                        "oll::replica",
                        "working_tree_reconciliation_completed",
                        correlation_id,
                        json!({
                            "replica_state": "uninitialized",
                            "duration_ms": elapsed_ms(started),
                            "changed": false,
                        }),
                    )
                    .map_err(|error| ReplicaError::Internal(error.to_string()))?;
                return Ok(());
            }
            None => initialize_from_disk(&disk, self.writer_node_id, correlation_id)?,
            Some(replica) => reconcile_disk(replica, &disk, self.writer_node_id, correlation_id)?,
        };
        if !change.changed {
            self.logger
                .emit(
                    LogLevel::Info,
                    "oll::replica",
                    "working_tree_reconciliation_completed",
                    correlation_id,
                    json!({
                        "replica_id": change.replica.replica_id.to_string(),
                        "duration_ms": elapsed_ms(started),
                        "changed": false,
                    }),
                )
                .map_err(|error| ReplicaError::Internal(error.to_string()))?;
            return Ok(());
        }
        if was_uninitialized {
            self.store
                .initialize(
                    &change.replica,
                    &change.blobs,
                    &change.operations,
                    &change.projection_paths,
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
        *self.state.write().await = Some(change.replica.clone());
        if !change.projection_paths.is_empty() {
            self.project_targeted(&change.replica, &change.projection_paths)
                .await?;
            self.store
                .clear_projection_paths(change.replica.generation_id)
                .await?;
        }
        self.logger
            .emit(
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
            )
            .map_err(|error| ReplicaError::Internal(error.to_string()))
    }

    async fn reconcile_rename(
        &self,
        source: &Path,
        destination: &Path,
        correlation_id: &str,
    ) -> Result<(), ReplicaError> {
        let source = absolute_to_namespace(&self.root, source)?;
        let destination = absolute_to_namespace(&self.root, destination)?;
        let _coordinator = self.coordinator.lock().await;
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
        *self.state.write().await = Some(change.replica.clone());
        if !change.projection_paths.is_empty() {
            self.project_targeted(&change.replica, &change.projection_paths)
                .await?;
            self.store
                .clear_projection_paths(change.replica.generation_id)
                .await?;
        }
        self.logger
            .emit(
                LogLevel::Info,
                "oll::replica",
                "working_tree_entry_moved",
                correlation_id,
                json!({
                    "path_before": source,
                    "path_after": destination,
                    "replica_id": change.replica.replica_id.to_string(),
                }),
            )
            .map_err(|error| ReplicaError::Internal(error.to_string()))
    }

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
            self.logger
                .emit(
                    LogLevel::Warn,
                    "oll::replica",
                    "replica_projection_recovery_started",
                    correlation_id,
                    json!({
                        "replica_id": replica.replica_id.to_string(),
                        "scope": "complete",
                    }),
                )
                .map_err(|error| ReplicaError::Internal(error.to_string()))?;
            let started = std::time::Instant::now();
            let recovery = async {
                self.project_complete(&replica).await?;
                self.store
                    .clear_projection_pending(replica.generation_id)
                    .await
            }
            .await;
            if let Err(error) = recovery {
                let _ = self.logger.emit(
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
            self.logger
                .emit(
                    LogLevel::Info,
                    "oll::replica",
                    "replica_projection_recovery_completed",
                    correlation_id,
                    json!({
                        "replica_id": replica.replica_id.to_string(),
                        "scope": "complete",
                        "duration_ms": elapsed_ms(started),
                    }),
                )
                .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        } else {
            let paths = self.store.projection_paths(replica.generation_id).await?;
            if !paths.is_empty() {
                let started = std::time::Instant::now();
                self.logger
                    .emit(
                        LogLevel::Warn,
                        "oll::replica",
                        "replica_projection_recovery_started",
                        correlation_id,
                        json!({
                            "replica_id": replica.replica_id.to_string(),
                            "scope": "targeted",
                            "path_count": paths.len(),
                        }),
                    )
                    .map_err(|error| ReplicaError::Internal(error.to_string()))?;
                let recovery = async {
                    self.project_targeted(&replica, &paths).await?;
                    self.store
                        .clear_projection_paths(replica.generation_id)
                        .await
                }
                .await;
                if let Err(error) = recovery {
                    let _ = self.logger.emit(
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
                self.logger
                    .emit(
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
                    )
                    .map_err(|error| ReplicaError::Internal(error.to_string()))?;
            }
        }
        Ok(())
    }

    pub(crate) async fn project_targeted(
        &self,
        replica: &ActiveReplica,
        paths: &[String],
    ) -> Result<(), ReplicaError> {
        let desired = replica.projected_paths()?;
        let by_path = desired
            .iter()
            .map(|(id, path)| (path.as_str(), *id))
            .collect::<BTreeMap<_, _>>();
        let mut removals = Vec::new();
        let mut materialize = BTreeSet::new();
        for path in paths {
            if let Some(id) = by_path.get(path.as_str()) {
                materialize.insert(*id);
            } else {
                removals.push(path.clone());
            }
        }
        removals.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
        for path in removals {
            remove_path(&self.native_path(&path)?)?;
        }
        self.materialize_paths(replica, materialize).await
    }

    async fn materialize_paths(
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

    fn take_watcher(&self) {
        if let Ok(mut watcher) = self.watcher.lock() {
            watcher.take();
        }
    }

    fn log_failure(
        &self,
        event: &str,
        correlation_id: &str,
        error: &ReplicaError,
    ) -> Result<(), crate::node::NodeError> {
        self.logger.emit(
            LogLevel::Error,
            "oll::replica",
            event,
            correlation_id,
            json!({ "error_code": error.code() }),
        )
    }
}

fn ensure_projection_ancestors(root: &Path, path: &Path) -> Result<(), ReplicaError> {
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|error| ReplicaError::io("inspect replica root before projection", error))?;
    if !root_metadata.is_dir() {
        return Err(ReplicaError::InvalidArgument(
            "replica_root is not a real directory".to_owned(),
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        ReplicaError::CorruptStore("projected path escaped replica_root".to_owned())
    })?;
    let Some(parent) = relative.parent() else {
        return Ok(());
    };
    let mut current = root.to_owned();
    for component in parent.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(ReplicaError::CorruptStore(
                "projected path has a non-normal ancestor".to_owned(),
            ));
        };
        current.push(segment);
        ensure_projected_directory(&current)?;
    }
    Ok(())
}

fn event_requires_reconciliation(event: &notify_debouncer_full::DebouncedEvent) -> bool {
    event.need_rescan()
        || matches!(
            event.kind,
            EventKind::Any
                | EventKind::Create(_)
                | EventKind::Remove(_)
                | EventKind::Modify(
                    ModifyKind::Any
                        | ModifyKind::Data(_)
                        | ModifyKind::Name(_)
                        | ModifyKind::Metadata(
                            MetadataKind::Any | MetadataKind::WriteTime | MetadataKind::Other
                        )
                        | ModifyKind::Other
                )
                | EventKind::Access(AccessKind::Close(AccessMode::Write))
        )
}

fn ensure_projected_directory(path: &Path) -> Result<(), ReplicaError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => remove_path(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ReplicaError::io("inspect projected directory", error)),
    }
    fs::create_dir_all(path)
        .map_err(|error| ReplicaError::io("create projected directory", error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| ReplicaError::io("set projected directory permissions", error))?;
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io("sync projected directory", error))?;
    sync_parent(path, "sync projected directory parent")
}

fn atomic_project_file(path: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected file has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create projected file parent", error))?;
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
        fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("replace projected directory with file", error))?;
    }
    let temporary = parent.join(format!(".oll-project-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| ReplicaError::io("create projected temporary file", error))?;
        output
            .write_all(bytes)
            .map_err(|error| ReplicaError::io("write projected temporary file", error))?;
        output
            .sync_all()
            .map_err(|error| ReplicaError::io("sync projected temporary file", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| ReplicaError::io("publish projected file", error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReplicaError::io("sync projected directory", error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

async fn atomic_project_blob(
    store: &ReplicaStore,
    path: &Path,
    sha256: &str,
) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected file has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create projected file parent", error))?;
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
        fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("replace projected directory with file", error))?;
    }
    let temporary = parent.join(format!(".oll-project-{}.tmp", Uuid::new_v4()));
    let result = async {
        store.write_blob_to_path(sha256, &temporary).await?;
        fs::rename(&temporary, path)
            .map_err(|error| ReplicaError::io("publish projected file", error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReplicaError::io("sync projected directory", error))
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn remove_path(path: &Path) -> Result<(), ReplicaError> {
    let removed = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("remove stale projected directory", error)),
        Ok(_) => fs::remove_file(path)
            .map_err(|error| ReplicaError::io("remove stale projected file", error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => Err(ReplicaError::io("inspect stale projected path", error)),
    };
    removed?;
    sync_parent(path, "sync stale projection parent")
}

fn sync_parent(path: &Path, operation: &'static str) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected path has no parent directory".to_owned())
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io(operation, error))
}

fn elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
