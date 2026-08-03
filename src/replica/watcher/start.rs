use std::{
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
};

use notify_debouncer_full::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
use tokio::sync::{Mutex, RwLock, mpsc, watch};

use crate::{
    configuration::ReplicaStoreConfig,
    node::{
        identity::IdentityCoordinator,
        logging::{NodeLogger, new_correlation_id},
    },
};

use super::{
    super::{ReplicaError, identity, store::ReplicaStore},
    WATCH_DEBOUNCE,
    types::ReplicaRuntime,
};

impl ReplicaRuntime {
    pub(crate) async fn start(
        config_root: PathBuf,
        root: PathBuf,
        store_config: &ReplicaStoreConfig,
        identities: Arc<IdentityCoordinator>,
        logger: Arc<NodeLogger>,
    ) -> Result<Arc<Self>, ReplicaError> {
        let store = Arc::new(ReplicaStore::open(store_config).await?);
        identity::recover_transition(&store, &config_root).await?;
        store.clear_bootstrap_claim_on_startup().await?;
        store.discard_orphaned_generations_on_startup().await?;
        let mut active = store.load_active().await?;
        identity::reconcile_startup_identity(&store, &config_root, &mut active).await?;
        store.ensure_active_state_guard(active.as_ref()).await?;
        let (event_shutdown, event_shutdown_rx) = watch::channel(false);
        let runtime = Arc::new(Self {
            config_root,
            root,
            store,
            state: RwLock::new(active),
            identities,
            logger,
            watcher: StdMutex::new(None),
            event_shutdown,
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
            task_runtime.event_loop(receiver, event_shutdown_rx).await;
        });
        *runtime.event_task.lock().await = Some(task);
        Ok(runtime)
    }
}
