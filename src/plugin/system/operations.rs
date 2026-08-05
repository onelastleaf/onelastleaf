use std::{collections::HashSet, future::Future, panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt as _;
use serde_json::json;
use tokio::{sync::Mutex, task::JoinHandle, time::Instant};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{PluginError, PluginJobId, PluginOperationId},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum OperationKey {
    StartJob(PluginOperationId),
    CancelJob(PluginJobId),
    JobDeadline(PluginJobId),
}

pub(super) struct OperationContext {
    kind: &'static str,
    correlation_id: String,
}

impl OperationContext {
    pub(super) fn new(kind: &'static str, correlation_id: impl Into<String>) -> Self {
        Self {
            kind,
            correlation_id: correlation_id.into(),
        }
    }
}

struct TrackedTask {
    timer: bool,
    task: JoinHandle<()>,
}

struct State {
    accepting: bool,
    keys: HashSet<OperationKey>,
    tasks: Vec<TrackedTask>,
}

/// Owns durable plugin continuations independently of the RPC futures that
/// initiated them.
pub(super) struct OperationTracker {
    state: Mutex<State>,
    logger: Arc<NodeLogger>,
}

pub(super) fn operation_result_lost<T>(_: T) -> PluginError {
    PluginError::FailedPrecondition(
        "plugin runtime operation ended without publishing its result".to_owned(),
    )
}

impl OperationTracker {
    pub(super) fn new(logger: Arc<NodeLogger>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                accepting: true,
                keys: HashSet::new(),
                tasks: Vec::new(),
            }),
            logger,
        })
    }

    pub(super) async fn spawn<F>(
        self: &Arc<Self>,
        context: OperationContext,
        future: F,
    ) -> Result<(), PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(None, false, context, future)
            .await
            .map(|_| ())
    }

    pub(super) async fn spawn_unique<F>(
        self: &Arc<Self>,
        key: OperationKey,
        context: OperationContext,
        future: F,
    ) -> Result<bool, PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(Some(key), false, context, future).await
    }

    pub(super) async fn spawn_timer<F>(
        self: &Arc<Self>,
        key: OperationKey,
        context: OperationContext,
        future: F,
    ) -> Result<bool, PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(Some(key), true, context, future).await
    }

    async fn spawn_inner<F>(
        self: &Arc<Self>,
        key: Option<OperationKey>,
        timer: bool,
        context: OperationContext,
        future: F,
    ) -> Result<bool, PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (completed, started) = {
            let mut state = self.state.lock().await;
            if !state.accepting {
                return Err(PluginError::FailedPrecondition(
                    "plugin runtime is shutting down".to_owned(),
                ));
            }
            let mut completed = Vec::new();
            let mut index = 0;
            while index < state.tasks.len() {
                if state.tasks[index].task.is_finished() {
                    completed.push(state.tasks.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            if key.as_ref().is_some_and(|key| state.keys.contains(key)) {
                (completed, false)
            } else {
                if let Some(key) = &key {
                    state.keys.insert(key.clone());
                }
                let owner = Arc::downgrade(self);
                let cleanup_key = key.clone();
                let logger = Arc::clone(&self.logger);
                let task = tokio::spawn(async move {
                    let panicked = AssertUnwindSafe(future).catch_unwind().await.is_err();
                    if let (Some(owner), Some(key)) = (owner.upgrade(), cleanup_key) {
                        owner.state.lock().await.keys.remove(&key);
                    }
                    if panicked {
                        logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_runtime_operation_panicked",
                            &context.correlation_id,
                            json!({
                                "operation_kind": context.kind,
                                "recoverable": true,
                            }),
                        );
                    }
                });
                state.tasks.push(TrackedTask { timer, task });
                (completed, true)
            }
        };
        for completed in completed {
            let _ = completed.task.await;
        }
        Ok(started)
    }

    pub(super) async fn shutdown(&self, deadline: Instant) -> Result<(), PluginError> {
        let mut tasks = {
            let mut state = self.state.lock().await;
            state.accepting = false;
            state.keys.clear();
            std::mem::take(&mut state.tasks)
        };
        for task in &tasks {
            if task.timer {
                task.task.abort();
            }
        }
        let mut failure = None;
        for index in 0..tasks.len() {
            match tokio::time::timeout_at(deadline, &mut tasks[index].task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() && tasks[index].timer => {}
                Ok(Err(_)) => {
                    failure.get_or_insert_with(|| {
                        PluginError::FailedPrecondition(
                            "plugin runtime operation task failed".to_owned(),
                        )
                    });
                }
                Err(_) => {
                    for task in &tasks[index..] {
                        task.task.abort();
                    }
                    for task in &mut tasks[index..] {
                        let _ = (&mut task.task).await;
                    }
                    return Err(PluginError::FailedPrecondition(
                        "plugin runtime operations exceeded the node shutdown deadline".to_owned(),
                    ));
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    pub(super) async fn contains_key(&self, key: &OperationKey) -> bool {
        self.state.lock().await.keys.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    };

    use tokio::{
        sync::{Notify, oneshot},
        time::Instant,
    };

    use crate::node::{NodeIdentity, logging::NodeLogger};

    use super::{OperationContext, OperationKey, OperationTracker};

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn durable_continuation_outlives_an_aborted_observer() {
        let directory = tempfile::TempDir::new().unwrap();
        let logger = NodeLogger::open(
            &directory.path().join("logs"),
            NodeIdentity::generate("operation-test".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = OperationTracker::new(logger);
        let phase = Arc::new(AtomicU8::new(0));
        let release = Arc::new(Notify::new());
        let (durable, durable_rx) = oneshot::channel();
        let (response, result) = oneshot::channel();
        let operation_phase = Arc::clone(&phase);
        let operation_release = Arc::clone(&release);
        owner
            .spawn(
                OperationContext::new("test_continuation", "test-continuation"),
                async move {
                    operation_phase.store(1, Ordering::SeqCst);
                    let _ = durable.send(());
                    operation_release.notified().await;
                    operation_phase.store(2, Ordering::SeqCst);
                    let _ = response.send(());
                },
            )
            .await
            .unwrap();
        let observer = tokio::spawn(result);
        durable_rx.await.unwrap();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        release.notify_one();
        owner
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(phase.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn panicked_unique_operation_clears_its_key_and_is_observed() {
        let directory = tempfile::TempDir::new().unwrap();
        let log_dir = directory.path().join("logs");
        let logger = NodeLogger::open(
            &log_dir,
            NodeIdentity::generate("operation-test".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = OperationTracker::new(Arc::clone(&logger));
        let operation_id = "panicked-operation".parse().unwrap();
        let key = OperationKey::StartJob(operation_id);
        assert!(
            owner
                .spawn_unique(
                    key.clone(),
                    OperationContext::new("start_job", "panic-correlation"),
                    async { panic!("representative plugin secret must not enter the log") },
                )
                .await
                .unwrap()
        );

        let completed = Arc::new(AtomicU8::new(0));
        let retry_deadline = Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let retry_completed = Arc::clone(&completed);
            if owner
                .spawn_unique(
                    key.clone(),
                    OperationContext::new("start_job", "retry-correlation"),
                    async move {
                        retry_completed.store(1, Ordering::SeqCst);
                    },
                )
                .await
                .unwrap()
            {
                break;
            }
            assert!(
                Instant::now() < retry_deadline,
                "panic left operation key stuck"
            );
            tokio::task::yield_now().await;
        }
        owner
            .shutdown(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        logger
            .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(1))
            .unwrap();
        let log = std::fs::read_to_string(log_dir.join("oll.log")).unwrap();
        assert!(log.contains("plugin_runtime_operation_panicked"));
        assert!(log.contains("panic-correlation"));
        assert!(!log.contains("representative plugin secret"));
    }

    #[tokio::test]
    async fn shutdown_deadline_reaps_aborted_operations_before_returning() {
        let directory = tempfile::TempDir::new().unwrap();
        let logger = NodeLogger::open(
            &directory.path().join("logs"),
            NodeIdentity::generate("operation-test".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = OperationTracker::new(logger);
        let dropped = Arc::new(AtomicBool::new(false));
        let (started, started_rx) = oneshot::channel();
        let operation_dropped = Arc::clone(&dropped);
        owner
            .spawn(
                OperationContext::new("deadline_test", "deadline-test"),
                async move {
                    let _drop_signal = DropSignal(operation_dropped);
                    let _ = started.send(());
                    std::future::pending::<()>().await;
                },
            )
            .await
            .unwrap();
        started_rx.await.unwrap();

        let result = owner.shutdown(Instant::now()).await;
        assert!(result.is_err());
        assert!(
            dropped.load(Ordering::SeqCst),
            "shutdown returned before the aborted operation future was dropped"
        );
    }

    #[tokio::test]
    async fn operation_join_failures_return_only_a_stable_message() {
        let directory = tempfile::TempDir::new().unwrap();
        let logger = NodeLogger::open(
            &directory.path().join("logs"),
            NodeIdentity::generate("operation-test".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = OperationTracker::new(logger);
        owner
            .spawn(
                OperationContext::new("join_failure_test", "join-failure-test"),
                std::future::pending(),
            )
            .await
            .unwrap();
        owner.state.lock().await.tasks[0].task.abort();

        let error = owner
            .shutdown(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "plugin runtime operation task failed");
    }
}
