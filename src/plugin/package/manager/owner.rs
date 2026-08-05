use std::{collections::HashMap, future::Future, sync::Arc};

use serde_json::json;
use tokio::{
    sync::{Mutex, oneshot},
    task::JoinSet,
    time::Instant,
};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{PluginError, PluginId},
};

struct PackageTaskState {
    accepting: bool,
    tasks: JoinSet<()>,
    contexts: HashMap<tokio::task::Id, DurablePublishContext>,
    failed_tasks: usize,
}

pub(super) struct DurablePublishContext {
    pub(super) plugin_id: PluginId,
    pub(super) operation_id: String,
    pub(super) correlation_id: String,
}

pub(in crate::plugin::package) struct PackageTaskOwner {
    state: Mutex<PackageTaskState>,
    pub(super) logger: Arc<NodeLogger>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishPause {
    AfterIntent,
    AfterCurrentSwitch,
    PanicAfterIntent,
}

#[cfg(test)]
pub(super) struct PublishTestHook {
    pub(super) pause: PublishPause,
    reached: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
impl PublishTestHook {
    pub(super) fn new(pause: PublishPause) -> Arc<Self> {
        Arc::new(Self {
            pause,
            reached: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    pub(super) async fn wait_until_reached(&self) {
        self.reached
            .acquire()
            .await
            .expect("publish test hook remains open")
            .forget();
    }

    pub(super) fn reached(&self) {
        self.reached.add_permits(1);
    }

    pub(super) async fn wait_for_release(&self) {
        self.release
            .acquire()
            .await
            .expect("publish test hook remains open")
            .forget();
    }

    pub(super) fn resume(&self) {
        self.release.add_permits(1);
    }
}

impl PackageTaskOwner {
    pub(in crate::plugin::package) fn new(logger: Arc<NodeLogger>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PackageTaskState {
                accepting: true,
                tasks: JoinSet::new(),
                contexts: HashMap::new(),
                failed_tasks: 0,
            }),
            logger,
        })
    }

    pub(super) async fn spawn_publish<F>(
        &self,
        context: DurablePublishContext,
        future: F,
    ) -> Result<(), PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self.state.lock().await;
        self.reap_completed(&mut state);
        if !state.accepting {
            return Err(PluginError::FailedPrecondition(
                "plugin package manager is shutting down".to_owned(),
            ));
        }
        let task = state.tasks.spawn(future);
        state.contexts.insert(task.id(), context);
        Ok(())
    }

    pub(in crate::plugin::package) async fn spawn_process<F, T>(
        &self,
        future: F,
    ) -> Result<oneshot::Receiver<T>, PluginError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.state.lock().await;
        self.reap_completed(&mut state);
        if !state.accepting {
            return Err(PluginError::FailedPrecondition(
                "plugin package manager is shutting down".to_owned(),
            ));
        }
        let (sender, receiver) = oneshot::channel();
        state.tasks.spawn(async move {
            let result = future.await;
            let _ = sender.send(result);
        });
        Ok(receiver)
    }

    pub(super) async fn is_accepting(&self) -> bool {
        let mut state = self.state.lock().await;
        self.reap_completed(&mut state);
        state.accepting
    }

    pub(in crate::plugin::package) async fn shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), PluginError> {
        let (mut tasks, mut contexts, mut failed_tasks) = {
            let mut state = self.state.lock().await;
            self.reap_completed(&mut state);
            state.accepting = false;
            (
                std::mem::take(&mut state.tasks),
                std::mem::take(&mut state.contexts),
                std::mem::take(&mut state.failed_tasks),
            )
        };
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next_with_id()).await {
                Ok(Some(Ok((task_id, ())))) => {
                    contexts.remove(&task_id);
                }
                Ok(Some(Err(error))) => {
                    failed_tasks += 1;
                    self.log_task_failure(contexts.remove(&error.id()), &error);
                }
                Ok(None) => break,
                Err(_) => {
                    tasks.abort_all();
                    while let Some(completed) = tasks.join_next_with_id().await {
                        let task_id = match completed {
                            Ok((task_id, ())) => task_id,
                            Err(error) => error.id(),
                        };
                        if let Some(context) = contexts.remove(&task_id) {
                            self.logger.emit(
                                LogLevel::Error,
                                "oll::plugin::package",
                                "plugin_package_publication_task_failed",
                                &context.correlation_id,
                                json!({
                                    "plugin_id": context.plugin_id.as_str(),
                                    "package_operation_id": context.operation_id,
                                    "error_code": "shutdown_deadline_exceeded",
                                }),
                            );
                        }
                    }
                    debug_assert!(contexts.is_empty());
                    return Err(PluginError::FailedPrecondition(
                        "plugin package work exceeded the node shutdown deadline".to_owned(),
                    ));
                }
            }
        }
        if failed_tasks == 0 {
            Ok(())
        } else {
            Err(PluginError::FailedPrecondition(
                "one or more plugin package tasks failed".to_owned(),
            ))
        }
    }

    fn reap_completed(&self, state: &mut PackageTaskState) {
        while let Some(completed) = state.tasks.try_join_next_with_id() {
            match completed {
                Ok((task_id, ())) => {
                    state.contexts.remove(&task_id);
                }
                Err(error) => {
                    state.failed_tasks += 1;
                    let context = state.contexts.remove(&error.id());
                    self.log_task_failure(context, &error);
                }
            }
        }
    }

    fn log_task_failure(
        &self,
        context: Option<DurablePublishContext>,
        error: &tokio::task::JoinError,
    ) {
        let Some(context) = context else {
            return;
        };
        self.logger.emit(
            LogLevel::Error,
            "oll::plugin::package",
            "plugin_package_publication_task_failed",
            &context.correlation_id,
            json!({
                "plugin_id": context.plugin_id.as_str(),
                "package_operation_id": context.operation_id,
                "error_code": if error.is_panic() { "task_panicked" } else { "task_cancelled" },
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::*;

    struct DropNotice(Arc<AtomicBool>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn shutdown_drains_aborted_tasks_before_returning() {
        let directory = tempfile::TempDir::new().unwrap();
        let logger = NodeLogger::open(
            &directory.path().join("logs"),
            crate::node::identity::NodeIdentity::generate("owner-tests".parse().unwrap()),
        )
        .unwrap();
        let owner = PackageTaskOwner::new(logger);
        let dropped = Arc::new(AtomicBool::new(false));
        let reached = Arc::new(tokio::sync::Semaphore::new(0));
        let task_dropped = Arc::clone(&dropped);
        let task_reached = Arc::clone(&reached);
        let _result = owner
            .spawn_process(async move {
                let _drop_notice = DropNotice(task_dropped);
                task_reached.add_permits(1);
                pending::<()>().await;
            })
            .await
            .unwrap();
        reached
            .acquire()
            .await
            .expect("package task test semaphore remains open")
            .forget();

        let shutdown = owner.shutdown(Instant::now()).await;

        assert!(matches!(
            shutdown,
            Err(PluginError::FailedPrecondition(message))
                if message == "plugin package work exceeded the node shutdown deadline"
        ));
        assert!(dropped.load(Ordering::Acquire));
    }
}
