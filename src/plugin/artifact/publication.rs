use std::{collections::HashMap, future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

use futures_util::FutureExt as _;
use serde_json::json;
use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::Instant,
};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{ArtifactPublishIntent, PluginArtifact, PluginError, PluginId},
};

use super::{
    ArtifactPublisher, artifact_matches_intent,
    filesystem::{self, PublishOutcome},
};

type PublicationFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Clone)]
pub(super) struct PublicationTracker {
    commands: mpsc::UnboundedSender<PublicationCommand>,
}

enum PublicationCommand {
    Spawn {
        plugin_id: PluginId,
        correlation_id: String,
        future: PublicationFuture,
        accepted: oneshot::Sender<()>,
    },
    WaitPlugin {
        plugin_id: PluginId,
        response: oneshot::Sender<()>,
    },
    Shutdown {
        deadline: Instant,
        correlation_id: String,
        response: oneshot::Sender<Result<(), PluginError>>,
    },
}

struct PublicationCompletion {
    plugin_id: PluginId,
    correlation_id: String,
    panicked: bool,
}

struct ShutdownRequest {
    deadline: Instant,
    correlation_id: String,
    response: oneshot::Sender<Result<(), PluginError>>,
}

impl PublicationTracker {
    pub(super) fn new(logger: Arc<NodeLogger>) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_tracker(receiver, logger));
        Self { commands }
    }

    async fn spawn<F>(
        &self,
        plugin_id: PluginId,
        correlation_id: String,
        future: F,
    ) -> Result<(), PluginError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (accepted, result) = oneshot::channel();
        self.commands
            .send(PublicationCommand::Spawn {
                plugin_id,
                correlation_id,
                future: Box::pin(future),
                accepted,
            })
            .map_err(|_| {
                PluginError::FailedPrecondition(
                    "artifact publication owner is shutting down".to_owned(),
                )
            })?;
        result.await.map_err(|_| {
            PluginError::Store("artifact publication owner ended unexpectedly".to_owned())
        })
    }

    pub(super) async fn wait_for_plugin(&self, plugin_id: &PluginId) -> Result<(), PluginError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(PublicationCommand::WaitPlugin {
                plugin_id: plugin_id.clone(),
                response,
            })
            .map_err(|_| {
                PluginError::FailedPrecondition(
                    "artifact publication owner is shutting down".to_owned(),
                )
            })?;
        result.await.map_err(|_| {
            PluginError::FailedPrecondition(
                "artifact publication wait was interrupted by shutdown".to_owned(),
            )
        })
    }

    pub(super) async fn shutdown(
        &self,
        deadline: Instant,
        correlation_id: &str,
    ) -> Result<(), PluginError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(PublicationCommand::Shutdown {
                deadline,
                correlation_id: correlation_id.to_owned(),
                response,
            })
            .map_err(|_| {
                PluginError::FailedPrecondition(
                    "artifact publication owner already stopped".to_owned(),
                )
            })?;
        result.await.map_err(|_| {
            PluginError::Store("artifact publication owner ended unexpectedly".to_owned())
        })?
    }
}

impl ArtifactPublisher {
    pub(super) async fn publish_owned(
        &self,
        intent: ArtifactPublishIntent,
        now: OffsetDateTime,
    ) -> Result<PluginArtifact, PluginError> {
        let publisher = self.clone();
        let plugin_id = intent.plugin_id.clone();
        let correlation_id = intent.correlation_id.clone();
        let (response, result) = oneshot::channel();
        self.publications
            .spawn(plugin_id, correlation_id, async move {
                let outcome = publisher.continue_publication(&intent, now).await;
                let _ = response.send(outcome);
            })
            .await?;
        result.await.map_err(|_| {
            PluginError::Store("artifact publication continuation ended unexpectedly".to_owned())
        })?
    }

    async fn continue_publication(
        &self,
        intent: &ArtifactPublishIntent,
        now: OffsetDateTime,
    ) -> Result<PluginArtifact, PluginError> {
        let publication = match filesystem::publish_staging_async(intent.clone()).await {
            Ok(publication) => publication,
            Err(error) => {
                self.log_recovery_deferred(intent, &error);
                return Err(error);
            }
        };
        match publication {
            PublishOutcome::Published | PublishOutcome::AlreadyMatching => {}
            PublishOutcome::Collision => {
                self.fail_publish_intent(intent, now, "artifact_destination_collision")
                    .await?;
                return Err(PluginError::AlreadyExists(
                    "artifact destination already exists".to_owned(),
                ));
            }
        }
        let artifact = match self
            .store
            .finalize_artifact_publish(intent.artifact_id, now)
            .await
        {
            Ok(artifact) => artifact,
            Err(error) => match self.store.get_artifact(intent.artifact_id).await {
                Ok(artifact) => artifact,
                Err(_) => {
                    self.log_recovery_deferred(intent, &error);
                    return Err(error);
                }
            },
        };
        if !artifact_matches_intent(&artifact, intent) {
            let error = PluginError::CorruptStore(
                "stored plugin artifact contradicts its publication intent".to_owned(),
            );
            self.log_recovery_deferred(intent, &error);
            return Err(error);
        }
        self.logger.emit(
            LogLevel::Info,
            "oll::plugin::artifact",
            "plugin_artifact_published",
            &intent.correlation_id,
            json!({
                "plugin_id": intent.plugin_id.as_str(),
                "job_id": intent.job_id.to_string(),
                "artifact_id": intent.artifact_id.to_string(),
                "bytes": intent.size_bytes,
            }),
        );
        Ok(artifact)
    }
}

async fn run_tracker(
    mut commands: mpsc::UnboundedReceiver<PublicationCommand>,
    logger: Arc<NodeLogger>,
) {
    let mut tasks = JoinSet::new();
    let mut active = HashMap::<PluginId, usize>::new();
    let mut waiters = HashMap::<PluginId, Vec<oneshot::Sender<()>>>::new();
    let mut shutdown = None::<ShutdownRequest>;

    loop {
        if tasks.is_empty()
            && let Some(shutdown) = shutdown.take()
        {
            let _ = shutdown.response.send(Ok(()));
            return;
        }
        tokio::select! {
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(joined) = joined {
                    finish_publication(joined, &mut active, &mut waiters, &logger);
                }
            }
            command = commands.recv(), if shutdown.is_none() => {
                match command {
                    Some(PublicationCommand::Spawn {
                        plugin_id,
                        correlation_id,
                        future,
                        accepted,
                    }) => {
                        *active.entry(plugin_id.clone()).or_default() += 1;
                        tasks.spawn(async move {
                            let panicked = AssertUnwindSafe(future).catch_unwind().await.is_err();
                            PublicationCompletion {
                                plugin_id,
                                correlation_id,
                                panicked,
                            }
                        });
                        let _ = accepted.send(());
                    }
                    Some(PublicationCommand::WaitPlugin { plugin_id, response }) => {
                        if active.contains_key(&plugin_id) {
                            waiters.entry(plugin_id).or_default().push(response);
                        } else {
                            let _ = response.send(());
                        }
                    }
                    Some(PublicationCommand::Shutdown {
                        deadline,
                        correlation_id,
                        response,
                    }) => {
                        shutdown = Some(ShutdownRequest {
                            deadline,
                            correlation_id,
                            response,
                        });
                    }
                    None => {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return;
                    }
                }
            }
            _ = tokio::time::sleep_until(
                shutdown.as_ref().map_or_else(Instant::now, |request| request.deadline)
            ), if shutdown.is_some() => {
                let shutdown = shutdown.take().expect("guarded publication shutdown");
                let aborted = active.values().copied().sum::<usize>();
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                logger.emit(
                    LogLevel::Warn,
                    "oll::plugin::artifact",
                    "plugin_artifact_publications_aborted",
                    &shutdown.correlation_id,
                    json!({
                        "artifact_publication_count": aborted,
                        "error_code": "shutdown_deadline_exceeded",
                    }),
                );
                let _ = shutdown.response.send(Err(PluginError::FailedPrecondition(
                    "artifact publication exceeded the node shutdown deadline".to_owned(),
                )));
                return;
            }
        }
    }
}

fn finish_publication(
    joined: Result<PublicationCompletion, tokio::task::JoinError>,
    active: &mut HashMap<PluginId, usize>,
    waiters: &mut HashMap<PluginId, Vec<oneshot::Sender<()>>>,
    logger: &NodeLogger,
) {
    let Ok(completion) = joined else {
        // Publication futures catch panics themselves. A JoinError is possible
        // only during tracker teardown, where no removal waiter may proceed.
        return;
    };
    if completion.panicked {
        logger.emit(
            LogLevel::Error,
            "oll::plugin::artifact",
            "plugin_artifact_publication_task_failed",
            &completion.correlation_id,
            json!({
                "plugin_id": completion.plugin_id.as_str(),
                "error_code": "publication_task_panicked",
            }),
        );
    }
    let Some(count) = active.get_mut(&completion.plugin_id) else {
        return;
    };
    *count -= 1;
    if *count == 0 {
        active.remove(&completion.plugin_id);
        if let Some(waiters) = waiters.remove(&completion.plugin_id) {
            for waiter in waiters {
                let _ = waiter.send(());
            }
        }
    }
}
