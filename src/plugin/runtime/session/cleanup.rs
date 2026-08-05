use std::collections::HashMap;

use tokio::task::{Id, JoinHandle, JoinSet};

use crate::{
    node::logging::LogLevel,
    plugin::{InstalledPlugin, PluginInstanceId},
};

use super::{super::RuntimeDependencies, outcome::SessionOutcome};

#[allow(clippy::too_many_arguments)]
pub(super) async fn finish_session(
    dependencies: &RuntimeDependencies,
    plugin: &InstalledPlugin,
    instance_id: PluginInstanceId,
    session_id: &str,
    outcome: SessionOutcome,
    mut tasks: JoinSet<()>,
    mut task_contexts: HashMap<Id, String>,
    mut artifact_task: JoinHandle<()>,
) -> SessionOutcome {
    let task_drain = async {
        while let Some(completed) = tasks.join_next_with_id().await {
            match completed {
                Ok((task_id, ())) => {
                    task_contexts.remove(&task_id);
                }
                Err(error) => {
                    let correlation_id = task_contexts
                        .remove(&error.id())
                        .unwrap_or_else(|| outcome.correlation_id.clone());
                    if error.is_panic() {
                        dependencies.logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_session_worker_failed",
                            &correlation_id,
                            serde_json::json!({
                                "plugin_id": plugin.plugin_id.as_str(),
                                "plugin_instance_id": instance_id.to_string(),
                                "error_code": "task_panicked",
                            }),
                        );
                    }
                }
            }
        }
    };
    let artifact_drain = async {
        let _ = (&mut artifact_task).await;
    };
    if tokio::time::timeout_at(outcome.absolute_deadline, async {
        tokio::join!(task_drain, artifact_drain);
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        artifact_task.abort();
        while let Some(completed) = tasks.join_next_with_id().await {
            let task_id = match completed {
                Ok((task_id, ())) => task_id,
                Err(error) => error.id(),
            };
            task_contexts.remove(&task_id);
        }
        let _ = artifact_task.await;
    }
    let config = dependencies.config.clone();
    let cleanup_session_id = session_id.to_owned();
    let cleanup =
        tokio::task::spawn_blocking(move || config.end_plugin_session(&cleanup_session_id));
    if !matches!(
        tokio::time::timeout_at(outcome.absolute_deadline, cleanup).await,
        Ok(Ok(Ok(())))
    ) {
        dependencies.logger.emit(
            LogLevel::Warn,
            "oll::plugin",
            "plugin_config_session_cleanup_failed",
            &outcome.correlation_id,
            serde_json::json!({
                "plugin_id": plugin.plugin_id.as_str(),
                "plugin_instance_id": instance_id.to_string(),
            }),
        );
    }
    outcome
}
