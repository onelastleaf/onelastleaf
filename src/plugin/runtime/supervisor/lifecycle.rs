use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use time::OffsetDateTime;
use tokio::{
    sync::{RwLock, oneshot},
    task::JoinSet,
};

use crate::{
    node::logging::LogLevel,
    plugin::{ObservedPluginState, PluginError, PluginId, PluginInstanceId},
};

#[cfg(test)]
use super::super::InstanceSender;
use super::super::{InstanceNotice, RuntimeDependencies};
use super::{
    ActiveInstance, ControllerCommand,
    reconciliation::{lifecycle_error, retryable_reconcile_error, schedule_reconcile_retry},
};

pub(super) async fn handle_notice(
    dependencies: &RuntimeDependencies,
    active: &Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    notice: InstanceNotice,
    shutting_down: bool,
    removal_blocks: &HashSet<PluginId>,
    removal_waiters: &mut HashMap<PluginId, oneshot::Sender<Result<(), PluginError>>>,
    reconcile_timers: &mut JoinSet<ControllerCommand>,
) {
    match notice {
        InstanceNotice::Spawned {
            plugin_id,
            instance_id,
            process_id,
            started_at,
        } => {
            if let Some(instance) = active.write().await.get_mut(&plugin_id)
                && instance.instance_id == instance_id
            {
                instance.process_id = Some(process_id);
                instance.started_at = Some(started_at);
            }
        }
        InstanceNotice::Ready {
            plugin_id,
            instance_id,
            actions,
            correlation_id,
        } => {
            let became_ready = {
                let mut active = active.write().await;
                active.get_mut(&plugin_id).is_some_and(|instance| {
                    transition_to_ready(instance, instance_id, actions, OffsetDateTime::now_utc())
                })
            };
            if became_ready {
                if let Err(error) = dependencies
                    .store
                    .record_instance_ready(&plugin_id, instance_id)
                    .await
                {
                    lifecycle_error(
                        dependencies,
                        "plugin_ready_persistence_failed",
                        &correlation_id,
                        Some(&plugin_id),
                        &error,
                    );
                    if retryable_reconcile_error(&error) {
                        schedule_reconcile_retry(
                            reconcile_timers,
                            plugin_id.clone(),
                            correlation_id.clone(),
                            0,
                        );
                    }
                }
                dependencies.logger.emit(
                    LogLevel::Info,
                    "oll::plugin",
                    "plugin_process_ready",
                    &correlation_id,
                    serde_json::json!({
                        "plugin_id": plugin_id.as_str(),
                        "plugin_instance_id": instance_id.to_string(),
                    }),
                );
            }
        }
        InstanceNotice::Ended {
            plugin_id,
            instance_id,
            failure,
            correlation_id,
            ended_at,
        } => {
            super::settlement::handle_ended_notice(
                dependencies,
                active,
                plugin_id,
                instance_id,
                failure,
                correlation_id,
                ended_at,
                shutting_down,
                removal_blocks,
                removal_waiters,
                reconcile_timers,
            )
            .await;
        }
    }
}

fn transition_to_ready(
    instance: &mut ActiveInstance,
    instance_id: PluginInstanceId,
    actions: Vec<super::PluginAction>,
    ready_at: OffsetDateTime,
) -> bool {
    if instance.instance_id != instance_id || instance.state != ObservedPluginState::Starting {
        return false;
    }
    instance.state = ObservedPluginState::Ready;
    instance.ready_at = Some(ready_at);
    instance.actions = actions;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_ready_cannot_reverse_a_stopping_transition() {
        let (work, _work_receiver) = tokio::sync::mpsc::channel(1);
        let (shutdown, _shutdown_receiver) = tokio::sync::watch::channel(None);
        let instance_id = PluginInstanceId::new();
        let mut instance = ActiveInstance {
            instance_id,
            generation: uuid::Uuid::new_v4(),
            sender: InstanceSender { work, shutdown },
            state: ObservedPluginState::Stopping,
            process_id: Some(17),
            started_at: None,
            ready_at: None,
            actions: Vec::new(),
        };

        assert!(!transition_to_ready(
            &mut instance,
            instance_id,
            vec![super::super::PluginAction {
                name: "late".to_owned(),
                description: "must be ignored".to_owned(),
            }],
            OffsetDateTime::UNIX_EPOCH,
        ));
        assert_eq!(instance.state, ObservedPluginState::Stopping);
        assert!(instance.ready_at.is_none());
        assert!(instance.actions.is_empty());
    }
}
