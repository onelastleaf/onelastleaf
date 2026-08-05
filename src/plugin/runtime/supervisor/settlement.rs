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

use super::super::RuntimeDependencies;
use super::{
    ActiveInstance, ControllerCommand,
    reconciliation::{lifecycle_error, reconcile_retry_backoff, schedule_reconcile},
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ended_notice(
    dependencies: &RuntimeDependencies,
    active: &Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    plugin_id: PluginId,
    instance_id: PluginInstanceId,
    failure: Option<String>,
    correlation_id: String,
    ended_at: OffsetDateTime,
    shutting_down: bool,
    removal_blocks: &HashSet<PluginId>,
    removal_waiters: &mut HashMap<PluginId, oneshot::Sender<Result<(), PluginError>>>,
    timers: &mut JoinSet<ControllerCommand>,
) {
    let expected = {
        let mut active = active.write().await;
        let Some(instance) = active
            .get_mut(&plugin_id)
            .filter(|instance| instance.instance_id == instance_id)
        else {
            return;
        };
        let expected = instance.state == ObservedPluginState::Stopping;
        if !expected {
            instance.state = ObservedPluginState::Failed;
        }
        expected
    };
    settle_ended_instance(
        dependencies,
        active,
        plugin_id,
        instance_id,
        expected,
        failure,
        correlation_id,
        ended_at,
        0,
        shutting_down,
        removal_blocks,
        removal_waiters,
        timers,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn settle_ended_instance(
    dependencies: &RuntimeDependencies,
    active: &Arc<RwLock<BTreeMap<PluginId, ActiveInstance>>>,
    plugin_id: PluginId,
    instance_id: PluginInstanceId,
    expected: bool,
    failure: Option<String>,
    correlation_id: String,
    ended_at: OffsetDateTime,
    retry_attempt: u32,
    shutting_down: bool,
    removal_blocks: &HashSet<PluginId>,
    removal_waiters: &mut HashMap<PluginId, oneshot::Sender<Result<(), PluginError>>>,
    timers: &mut JoinSet<ControllerCommand>,
) {
    if !active
        .read()
        .await
        .get(&plugin_id)
        .is_some_and(|instance| instance.instance_id == instance_id)
    {
        return;
    }
    let lifecycle_failure = if expected {
        None
    } else {
        Some(failure.as_deref().unwrap_or("plugin_process_exited"))
    };
    let job_error_code = if expected {
        "plugin_process_stopped"
    } else {
        "plugin_process_failed"
    };
    if let Err(error) = dependencies
        .store
        .settle_ended_instance(
            &plugin_id,
            instance_id,
            lifecycle_failure,
            ended_at,
            job_error_code,
        )
        .await
    {
        lifecycle_error(
            dependencies,
            "plugin_process_settlement_failed",
            &correlation_id,
            Some(&plugin_id),
            &error,
        );
        let next_attempt = retry_attempt.saturating_add(1);
        let delay = reconcile_retry_backoff(next_attempt);
        timers.spawn(async move {
            tokio::time::sleep(delay).await;
            ControllerCommand::SettleEnded {
                plugin_id,
                instance_id,
                expected,
                failure,
                correlation_id,
                ended_at,
                retry_attempt: next_attempt,
            }
        });
        return;
    }

    let removed = {
        let mut active = active.write().await;
        if active
            .get(&plugin_id)
            .is_some_and(|instance| instance.instance_id == instance_id)
        {
            active.remove(&plugin_id)
        } else {
            None
        }
    };
    if removed.is_none() {
        return;
    }
    dependencies.logger.emit(
        if expected {
            LogLevel::Info
        } else {
            LogLevel::Warn
        },
        "oll::plugin",
        if expected {
            "plugin_process_stopped"
        } else {
            "plugin_process_failed"
        },
        &correlation_id,
        serde_json::json!({
            "plugin_id": plugin_id.as_str(),
            "plugin_instance_id": instance_id.to_string(),
            "error_code": lifecycle_failure,
        }),
    );
    if let Some(response) = removal_waiters.remove(&plugin_id) {
        let _ = response.send(Ok(()));
    }
    if !removal_blocks.contains(&plugin_id) {
        prune_unused_generations(dependencies, &plugin_id, &correlation_id).await;
    }
    if !shutting_down && !removal_blocks.contains(&plugin_id) {
        schedule_reconcile(
            timers,
            plugin_id,
            correlation_id,
            std::time::Duration::ZERO,
            0,
        );
    }
}

async fn prune_unused_generations(
    dependencies: &RuntimeDependencies,
    plugin_id: &PluginId,
    correlation_id: &str,
) {
    let _gate = dependencies.package_gates.lock(plugin_id).await;
    let plugin = match dependencies
        .store
        .get_plugin(&crate::plugin::PluginSelector::Id(plugin_id.clone()))
        .await
    {
        Ok(plugin) => plugin,
        Err(error) => {
            lifecycle_error(
                dependencies,
                "plugin_generation_cleanup_failed",
                correlation_id,
                Some(plugin_id),
                &error,
            );
            return;
        }
    };
    let mut retained = std::collections::BTreeSet::from([plugin.current_generation]);
    retained.extend(plugin.running_generation);
    if dependencies
        .packages
        .prune_generations(plugin_id, &retained)
        .is_err()
    {
        dependencies.logger.emit(
            LogLevel::Warn,
            "oll::plugin",
            "plugin_generation_cleanup_failed",
            correlation_id,
            serde_json::json!({
                "plugin_id": plugin_id.as_str(),
                "error_code": "package_cleanup_failed",
            }),
        );
    }
}
