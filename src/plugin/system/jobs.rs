use std::{collections::BTreeMap, sync::Arc, time::Duration};

use time::OffsetDateTime;
use tokio::{sync::oneshot, time::Instant};

use crate::plugin::{
    JobCancellationReason, JobState, NormalizedJobPayload, PluginArtifact, PluginError, PluginJob,
    PluginJobId, PluginName, PluginOperationId, PluginSelector, PluginStore,
    runtime::{JOB_RESPONSE_TIMEOUT, PluginSupervisor},
};

use super::{
    PluginRuntime,
    operations::{OperationContext, OperationKey, OperationTracker, operation_result_lost},
};

#[derive(Clone, Debug)]
pub struct PluginJobListEntry {
    pub job: PluginJob,
    pub plugin_name: PluginName,
}

#[derive(Clone, Debug)]
pub struct PluginJobInspection {
    pub job: PluginJob,
    pub plugin_name: PluginName,
    pub artifacts: Vec<PluginArtifact>,
}

impl PluginRuntime {
    pub async fn start_job(
        self: &Arc<Self>,
        selector: &PluginSelector,
        operation_id: &PluginOperationId,
        action: String,
        arguments: Vec<String>,
        deadline: Option<OffsetDateTime>,
        correlation_id: &str,
    ) -> Result<PluginJob, PluginError> {
        let plugin = self.store.get_plugin(selector).await?;
        let payload = NormalizedJobPayload::new(plugin.plugin_id, action, arguments, deadline)?;
        if let Some(existing) = retained_job(&self.store, operation_id, &payload).await?
            && existing.state != JobState::Dispatching
        {
            return Ok(existing);
        }
        let operation_id = operation_id.clone();
        let operation_key = OperationKey::StartJob(operation_id.clone());
        let task_operation_id = operation_id.clone();
        let supervisor = Arc::clone(&self.supervisor);
        let operations = Arc::clone(&self.operations);
        let store = self.store.clone();
        let correlation_id = correlation_id.to_owned();
        let payload_for_task = payload.clone();
        let (response, result) = oneshot::channel();
        let started = self
            .operations
            .spawn_unique(
                operation_key,
                OperationContext::new("start_job", correlation_id.clone()),
                async move {
                    let outcome = supervisor
                        .start_job(&task_operation_id, &payload_for_task, &correlation_id)
                        .await;
                    if let Ok(job) = &outcome {
                        schedule_job_deadline(
                            &operations,
                            Arc::clone(&supervisor),
                            store,
                            job.clone(),
                        )
                        .await;
                    }
                    let _ = response.send(outcome);
                },
            )
            .await?;
        if started {
            match tokio::time::timeout(JOB_RESPONSE_TIMEOUT, result).await {
                Ok(Ok(result)) => return result,
                Ok(Err(_)) => return Err(operation_result_lost(())),
                Err(_) => {}
            }
        }
        wait_for_started_job(&self.store, &operation_id, &payload).await
    }

    pub async fn list_jobs(&self, limit: usize) -> Result<Vec<PluginJobListEntry>, PluginError> {
        if !(1..=1000).contains(&limit) {
            return Err(PluginError::InvalidArgument(
                "plugin job list limit must be in 1..=1000".to_owned(),
            ));
        }
        let names = self
            .store
            .list_plugins()
            .await?
            .into_iter()
            .map(|plugin| (plugin.plugin_id, plugin.plugin_name))
            .collect::<BTreeMap<_, _>>();
        self.store
            .list_jobs(None, limit)
            .await?
            .into_iter()
            .map(|job| {
                let plugin_name = names.get(&job.payload.plugin_id).cloned().ok_or_else(|| {
                    PluginError::CorruptStore(format!(
                        "plugin job {} has no installed plugin identity",
                        job.job_id
                    ))
                })?;
                Ok(PluginJobListEntry { job, plugin_name })
            })
            .collect()
    }

    pub async fn inspect_job(
        &self,
        job_id: PluginJobId,
    ) -> Result<PluginJobInspection, PluginError> {
        let job = self.store.get_job(job_id).await?;
        let plugin_name = self
            .store
            .get_plugin(&PluginSelector::Id(job.payload.plugin_id.clone()))
            .await?
            .plugin_name;
        let artifacts = self.store.artifacts_for_job(job_id).await?;
        Ok(PluginJobInspection {
            job,
            plugin_name,
            artifacts,
        })
    }

    pub async fn stop_job(
        self: &Arc<Self>,
        job_id: PluginJobId,
        correlation_id: &str,
    ) -> Result<PluginJob, PluginError> {
        let current = self.store.get_job(job_id).await?;
        if current.state.is_terminal() {
            return Ok(current);
        }
        let supervisor = Arc::clone(&self.supervisor);
        let store = self.store.clone();
        let correlation_id = correlation_id.to_owned();
        let (response, result) = oneshot::channel();
        let started = self
            .operations
            .spawn_unique(
                OperationKey::CancelJob(job_id),
                OperationContext::new("cancel_job", correlation_id.clone()),
                async move {
                    let outcome = supervisor
                        .cancel_job(job_id, JobCancellationReason::UserRequest, &correlation_id)
                        .await;
                    match outcome {
                        Ok(job) => {
                            let _ = response.send(Ok(job.clone()));
                            wait_for_cancel_terminal(&store, job_id).await;
                        }
                        Err(error) => {
                            let _ = response.send(Err(error));
                        }
                    }
                },
            )
            .await?;
        if started {
            match tokio::time::timeout(JOB_RESPONSE_TIMEOUT, result).await {
                Ok(Ok(result)) => return result,
                Ok(Err(_)) => return Err(operation_result_lost(())),
                Err(_) => {}
            }
        }
        wait_for_cancellation_started(&self.store, job_id).await
    }
}

async fn retained_job(
    store: &PluginStore,
    operation_id: &PluginOperationId,
    payload: &NormalizedJobPayload,
) -> Result<Option<PluginJob>, PluginError> {
    match store.job_by_operation_id(operation_id).await {
        Ok(job) if job.payload == *payload => Ok(Some(job)),
        Ok(_) => Err(PluginError::AlreadyExists(
            "plugin operation ID is already bound to another normalized payload".to_owned(),
        )),
        Err(PluginError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn wait_for_started_job(
    store: &PluginStore,
    operation_id: &PluginOperationId,
    payload: &NormalizedJobPayload,
) -> Result<PluginJob, PluginError> {
    let deadline = Instant::now() + JOB_RESPONSE_TIMEOUT;
    loop {
        if let Some(job) = retained_job(store, operation_id, payload).await?
            && job.state != JobState::Dispatching
        {
            return Ok(job);
        }
        if Instant::now() >= deadline {
            return Err(PluginError::FailedPrecondition(
                "plugin did not accept the job before the deadline".to_owned(),
            ));
        }
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(10)).min(deadline)).await;
    }
}

async fn wait_for_cancellation_started(
    store: &PluginStore,
    job_id: PluginJobId,
) -> Result<PluginJob, PluginError> {
    let deadline = Instant::now() + JOB_RESPONSE_TIMEOUT;
    loop {
        let job = store.get_job(job_id).await?;
        if job.state != JobState::Dispatching && job.state != JobState::Running {
            return Ok(job);
        }
        if Instant::now() >= deadline {
            return Err(PluginError::FailedPrecondition(
                "plugin job cancellation was not dispatched before the deadline".to_owned(),
            ));
        }
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(10)).min(deadline)).await;
    }
}

async fn wait_for_cancel_terminal(store: &PluginStore, job_id: PluginJobId) {
    let deadline = Instant::now() + JOB_RESPONSE_TIMEOUT;
    loop {
        if store
            .get_job(job_id)
            .await
            .is_ok_and(|job| job.state.is_terminal())
            || Instant::now() >= deadline
        {
            return;
        }
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(10)).min(deadline)).await;
    }
}

async fn schedule_job_deadline(
    operations: &Arc<OperationTracker>,
    supervisor: Arc<PluginSupervisor>,
    store: PluginStore,
    job: PluginJob,
) {
    if job.state.is_terminal() {
        return;
    }
    let owner = Arc::downgrade(operations);
    let job_id = job.job_id;
    let correlation_id = job.correlation_id.clone();
    let deadline = job.absolute_deadline;
    let _ = operations
        .spawn_timer(
            OperationKey::JobDeadline(job_id),
            OperationContext::new("job_deadline", correlation_id.clone()),
            async move {
                let delay = deadline - OffsetDateTime::now_utc();
                if delay.is_positive() {
                    tokio::select! {
                        () = store.wait_for_job_terminal(job_id) => return,
                        () = tokio::time::sleep(delay.unsigned_abs()) => {}
                    }
                }
                let Some(owner) = owner.upgrade() else {
                    return;
                };
                let cancellation_supervisor = supervisor;
                let cancellation_store = store;
                let cancellation_context =
                    OperationContext::new("cancel_job_deadline", correlation_id.clone());
                let _ = owner
                    .spawn_unique(
                        OperationKey::CancelJob(job_id),
                        cancellation_context,
                        async move {
                            if cancellation_supervisor
                                .cancel_job(
                                    job_id,
                                    JobCancellationReason::Deadline,
                                    &correlation_id,
                                )
                                .await
                                .is_ok()
                            {
                                wait_for_cancel_terminal(&cancellation_store, job_id).await;
                            }
                        },
                    )
                    .await;
            },
        )
        .await;
}

#[cfg(test)]
impl PluginRuntime {
    pub(crate) async fn has_job_deadline_operation(&self, job_id: PluginJobId) -> bool {
        self.operations
            .contains_key(&OperationKey::JobDeadline(job_id))
            .await
    }
}
