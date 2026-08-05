use std::sync::Arc;

use time::OffsetDateTime;
use tokio::sync::oneshot;

use crate::{
    node::logging::LogLevel,
    plugin::{
        JobAdmission, JobCancellationReason, JobState, NormalizedJobPayload, PluginError,
        PluginJob, PluginJobId, PluginOperationId, PluginStore,
    },
};

use super::super::{InstanceCommand, InstanceSender, JOB_RESPONSE_TIMEOUT};
use super::{ActiveInstance, PluginSupervisor, require_correlation};

impl PluginSupervisor {
    pub async fn start_job(
        self: &Arc<Self>,
        operation_id: &PluginOperationId,
        payload: &NormalizedJobPayload,
        correlation_id: &str,
    ) -> Result<PluginJob, PluginError> {
        require_correlation(correlation_id)?;
        if let Some(existing) =
            retained_operation(&self.dependencies.store, operation_id, payload).await?
        {
            if existing.state != JobState::Dispatching {
                return Ok(existing);
            }
            let instance = self.ready_instance(&payload.plugin_id).await?;
            if instance.instance_id != existing.plugin_instance_id {
                return self
                    .fail_dispatch(
                        existing,
                        "plugin_session_ended",
                        "admitted job belongs to a prior plugin session",
                    )
                    .await;
            }
            return self.dispatch_admitted_job(existing, instance).await;
        }
        let instance = self.ready_instance(&payload.plugin_id).await?;
        if !instance
            .actions
            .iter()
            .any(|action| action.name == payload.action)
        {
            return Err(PluginError::InvalidArgument(format!(
                "plugin {} does not declare action {}",
                payload.plugin_id, payload.action
            )));
        }
        let admission = self
            .dependencies
            .store
            .admit_job(
                operation_id,
                payload,
                instance.instance_id,
                OffsetDateTime::now_utc(),
                correlation_id,
            )
            .await?;
        let job = match admission {
            JobAdmission::Created(job) => job,
            JobAdmission::Existing(job) if job.state != JobState::Dispatching => return Ok(job),
            JobAdmission::Existing(job) => job,
        };

        self.dispatch_admitted_job(job, instance).await
    }

    async fn dispatch_admitted_job(
        &self,
        job: PluginJob,
        instance: ActiveInstance,
    ) -> Result<PluginJob, PluginError> {
        if !instance
            .actions
            .iter()
            .any(|action| action.name == job.payload.action)
        {
            return self
                .fail_dispatch(
                    job,
                    "invalid_argument",
                    "plugin no longer declares the admitted job action",
                )
                .await;
        }
        self.dependencies.logger.emit(
            LogLevel::Info,
            "oll::plugin::job",
            "plugin_job_dispatch_started",
            &job.correlation_id,
            serde_json::json!({
                "plugin_id": job.payload.plugin_id.as_str(),
                "plugin_instance_id": instance.instance_id.to_string(),
                "job_id": job.job_id.to_string(),
                "operation_id": job.operation_id.as_str(),
            }),
        );
        let (response, result) = oneshot::channel();
        if instance
            .sender
            .send_work(InstanceCommand::StartJob {
                job: job.clone(),
                response,
            })
            .await
            .is_err()
        {
            return self
                .fail_dispatch(
                    job,
                    "plugin_session_ended",
                    "plugin session ended before dispatch",
                )
                .await;
        }
        match tokio::time::timeout(JOB_RESPONSE_TIMEOUT, result).await {
            Ok(Ok(Ok(job))) => Ok(job),
            Ok(Ok(Err(error))) => {
                self.fail_dispatch(
                    job,
                    error.code(),
                    "plugin rejected or failed to acknowledge the job",
                )
                .await
            }
            Ok(Err(_)) | Err(_) => {
                self.fail_dispatch(
                    job,
                    "job_acceptance_timeout",
                    "plugin did not accept the job before the deadline",
                )
                .await
            }
        }
    }

    pub async fn cancel_job(
        &self,
        job_id: PluginJobId,
        reason: JobCancellationReason,
        correlation_id: &str,
    ) -> Result<PluginJob, PluginError> {
        require_correlation(correlation_id)?;
        let cancellation = self
            .dependencies
            .store
            .begin_job_cancellation(job_id, reason, OffsetDateTime::now_utc())
            .await?;
        if !cancellation.needs_request_dispatch() {
            return Ok(cancellation.job);
        }
        let reason = cancellation.job.cancellation_reason.unwrap_or(reason);
        let instance = self
            .active
            .read()
            .await
            .get(&cancellation.job.payload.plugin_id)
            .filter(|instance| instance.instance_id == cancellation.job.plugin_instance_id)
            .cloned();
        let Some(instance) = instance else {
            return self
                .fail_dispatch(
                    cancellation.job,
                    "plugin_session_ended",
                    "plugin session ended before cancellation",
                )
                .await;
        };
        if let Err(error) =
            dispatch_job_cancellation(&instance.sender, &cancellation.job, reason).await
        {
            return self
                .fail_dispatch(
                    cancellation.job,
                    error.code(),
                    "plugin session ended before cancellation request dispatch",
                )
                .await;
        }
        Ok(cancellation.job)
    }

    async fn ready_instance(
        &self,
        plugin_id: &crate::plugin::PluginId,
    ) -> Result<ActiveInstance, PluginError> {
        self.active
            .read()
            .await
            .get(plugin_id)
            .filter(|instance| instance.state == crate::plugin::ObservedPluginState::Ready)
            .cloned()
            .ok_or_else(|| {
                PluginError::FailedPrecondition(format!(
                    "plugin {plugin_id} has no ready runtime session"
                ))
            })
    }

    async fn fail_dispatch(
        &self,
        job: PluginJob,
        error_code: &str,
        message: &str,
    ) -> Result<PluginJob, PluginError> {
        let finished = self
            .dependencies
            .store
            .finish_job(
                job.job_id,
                job.plugin_instance_id,
                JobState::Failed,
                None,
                Some(error_code),
                Some(message),
                OffsetDateTime::now_utc(),
            )
            .await?;
        self.dependencies.logger.emit(
            LogLevel::Warn,
            "oll::plugin::job",
            "plugin_job_failed",
            &finished.correlation_id,
            serde_json::json!({
                "plugin_id": finished.payload.plugin_id.as_str(),
                "plugin_instance_id": finished.plugin_instance_id.to_string(),
                "job_id": finished.job_id.to_string(),
                "error_code": error_code,
            }),
        );
        Ok(finished)
    }
}

pub(in crate::plugin::runtime) async fn dispatch_job_cancellation(
    sender: &InstanceSender,
    job: &PluginJob,
    reason: JobCancellationReason,
) -> Result<(), PluginError> {
    let (dispatched, result) = oneshot::channel();
    sender
        .send_work(InstanceCommand::CancelJob {
            job: job.clone(),
            reason,
            dispatched,
        })
        .await
        .map_err(|_| {
            PluginError::FailedPrecondition(
                "plugin session ended before cancellation request dispatch".to_owned(),
            )
        })?;
    result.await.map_err(|_| {
        PluginError::FailedPrecondition(
            "plugin session ended before cancellation request dispatch".to_owned(),
        )
    })?
}

pub(in crate::plugin::runtime) async fn retained_operation(
    store: &PluginStore,
    operation_id: &PluginOperationId,
    payload: &NormalizedJobPayload,
) -> Result<Option<PluginJob>, PluginError> {
    match store.job_by_operation_id(operation_id).await {
        Ok(existing) if existing.payload == *payload => Ok(Some(existing)),
        Ok(_) => Err(PluginError::AlreadyExists(
            "plugin operation ID is already bound to another normalized payload".to_owned(),
        )),
        Err(PluginError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
