use std::{collections::HashMap, pin::Pin};

use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::Instant,
};

use crate::{
    node::logging::LogLevel,
    plugin::{PluginError, PluginInstanceId, PluginJob, protocol},
    protocol::oll,
};

use super::super::{RuntimeDependencies, trace::insert_trace_fields};

pub(super) struct SessionWorkerFailure {
    pub(super) correlation_id: String,
    pub(super) code: &'static str,
}

pub(super) enum PendingRequest {
    StartJob {
        job: PluginJob,
        response: oneshot::Sender<Result<PluginJob, PluginError>>,
    },
    CancelJob {
        job: PluginJob,
        sent_at: Instant,
    },
    Heartbeat {
        nonce: u64,
        correlation_id: String,
        sent_at: Instant,
    },
    Shutdown {
        correlation_id: String,
        graceful_deadline: Instant,
        absolute_deadline: Instant,
    },
}

pub(super) async fn receive_job_accepted(
    dependencies: &RuntimeDependencies,
    instance_id: PluginInstanceId,
    reply_to: Option<u64>,
    trace: &oll::TraceContext,
    accepted: oll::JobAccepted,
    pending: &mut HashMap<u64, PendingRequest>,
    failures: mpsc::UnboundedSender<SessionWorkerFailure>,
) -> Result<(), String> {
    let reply_to =
        reply_to.ok_or_else(|| "JobAccepted must name its StartJobRequest".to_owned())?;
    if !matches!(
        pending.get(&reply_to),
        Some(PendingRequest::StartJob { .. })
    ) {
        return Err("JobAccepted does not name an outstanding StartJobRequest".to_owned());
    }
    let dependencies = dependencies.clone();
    let PendingRequest::StartJob { job, response } = pending
        .remove(&reply_to)
        .expect("validated pending StartJob request")
    else {
        unreachable!("validated pending StartJob request kind")
    };
    let accepted_job = match protocol::decode_plugin_job_id(accepted.job_id.as_ref(), "job_id") {
        Ok(job_id) => job_id,
        Err(error) => {
            let _ = response.send(Err(PluginError::FailedPrecondition(
                "JobAccepted contains an invalid job ID".to_owned(),
            )));
            return Err(error.to_string());
        }
    };
    if trace.correlation_id != job.correlation_id || accepted_job != job.job_id {
        let _ = response.send(Err(PluginError::FailedPrecondition(
            "JobAccepted does not match the pending job".to_owned(),
        )));
        return Err("JobAccepted identity or correlation context differs".to_owned());
    }
    // Acceptance is the ordered SQL linearization point for StartPluginJob.
    // Persist it before reading the next envelope so an immediately following
    // terminal JobUpdate cannot race ahead of JobAccepted.
    let result = dependencies
        .store
        .mark_job_accepted(job.job_id, instance_id, OffsetDateTime::now_utc())
        .await;
    if result.is_err() {
        let mut fields = serde_json::json!({
            "plugin_id": job.payload.plugin_id.as_str(),
            "plugin_instance_id": instance_id.to_string(),
            "job_id": job.job_id.to_string(),
            "transition": "accept_job",
            "error_code": "store_write_failed",
        });
        insert_trace_fields(
            fields
                .as_object_mut()
                .expect("job persistence log fields are an object"),
            trace,
        );
        dependencies.logger.emit(
            LogLevel::Error,
            "oll::plugin::job",
            "plugin_job_persistence_failed",
            &job.correlation_id,
            fields,
        );
        let _ = failures.send(SessionWorkerFailure {
            correlation_id: job.correlation_id.clone(),
            code: "plugin_job_acceptance_persistence_failed",
        });
    }
    let _ = response.send(result);
    Ok(())
}

pub(super) fn receive_cancel_acknowledged(
    dependencies: &RuntimeDependencies,
    instance_id: PluginInstanceId,
    reply_to: Option<u64>,
    trace: &oll::TraceContext,
    acknowledged: oll::CancelJobAcknowledged,
    pending: &mut HashMap<u64, PendingRequest>,
    failures: mpsc::UnboundedSender<SessionWorkerFailure>,
) -> Result<Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>, String> {
    let reply_to = reply_to
        .ok_or_else(|| "CancelJobAcknowledged must name its CancelJobRequest".to_owned())?;
    if !matches!(
        pending.get(&reply_to),
        Some(PendingRequest::CancelJob { .. })
    ) {
        return Err(
            "CancelJobAcknowledged does not name an outstanding CancelJobRequest".to_owned(),
        );
    }
    let PendingRequest::CancelJob { job, .. } = pending
        .get(&reply_to)
        .expect("validated pending CancelJob request")
    else {
        unreachable!("validated pending CancelJob request kind")
    };
    let acknowledged_job =
        match protocol::decode_plugin_job_id(acknowledged.job_id.as_ref(), "job_id") {
            Ok(job_id) => job_id,
            Err(error) => return Err(error.to_string()),
        };
    if trace.correlation_id != job.correlation_id || acknowledged_job != job.job_id {
        return Err("CancelJobAcknowledged identity or correlation context differs".to_owned());
    }
    let dependencies = dependencies.clone();
    let PendingRequest::CancelJob { job, .. } = pending
        .remove(&reply_to)
        .expect("validated pending CancelJob request")
    else {
        unreachable!("validated pending CancelJob request kind")
    };
    Ok(Box::pin(async move {
        if dependencies
            .store
            .complete_job_cancellation(job.job_id, instance_id, OffsetDateTime::now_utc())
            .await
            .is_err()
        {
            dependencies.logger.emit(
                LogLevel::Error,
                "oll::plugin::job",
                "plugin_job_persistence_failed",
                &job.correlation_id,
                serde_json::json!({
                    "plugin_id": job.payload.plugin_id.as_str(),
                    "plugin_instance_id": instance_id.to_string(),
                    "job_id": job.job_id.to_string(),
                    "transition": "complete_cancellation",
                    "error_code": "store_write_failed",
                }),
            );
            let _ = failures.send(SessionWorkerFailure {
                correlation_id: job.correlation_id,
                code: "plugin_job_cancellation_persistence_failed",
            });
        }
    }))
}

pub(super) struct ProtocolErrorContext<'a> {
    pub(super) dependencies: &'a RuntimeDependencies,
    pub(super) instance_id: PluginInstanceId,
    pub(super) pending: &'a mut HashMap<u64, PendingRequest>,
    pub(super) tasks: &'a mut JoinSet<()>,
    pub(super) task_contexts: &'a mut HashMap<tokio::task::Id, String>,
    pub(super) failures: mpsc::UnboundedSender<SessionWorkerFailure>,
}

pub(super) fn receive_protocol_error(
    context: ProtocolErrorContext<'_>,
    reply_to: Option<u64>,
    trace: &oll::TraceContext,
    error: oll::ProtocolError,
) -> Result<(), String> {
    let ProtocolErrorContext {
        dependencies,
        instance_id,
        pending,
        tasks,
        task_contexts,
        failures,
    } = context;
    let request = reply_to
        .and_then(|reply_to| pending.remove(&reply_to))
        .ok_or_else(|| "plugin ProtocolError has no pending request".to_owned())?;
    match request {
        PendingRequest::StartJob { job, response } => {
            if trace.correlation_id != job.correlation_id {
                return Err("plugin ProtocolError correlation context differs".to_owned());
            }
            let store = dependencies.store.clone();
            let logger = dependencies.logger.clone();
            let task_correlation_id = job.correlation_id.clone();
            let task = tasks.spawn(async move {
                let result = store
                    .finish_job(
                        job.job_id,
                        instance_id,
                        crate::plugin::JobState::Failed,
                        None,
                        Some("plugin_protocol_error"),
                        Some(&error.message),
                        OffsetDateTime::now_utc(),
                    )
                    .await;
                if result.is_err() {
                    logger.emit(
                        LogLevel::Error,
                        "oll::plugin::job",
                        "plugin_job_persistence_failed",
                        &job.correlation_id,
                        serde_json::json!({
                            "plugin_id": job.payload.plugin_id.as_str(),
                            "plugin_instance_id": instance_id.to_string(),
                            "job_id": job.job_id.to_string(),
                            "transition": "reject_start_job",
                            "error_code": "store_write_failed",
                        }),
                    );
                    let _ = failures.send(SessionWorkerFailure {
                        correlation_id: job.correlation_id.clone(),
                        code: "plugin_job_failure_persistence_failed",
                    });
                }
                let _ = response.send(result);
            });
            task_contexts.insert(task.id(), task_correlation_id);
            Ok(())
        }
        PendingRequest::CancelJob { job, .. } => {
            if trace.correlation_id != job.correlation_id {
                return Err("plugin ProtocolError correlation context differs".to_owned());
            }
            let store = dependencies.store.clone();
            let logger = dependencies.logger.clone();
            let task_correlation_id = job.correlation_id.clone();
            let task = tasks.spawn(async move {
                if store
                    .finish_job(
                        job.job_id,
                        instance_id,
                        crate::plugin::JobState::Failed,
                        None,
                        Some("plugin_protocol_error"),
                        Some(&error.message),
                        OffsetDateTime::now_utc(),
                    )
                    .await
                    .is_err()
                {
                    logger.emit(
                        LogLevel::Error,
                        "oll::plugin::job",
                        "plugin_job_persistence_failed",
                        &job.correlation_id,
                        serde_json::json!({
                            "plugin_id": job.payload.plugin_id.as_str(),
                            "plugin_instance_id": instance_id.to_string(),
                            "job_id": job.job_id.to_string(),
                            "transition": "reject_cancellation",
                            "error_code": "store_write_failed",
                        }),
                    );
                    let _ = failures.send(SessionWorkerFailure {
                        correlation_id: job.correlation_id,
                        code: "plugin_job_failure_persistence_failed",
                    });
                }
            });
            task_contexts.insert(task.id(), task_correlation_id);
            Ok(())
        }
        PendingRequest::Heartbeat { .. } => Err("plugin rejected a heartbeat".to_owned()),
        PendingRequest::Shutdown { .. } => Err("plugin rejected shutdown".to_owned()),
    }
}

pub(super) fn fail_pending(pending: HashMap<u64, PendingRequest>) {
    for request in pending.into_values() {
        let error =
            || PluginError::FailedPrecondition("plugin session ended before response".to_owned());
        match request {
            PendingRequest::StartJob { response, .. } => {
                let _ = response.send(Err(error()));
            }
            PendingRequest::CancelJob { .. }
            | PendingRequest::Heartbeat { .. }
            | PendingRequest::Shutdown { .. } => {}
        }
    }
}
