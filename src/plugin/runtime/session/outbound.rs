use std::sync::atomic::{AtomicU64, Ordering};

use time::OffsetDateTime;
use tokio::{sync::mpsc, time::Instant};
use tonic::Status;

use crate::{
    plugin::{JobCancellationReason, PluginError, PluginInstanceId, PluginJob, protocol},
    protocol::oll::{self, plugin_envelope},
};

use super::{super::host::protocol_error, SESSION_FAILURE_GRACE};

pub(super) fn send_start_job(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    job: &PluginJob,
) -> Result<u64, PluginError> {
    let deadline = protocol::encode_timestamp(job.absolute_deadline, "job deadline")?;
    try_send_payload(
        outgoing,
        ids,
        session_id,
        instance_id,
        root_trace(&job.correlation_id),
        None,
        plugin_envelope::Payload::StartJob(oll::StartJobRequest {
            job_id: Some(protocol::encode_plugin_job_id(job.job_id)),
            deadline: Some(deadline),
            invocation: Some(oll::start_job_request::Invocation::Action(
                oll::ActionInvocation {
                    action: job.payload.action.clone(),
                    arguments: job.payload.arguments.clone(),
                },
            )),
        }),
    )
    .map_err(|_| PluginError::FailedPrecondition("plugin session output closed".to_owned()))
}

pub(super) fn send_cancel_job(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    job: &PluginJob,
    reason: JobCancellationReason,
) -> Result<u64, PluginError> {
    try_send_payload(
        outgoing,
        ids,
        session_id,
        instance_id,
        root_trace(&job.correlation_id),
        None,
        plugin_envelope::Payload::CancelJob(oll::CancelJobRequest {
            job_id: Some(protocol::encode_plugin_job_id(job.job_id)),
            reason: protocol::encode_job_cancellation_reason(reason),
        }),
    )
    .map_err(|_| PluginError::FailedPrecondition("plugin session output closed".to_owned()))
}

pub(super) fn send_shutdown(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    correlation_id: &str,
    reason: String,
    deadline: Instant,
) -> Result<u64, ()> {
    let wall_deadline =
        OffsetDateTime::now_utc() + deadline.saturating_duration_since(Instant::now());
    let deadline =
        protocol::encode_timestamp(wall_deadline, "shutdown deadline").map_err(|_| ())?;
    try_send_payload(
        outgoing,
        ids,
        session_id,
        instance_id,
        root_trace(correlation_id),
        None,
        plugin_envelope::Payload::Shutdown(oll::ShutdownRequest {
            reason,
            grace_period_deadline: Some(deadline),
        }),
    )
}

pub(super) async fn send_protocol_shutdown(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    correlation_id: &str,
    reason: &str,
) {
    let trace = root_trace(correlation_id);
    let _ = try_send_payload(
        outgoing,
        ids,
        session_id,
        instance_id,
        trace.clone(),
        None,
        plugin_envelope::Payload::ProtocolError(protocol_error(
            oll::ErrorCode::FailedPrecondition,
            "plugin protocol session is closing",
            false,
        )),
    );
    let _ = send_shutdown(
        outgoing,
        ids,
        session_id,
        instance_id,
        correlation_id,
        reason.to_owned(),
        Instant::now() + SESSION_FAILURE_GRACE,
    );
}

pub(in crate::plugin::runtime) fn try_send_payload(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    trace: oll::TraceContext,
    reply_to: Option<u64>,
    payload: plugin_envelope::Payload,
) -> Result<u64, ()> {
    let permit = outgoing.try_reserve().map_err(|_| ())?;
    let message_id = next_outgoing_id(ids)?;
    permit.send(Ok(oll::PluginEnvelope {
        message_id,
        reply_to,
        session_id: session_id.to_owned(),
        plugin_instance_id: instance_id.to_string(),
        trace: Some(trace),
        payload: Some(payload),
    }));
    Ok(message_id)
}

pub(in crate::plugin::runtime) async fn send_payload(
    outgoing: &mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    ids: &AtomicU64,
    session_id: &str,
    instance_id: PluginInstanceId,
    trace: oll::TraceContext,
    reply_to: Option<u64>,
    payload: plugin_envelope::Payload,
) -> Result<u64, ()> {
    let message_id = next_outgoing_id(ids)?;
    outgoing
        .send(Ok(oll::PluginEnvelope {
            message_id,
            reply_to,
            session_id: session_id.to_owned(),
            plugin_instance_id: instance_id.to_string(),
            trace: Some(trace),
            payload: Some(payload),
        }))
        .await
        .map_err(|_| ())?;
    Ok(message_id)
}

fn next_outgoing_id(ids: &AtomicU64) -> Result<u64, ()> {
    ids.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        current.checked_add(1)
    })
    .map_err(|_| ())
}

pub(super) fn root_trace(correlation_id: &str) -> oll::TraceContext {
    oll::TraceContext {
        correlation_id: correlation_id.to_owned(),
        parent_call_id: None,
        call_depth: 0,
        causal_depth: 0,
        task_id: None,
        task_group_id: None,
    }
}
