use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::{sync::mpsc, task::JoinSet, time::Instant};
use tonic::Status;

use crate::{
    plugin::{InstalledPlugin, PluginInstanceId},
    protocol::oll::{self, plugin_envelope},
};

use super::{
    super::{
        RuntimeDependencies,
        host::{execute_host_call, protocol_error},
        jobs::handle_job_update,
        plugin_log::emit_plugin_record,
    },
    artifacts::ArtifactCommand,
    handshake::validate_envelope,
    jobs::{
        PendingRequest, ProtocolErrorContext, SessionWorkerFailure, receive_cancel_acknowledged,
        receive_job_accepted, receive_protocol_error,
    },
    outbound::{send_payload, send_protocol_shutdown, try_send_payload},
    outcome::SessionOutcome,
};

pub(super) struct IncomingContext<'a> {
    pub(super) dependencies: &'a RuntimeDependencies,
    pub(super) plugin: &'a InstalledPlugin,
    pub(super) instance_id: PluginInstanceId,
    pub(super) outgoing: &'a mpsc::Sender<Result<oll::PluginEnvelope, Status>>,
    pub(super) outgoing_ids: &'a Arc<AtomicU64>,
    pub(super) session_id: &'a str,
    pub(super) lifecycle_correlation_id: &'a str,
    pub(super) last_incoming_message_id: &'a mut u64,
    pub(super) pending: &'a mut HashMap<u64, PendingRequest>,
    pub(super) tasks: &'a mut JoinSet<()>,
    pub(super) task_contexts: &'a mut HashMap<tokio::task::Id, String>,
    pub(super) worker_failures: &'a mpsc::UnboundedSender<SessionWorkerFailure>,
    pub(super) pending_artifact: &'a mut Option<ArtifactCommand>,
    pub(super) shutdown_requested: bool,
}

pub(super) enum IncomingAction {
    Continue,
    Stop(SessionOutcome),
}

pub(super) async fn dispatch_incoming(
    incoming: Result<Option<oll::PluginEnvelope>, Status>,
    context: IncomingContext<'_>,
) -> IncomingAction {
    let envelope = match incoming {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            return IncomingAction::Stop(SessionOutcome::failed(
                "plugin_stream_closed".to_owned(),
                context.lifecycle_correlation_id.to_owned(),
            ));
        }
        Err(_) => {
            return IncomingAction::Stop(SessionOutcome::failed(
                "plugin_stream_failed".to_owned(),
                context.lifecycle_correlation_id.to_owned(),
            ));
        }
    };
    let trace = match validate_envelope(
        &envelope,
        context.session_id,
        context.instance_id,
        context.last_incoming_message_id,
    ) {
        Ok(trace) => trace,
        Err(error) => {
            send_protocol_shutdown(
                context.outgoing,
                context.outgoing_ids,
                context.session_id,
                context.instance_id,
                context.lifecycle_correlation_id,
                &error,
            )
            .await;
            return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                "plugin_protocol_violation".to_owned(),
                context.lifecycle_correlation_id.to_owned(),
            ));
        }
    };
    let message_id = envelope.message_id;
    let reply_to = envelope.reply_to;
    let Some(payload) = envelope.payload else {
        send_protocol_shutdown(
            context.outgoing,
            context.outgoing_ids,
            context.session_id,
            context.instance_id,
            context.lifecycle_correlation_id,
            "plugin envelope payload is required",
        )
        .await;
        return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
            "plugin_protocol_violation".to_owned(),
            context.lifecycle_correlation_id.to_owned(),
        ));
    };
    if context.shutdown_requested && !quiescing_allows(&payload) {
        if try_send_payload(
            context.outgoing,
            context.outgoing_ids,
            context.session_id,
            context.instance_id,
            trace,
            Some(message_id),
            plugin_envelope::Payload::ProtocolError(protocol_error(
                oll::ErrorCode::FailedPrecondition,
                "plugin session is quiescing",
                false,
            )),
        )
        .is_err()
        {
            return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                "plugin_session_output_backpressured".to_owned(),
                context.lifecycle_correlation_id.to_owned(),
            ));
        }
        return IncomingAction::Continue;
    }

    match payload {
        plugin_envelope::Payload::JobAccepted(accepted) => {
            match receive_job_accepted(
                context.dependencies,
                context.instance_id,
                reply_to,
                &trace,
                accepted,
                context.pending,
                context.worker_failures.clone(),
            )
            .await
            {
                Ok(()) => {}
                Err(error) => {
                    if try_send_payload(
                        context.outgoing,
                        context.outgoing_ids,
                        context.session_id,
                        context.instance_id,
                        trace,
                        Some(message_id),
                        plugin_envelope::Payload::ProtocolError(protocol_error(
                            oll::ErrorCode::InvalidArgument,
                            error,
                            false,
                        )),
                    )
                    .is_err()
                    {
                        return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                            "plugin_session_output_backpressured".to_owned(),
                            context.lifecycle_correlation_id.to_owned(),
                        ));
                    }
                }
            }
        }
        plugin_envelope::Payload::CancelJobAcknowledged(acknowledged) => {
            let task_correlation_id = trace.correlation_id.clone();
            match receive_cancel_acknowledged(
                context.dependencies,
                context.instance_id,
                reply_to,
                &trace,
                acknowledged,
                context.pending,
                context.worker_failures.clone(),
            ) {
                Ok(task) => {
                    let task = context.tasks.spawn(task);
                    context.task_contexts.insert(task.id(), task_correlation_id);
                }
                Err(error) => {
                    if try_send_payload(
                        context.outgoing,
                        context.outgoing_ids,
                        context.session_id,
                        context.instance_id,
                        trace,
                        Some(message_id),
                        plugin_envelope::Payload::ProtocolError(protocol_error(
                            oll::ErrorCode::InvalidArgument,
                            error,
                            false,
                        )),
                    )
                    .is_err()
                    {
                        return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                            "plugin_session_output_backpressured".to_owned(),
                            context.lifecycle_correlation_id.to_owned(),
                        ));
                    }
                }
            }
        }
        plugin_envelope::Payload::JobUpdate(update) if reply_to.is_none() => {
            let dependencies = context.dependencies.clone();
            let plugin_id = context.plugin.plugin_id.clone();
            let outgoing = context.outgoing.clone();
            let outgoing_ids = Arc::clone(context.outgoing_ids);
            let session_id = context.session_id.to_owned();
            let instance_id = context.instance_id;
            let task_correlation_id = trace.correlation_id.clone();
            let task = context.tasks.spawn(async move {
                if let Err(error) =
                    handle_job_update(&dependencies, &plugin_id, instance_id, update, &trace).await
                {
                    let _ = send_payload(
                        &outgoing,
                        &outgoing_ids,
                        &session_id,
                        instance_id,
                        trace,
                        Some(message_id),
                        plugin_envelope::Payload::ProtocolError(error),
                    )
                    .await;
                }
            });
            context.task_contexts.insert(task.id(), task_correlation_id);
        }
        plugin_envelope::Payload::HostCall(call) if reply_to.is_none() => {
            let dependencies = context.dependencies.clone();
            let plugin_id = context.plugin.plugin_id.clone();
            let outgoing = context.outgoing.clone();
            let outgoing_ids = Arc::clone(context.outgoing_ids);
            let session_id = context.session_id.to_owned();
            let instance_id = context.instance_id;
            let task_correlation_id = trace.correlation_id.clone();
            let task = context.tasks.spawn(async move {
                let response = execute_host_call(
                    dependencies,
                    &plugin_id,
                    instance_id,
                    session_id.clone(),
                    call,
                    &trace,
                )
                .await;
                let _ = send_payload(
                    &outgoing,
                    &outgoing_ids,
                    &session_id,
                    instance_id,
                    trace,
                    Some(message_id),
                    plugin_envelope::Payload::HostResult(response),
                )
                .await;
            });
            context.task_contexts.insert(task.id(), task_correlation_id);
        }
        plugin_envelope::Payload::Log(record) if reply_to.is_none() => {
            if let Err(error) = emit_plugin_record(
                &context.dependencies.logger,
                context.plugin,
                context.instance_id,
                &trace,
                record,
            ) && try_send_payload(
                context.outgoing,
                context.outgoing_ids,
                context.session_id,
                context.instance_id,
                trace,
                Some(message_id),
                plugin_envelope::Payload::ProtocolError(error),
            )
            .is_err()
            {
                return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                    "plugin_session_output_backpressured".to_owned(),
                    context.lifecycle_correlation_id.to_owned(),
                ));
            }
        }
        plugin_envelope::Payload::ArtifactStart(request) if reply_to.is_none() => {
            *context.pending_artifact = Some(ArtifactCommand::Start {
                message_id,
                trace,
                request,
            });
        }
        plugin_envelope::Payload::ArtifactChunk(request) if reply_to.is_none() => {
            *context.pending_artifact = Some(ArtifactCommand::Chunk {
                message_id,
                trace,
                request,
            });
        }
        plugin_envelope::Payload::ArtifactComplete(request) if reply_to.is_none() => {
            *context.pending_artifact = Some(ArtifactCommand::Complete {
                message_id,
                trace,
                request,
            });
        }
        plugin_envelope::Payload::Heartbeat(response) => {
            if let Err(error) = receive_heartbeat(reply_to, &trace, response, context.pending) {
                send_protocol_shutdown(
                    context.outgoing,
                    context.outgoing_ids,
                    context.session_id,
                    context.instance_id,
                    context.lifecycle_correlation_id,
                    &error,
                )
                .await;
                return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                    "plugin_heartbeat_invalid".to_owned(),
                    context.lifecycle_correlation_id.to_owned(),
                ));
            }
        }
        plugin_envelope::Payload::ShutdownAcknowledged(_) => {
            match receive_shutdown_acknowledged(reply_to, &trace, context.pending) {
                Ok((correlation_id, graceful_deadline, absolute_deadline)) => {
                    return IncomingAction::Stop(SessionOutcome::stopped(
                        correlation_id,
                        graceful_deadline,
                        absolute_deadline,
                        None,
                    ));
                }
                Err(error) => {
                    send_protocol_shutdown(
                        context.outgoing,
                        context.outgoing_ids,
                        context.session_id,
                        context.instance_id,
                        context.lifecycle_correlation_id,
                        &error,
                    )
                    .await;
                    return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                        "plugin_shutdown_ack_invalid".to_owned(),
                        context.lifecycle_correlation_id.to_owned(),
                    ));
                }
            }
        }
        plugin_envelope::Payload::ProtocolError(error) => {
            if let Err(reason) = receive_protocol_error(
                ProtocolErrorContext {
                    dependencies: context.dependencies,
                    instance_id: context.instance_id,
                    pending: context.pending,
                    tasks: context.tasks,
                    task_contexts: context.task_contexts,
                    failures: context.worker_failures.clone(),
                },
                reply_to,
                &trace,
                error,
            ) {
                send_protocol_shutdown(
                    context.outgoing,
                    context.outgoing_ids,
                    context.session_id,
                    context.instance_id,
                    context.lifecycle_correlation_id,
                    &reason,
                )
                .await;
                return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                    reason,
                    context.lifecycle_correlation_id.to_owned(),
                ));
            }
        }
        _ => {
            send_protocol_shutdown(
                context.outgoing,
                context.outgoing_ids,
                context.session_id,
                context.instance_id,
                context.lifecycle_correlation_id,
                "message kind or reply_to is invalid in a ready plugin session",
            )
            .await;
            return IncomingAction::Stop(SessionOutcome::failed_after_shutdown(
                "plugin_protocol_violation".to_owned(),
                context.lifecycle_correlation_id.to_owned(),
            ));
        }
    }
    IncomingAction::Continue
}

fn receive_heartbeat(
    reply_to: Option<u64>,
    trace: &oll::TraceContext,
    heartbeat: oll::Heartbeat,
    pending: &mut HashMap<u64, PendingRequest>,
) -> Result<(), String> {
    let request = reply_to
        .and_then(|reply_to| pending.remove(&reply_to))
        .ok_or_else(|| "heartbeat does not reply to an outstanding request".to_owned())?;
    let PendingRequest::Heartbeat {
        nonce,
        correlation_id,
        ..
    } = request
    else {
        return Err("heartbeat reply_to names another request kind".to_owned());
    };
    if heartbeat.nonce != nonce || trace.correlation_id != correlation_id {
        return Err("heartbeat nonce or correlation context differs".to_owned());
    }
    Ok(())
}

fn receive_shutdown_acknowledged(
    reply_to: Option<u64>,
    trace: &oll::TraceContext,
    pending: &mut HashMap<u64, PendingRequest>,
) -> Result<(String, Instant, Instant), String> {
    let request = reply_to
        .and_then(|reply_to| pending.remove(&reply_to))
        .ok_or_else(|| "shutdown acknowledgement has no outstanding request".to_owned())?;
    let PendingRequest::Shutdown {
        correlation_id,
        graceful_deadline,
        absolute_deadline,
    } = request
    else {
        return Err("shutdown acknowledgement reply_to names another request kind".to_owned());
    };
    if trace.correlation_id != correlation_id {
        return Err("shutdown acknowledgement correlation context differs".to_owned());
    }
    Ok((correlation_id, graceful_deadline, absolute_deadline))
}

pub(in crate::plugin::runtime) fn quiescing_allows(payload: &plugin_envelope::Payload) -> bool {
    matches!(
        payload,
        plugin_envelope::Payload::JobAccepted(_)
            | plugin_envelope::Payload::CancelJobAcknowledged(_)
            | plugin_envelope::Payload::Heartbeat(_)
            | plugin_envelope::Payload::Log(_)
            | plugin_envelope::Payload::ShutdownAcknowledged(_)
            | plugin_envelope::Payload::ProtocolError(_)
    )
}
