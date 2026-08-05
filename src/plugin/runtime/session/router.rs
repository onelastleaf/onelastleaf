use std::{collections::HashMap, sync::Arc};

use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    node::logging::LogLevel,
    plugin::{PluginError, PluginInstanceId},
    protocol::oll::{self, plugin_envelope},
};

use super::super::{InstanceCommand, InstanceShutdown, JOB_RESPONSE_TIMEOUT, RuntimeDependencies};

use super::{
    ARTIFACT_COMMAND_CAPACITY, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, SESSION_FAILURE_GRACE,
    SESSION_WORK_CAPACITY,
    artifacts::run_artifacts,
    cleanup::finish_session,
    handshake::establish_session,
    incoming::{IncomingAction, IncomingContext, dispatch_incoming},
    jobs::{PendingRequest, SessionWorkerFailure, fail_pending},
    outbound::{
        root_trace, send_cancel_job, send_protocol_shutdown, send_shutdown, send_start_job,
        try_send_payload,
    },
    outcome::SessionOutcome,
    service::ConnectedSession,
};

#[allow(clippy::too_many_arguments)]
pub(in crate::plugin::runtime) async fn run_session(
    dependencies: RuntimeDependencies,
    plugin: crate::plugin::InstalledPlugin,
    instance_id: PluginInstanceId,
    mut connected: ConnectedSession,
    mut commands: mpsc::Receiver<InstanceCommand>,
    mut shutdown: watch::Receiver<Option<InstanceShutdown>>,
    notices: mpsc::UnboundedSender<super::super::InstanceNotice>,
    lifecycle_correlation_id: String,
    handshake_deadline: Instant,
    mut process_ended: watch::Receiver<bool>,
    shutdown_deadline_signal: watch::Sender<Option<Instant>>,
) -> SessionOutcome {
    let established = match establish_session(
        &dependencies,
        &plugin,
        instance_id,
        &mut connected,
        &notices,
        &lifecycle_correlation_id,
        handshake_deadline,
    )
    .await
    {
        Ok(established) => established,
        Err(outcome) => return outcome,
    };
    let session_id = established.session_id;
    let outgoing_ids = established.outgoing_ids;
    let mut last_incoming_message_id = established.last_incoming_message_id;

    let (artifact_sender, artifact_receiver) = mpsc::channel(ARTIFACT_COMMAND_CAPACITY);
    let artifact_task = tokio::spawn(run_artifacts(
        dependencies.clone(),
        plugin.plugin_id.clone(),
        instance_id,
        session_id.clone(),
        connected.outgoing.clone(),
        Arc::clone(&outgoing_ids),
        artifact_receiver,
    ));
    let mut tasks = JoinSet::new();
    let mut task_contexts = HashMap::new();
    let mut pending = HashMap::new();
    let (worker_failure_sender, mut worker_failure_receiver) =
        mpsc::unbounded_channel::<SessionWorkerFailure>();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut heartbeat_nonce = 1_u64;
    let mut shutdown_requested = false;
    let mut pending_artifact = None;

    let outcome = loop {
        let heartbeat_deadline = pending.values().find_map(|request| match request {
            PendingRequest::Heartbeat { sent_at, .. } => Some(*sent_at + HEARTBEAT_TIMEOUT),
            _ => None,
        });
        let shutdown_deadline = pending.values().find_map(|request| match request {
            PendingRequest::Shutdown {
                correlation_id,
                graceful_deadline,
                absolute_deadline,
            } => Some((
                correlation_id.clone(),
                *graceful_deadline,
                *absolute_deadline,
            )),
            _ => None,
        });
        let cancellation_deadline = pending
            .iter()
            .filter_map(|(message_id, request)| match request {
                PendingRequest::CancelJob { sent_at, .. } => {
                    Some((*message_id, *sent_at + JOB_RESPONSE_TIMEOUT))
                }
                _ => None,
            })
            .min_by_key(|(_, deadline)| *deadline);
        tokio::select! {
            biased;
            Some(failure) = worker_failure_receiver.recv() => {
                break SessionOutcome::failed(
                    failure.code.to_owned(), failure.correlation_id
                );
            }
            changed = shutdown.changed() => {
                if changed.is_err() {
                    send_protocol_shutdown(
                        &connected.outgoing,
                        &outgoing_ids,
                        &session_id,
                        instance_id,
                        &lifecycle_correlation_id,
                        "plugin supervisor stopped",
                    ).await;
                    break SessionOutcome::failed_after_shutdown(
                        "plugin_supervisor_stopped".to_owned(), lifecycle_correlation_id.clone()
                    );
                }
                let request = shutdown
                    .borrow()
                    .clone()
                    .expect("changed shutdown lane contains a request");
                if !shutdown_requested {
                    shutdown_requested = true;
                    shutdown_deadline_signal.send_replace(Some(request.deadline));
                    let request_deadline = request
                        .deadline
                        .min(Instant::now() + SESSION_FAILURE_GRACE);
                    dependencies.logger.emit(
                        LogLevel::Info,
                        "oll::plugin",
                        "plugin_shutdown_requested",
                        &request.correlation_id,
                        serde_json::json!({
                            "plugin_id": plugin.plugin_id.as_str(),
                            "plugin_instance_id": instance_id.to_string(),
                            "reason": &request.reason,
                            "remaining_ms": u64::try_from(
                                request.deadline.saturating_duration_since(Instant::now()).as_millis()
                            ).unwrap_or(u64::MAX),
                        }),
                    );
                    match send_shutdown(
                        &connected.outgoing,
                        &outgoing_ids,
                        &session_id,
                        instance_id,
                        &request.correlation_id,
                        request.reason,
                        request_deadline,
                    ) {
                        Ok(message_id) => {
                            pending.insert(message_id, PendingRequest::Shutdown {
                                correlation_id: request.correlation_id,
                                graceful_deadline: request_deadline,
                                absolute_deadline: request.deadline,
                            });
                        }
                        Err(_) => break SessionOutcome::failed(
                            "plugin_shutdown_send_failed".to_owned(), lifecycle_correlation_id.clone()
                        ),
                    }
                } else {
                    let tightened_deadline = shutdown_deadline_signal
                        .borrow()
                        .map_or(request.deadline, |current| current.min(request.deadline));
                    shutdown_deadline_signal.send_replace(Some(tightened_deadline));
                    if let Some(PendingRequest::Shutdown {
                        graceful_deadline,
                        absolute_deadline,
                        ..
                    }) = pending
                        .values_mut()
                        .find(|request| matches!(request, PendingRequest::Shutdown { .. }))
                    {
                        *absolute_deadline = (*absolute_deadline).min(request.deadline);
                        *graceful_deadline = (*graceful_deadline).min(request.deadline);
                    }
                }
            }
            incoming = connected.incoming.message(),
                if pending_artifact.is_none() && tasks.len() < SESSION_WORK_CAPACITY => {
                match dispatch_incoming(incoming, IncomingContext {
                    dependencies: &dependencies,
                    plugin: &plugin,
                    instance_id,
                    outgoing: &connected.outgoing,
                    outgoing_ids: &outgoing_ids,
                    session_id: &session_id,
                    lifecycle_correlation_id: &lifecycle_correlation_id,
                    last_incoming_message_id: &mut last_incoming_message_id,
                    pending: &mut pending,
                    tasks: &mut tasks,
                    task_contexts: &mut task_contexts,
                    worker_failures: &worker_failure_sender,
                    pending_artifact: &mut pending_artifact,
                    shutdown_requested,
                }).await {
                    IncomingAction::Continue => {}
                    IncomingAction::Stop(outcome) => break outcome,
                }
            }
            permit = artifact_sender.reserve(), if pending_artifact.is_some() => {
                match permit {
                    Ok(permit) => permit.send(
                        pending_artifact.take().expect("guarded pending artifact command")
                    ),
                    Err(_) => {
                        send_protocol_shutdown(
                            &connected.outgoing,
                            &outgoing_ids,
                            &session_id,
                            instance_id,
                            &lifecycle_correlation_id,
                            "artifact owner stopped",
                        ).await;
                        break SessionOutcome::failed_after_shutdown(
                            "plugin_artifact_owner_stopped".to_owned(),
                            lifecycle_correlation_id.clone(),
                        );
                    }
                }
            }
            command = commands.recv() => {
                match command {
                    Some(InstanceCommand::StartJob { job, response }) if !shutdown_requested => {
                        if pending.len() + tasks.len() >= SESSION_WORK_CAPACITY {
                            let _ = response.send(Err(PluginError::FailedPrecondition(
                                "plugin session work queue is full".to_owned(),
                            )));
                            continue;
                        }
                        match send_start_job(
                            &connected.outgoing,
                            &outgoing_ids,
                            &session_id,
                            instance_id,
                            &job,
                        ) {
                            Ok(message_id) => {
                                pending.insert(message_id, PendingRequest::StartJob { job, response });
                            }
                            Err(error) => { let _ = response.send(Err(error)); }
                        }
                    }
                    Some(InstanceCommand::CancelJob { job, reason, dispatched }) if !shutdown_requested => {
                        if pending.len() + tasks.len() >= SESSION_WORK_CAPACITY {
                            let _ = dispatched.send(Err(PluginError::FailedPrecondition(
                                "plugin session work queue is full".to_owned(),
                            )));
                            continue;
                        }
                        match send_cancel_job(
                            &connected.outgoing,
                            &outgoing_ids,
                            &session_id,
                            instance_id,
                            &job,
                            reason,
                        ) {
                            Ok(message_id) => {
                                pending.insert(message_id, PendingRequest::CancelJob {
                                    job,
                                    sent_at: Instant::now(),
                                });
                                let _ = dispatched.send(Ok(()));
                            }
                            Err(error) => { let _ = dispatched.send(Err(error)); }
                        }
                    }
                    Some(InstanceCommand::StartJob { response, .. })
                    => {
                        let _ = response.send(Err(PluginError::FailedPrecondition(
                            "plugin process is stopping".to_owned()
                        )));
                    }
                    Some(InstanceCommand::CancelJob { dispatched, .. }) => {
                        let _ = dispatched.send(Err(PluginError::FailedPrecondition(
                            "plugin process is stopping".to_owned()
                        )));
                    }
                    None => {
                        send_protocol_shutdown(
                            &connected.outgoing,
                            &outgoing_ids,
                            &session_id,
                            instance_id,
                            &lifecycle_correlation_id,
                            "plugin supervisor stopped",
                        ).await;
                        break SessionOutcome::failed_after_shutdown(
                            "plugin_supervisor_stopped".to_owned(), lifecycle_correlation_id.clone()
                        );
                    }
                }
            }
            _ = heartbeat.tick(), if !shutdown_requested => {
                if !pending.values().any(|request| matches!(request, PendingRequest::Heartbeat { .. })) {
                    let trace = root_trace(&lifecycle_correlation_id);
                    if let Ok(message_id) = try_send_payload(
                        &connected.outgoing,
                        &outgoing_ids,
                        &session_id,
                        instance_id,
                        trace,
                        None,
                        plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: heartbeat_nonce }),
                    ) {
                        pending.insert(message_id, PendingRequest::Heartbeat {
                            nonce: heartbeat_nonce,
                            correlation_id: lifecycle_correlation_id.clone(),
                            sent_at: Instant::now(),
                        });
                        heartbeat_nonce = heartbeat_nonce.wrapping_add(1).max(1);
                    } else {
                        break SessionOutcome::failed_after_shutdown(
                            "plugin_session_output_backpressured".to_owned(),
                            lifecycle_correlation_id.clone(),
                        );
                    }
                }
            }
            _ = tokio::time::sleep_until(
                heartbeat_deadline.unwrap_or_else(Instant::now)
            ), if heartbeat_deadline.is_some() && !shutdown_requested => {
                send_protocol_shutdown(
                    &connected.outgoing,
                    &outgoing_ids,
                    &session_id,
                    instance_id,
                    &lifecycle_correlation_id,
                    "plugin heartbeat deadline exceeded",
                ).await;
                break SessionOutcome::failed_after_shutdown(
                    "plugin_heartbeat_timeout".to_owned(),
                    lifecycle_correlation_id.clone(),
                );
            }
            _ = tokio::time::sleep_until(
                cancellation_deadline
                    .map_or_else(Instant::now, |(_, deadline)| deadline)
            ), if cancellation_deadline.is_some() => {
                let (message_id, _) = cancellation_deadline
                    .expect("guarded cancellation deadline");
                if let Some(PendingRequest::CancelJob { job, .. }) = pending.remove(&message_id) {
                    let store = dependencies.store.clone();
                    let logger = dependencies.logger.clone();
                    let failures = worker_failure_sender.clone();
                    let task_correlation_id = job.correlation_id.clone();
                    let task = tasks.spawn(async move {
                        let result = store
                            .finish_job(
                                job.job_id,
                                instance_id,
                                crate::plugin::JobState::Failed,
                                None,
                                Some("job_cancellation_timeout"),
                                Some("plugin did not acknowledge job cancellation"),
                                OffsetDateTime::now_utc(),
                            )
                            .await;
                        if result.is_ok() {
                            logger.emit(
                                LogLevel::Warn,
                                "oll::plugin::job",
                                "plugin_job_cancellation_timeout",
                                &job.correlation_id,
                                serde_json::json!({
                                    "plugin_id": job.payload.plugin_id.as_str(),
                                    "plugin_instance_id": instance_id.to_string(),
                                    "job_id": job.job_id.to_string(),
                                    "store_updated": true,
                                }),
                            );
                        } else {
                            logger.emit(
                                LogLevel::Error,
                                "oll::plugin::job",
                                "plugin_job_persistence_failed",
                                &job.correlation_id,
                                serde_json::json!({
                                    "plugin_id": job.payload.plugin_id.as_str(),
                                    "plugin_instance_id": instance_id.to_string(),
                                    "job_id": job.job_id.to_string(),
                                    "transition": "cancellation_timeout",
                                    "error_code": "store_write_failed",
                                }),
                            );
                            let _ = failures.send(SessionWorkerFailure {
                                correlation_id: job.correlation_id,
                                code: "plugin_job_timeout_persistence_failed",
                            });
                        }
                    });
                    task_contexts.insert(task.id(), task_correlation_id);
                }
            }
            _ = tokio::time::sleep_until(
                shutdown_deadline
                    .as_ref()
                    .map_or_else(Instant::now, |(_, deadline, _)| *deadline)
            ), if shutdown_deadline.is_some() => {
                let (correlation_id, graceful_deadline, absolute_deadline) =
                    shutdown_deadline.expect("guarded shutdown deadline");
                break SessionOutcome::stopped(
                    correlation_id,
                    graceful_deadline,
                    absolute_deadline,
                    Some("plugin_shutdown_ack_timeout".to_owned()),
                );
            }
            changed = process_ended.changed() => {
                if changed.is_err() || *process_ended.borrow() {
                    break SessionOutcome::failed(
                        "plugin_process_exited".to_owned(), lifecycle_correlation_id.clone()
                    );
                }
            }
            Some(completed) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                match completed {
                    Ok((task_id, ())) => {
                        task_contexts.remove(&task_id);
                    }
                    Err(error) => {
                        let correlation_id = task_contexts
                            .remove(&error.id())
                            .unwrap_or_else(|| lifecycle_correlation_id.clone());
                        dependencies.logger.emit(
                            LogLevel::Error,
                            "oll::plugin",
                            "plugin_session_worker_failed",
                            &correlation_id,
                            serde_json::json!({
                                "plugin_id": plugin.plugin_id.as_str(),
                                "plugin_instance_id": instance_id.to_string(),
                                "error_code": if error.is_panic() {
                                    "task_panicked"
                                } else {
                                    "task_cancelled"
                                },
                            }),
                        );
                        break SessionOutcome::failed(
                            "plugin_session_worker_failed".to_owned(),
                            correlation_id,
                        );
                    }
                }
            }
        }
    };

    drop(artifact_sender);
    fail_pending(pending);
    finish_session(
        &dependencies,
        &plugin,
        instance_id,
        &session_id,
        outcome,
        tasks,
        task_contexts,
        artifact_task,
    )
    .await
}
