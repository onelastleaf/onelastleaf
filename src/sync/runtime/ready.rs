use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_ready_session(
    runtime: &SyncRuntime,
    mut channel: SessionChannel<TcpStream>,
    session_id: Uuid,
    bound_epoch: u64,
    remote_node_id: Uuid,
    direction: Direction,
    mode: SessionReplicaMode,
    max_chunk_bytes: u32,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut cancel: watch::Receiver<Option<SyncCloseCode>>,
) -> ConnectionDisposition {
    debug_assert!(matches!(
        mode,
        SessionReplicaMode::Waiting | SessionReplicaMode::Normal
    ));
    let mut shutdown = runtime.shutdown.subscribe();
    let mut epoch = runtime.identities.subscribe_epoch();
    let mut replica_status = runtime.replica.subscribe_status();
    let mut pings = HashMap::<u64, PendingPing>::new();
    let mut disposition = ConnectionDisposition::RetryWithBackoff;
    'session: loop {
        let immediate_close = if *shutdown.borrow() {
            Some((
                SyncCloseCode::ShuttingDown,
                "local daemon is shutting down",
                ConnectionDisposition::RetryWithBackoff,
            ))
        } else if let Some(code) = *cancel.borrow() {
            Some((
                code,
                "sync session was superseded",
                if code == SyncCloseCode::DuplicateSession {
                    ConnectionDisposition::SuppressedByActiveSession(remote_node_id)
                } else {
                    ConnectionDisposition::RetryWithBackoff
                },
            ))
        } else if mode == SessionReplicaMode::Waiting
            && !matches!(*replica_status.borrow(), ReplicaStatus::Uninitialized)
        {
            Some((
                SyncCloseCode::ReplicaAvailable,
                "local replica became available; reconnect required",
                ConnectionDisposition::ReconnectImmediately,
            ))
        } else if *epoch.borrow() != bound_epoch {
            Some((
                SyncCloseCode::Normal,
                "local identity changed; reconnect required",
                ConnectionDisposition::RetryWithBackoff,
            ))
        } else {
            None
        };
        if let Some((code, message, next)) = immediate_close {
            let correlation_id = new_correlation_id();
            if code == SyncCloseCode::ReplicaAvailable {
                log_replica_available(runtime, &correlation_id, &replica_status);
            }
            channel
                .close(
                    code,
                    message,
                    &correlation_id,
                    Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                )
                .await;
            disposition = next;
            break;
        }
        let next_ping_deadline = pings.values().map(|ping| ping.deadline).min();
        let ping_timeout = async move {
            match next_ping_deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        let heartbeat_deadline = channel.last_activity() + IDLE_HEARTBEAT_INTERVAL;
        let heartbeat_idle = sleep_until(heartbeat_deadline);
        tokio::pin!(heartbeat_idle);
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let stopping = changed.is_err() || *shutdown.borrow_and_update();
                if stopping {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::ShuttingDown, "local daemon is shutting down", &correlation_id, Some(Instant::now() + SESSION_CLOSE_DEADLINE)).await;
                    break;
                }
            }
            changed = cancel.changed() => {
                if changed.is_err() {
                    break;
                }
                let code = *cancel.borrow_and_update();
                if let Some(code) = code {
                    let correlation_id = new_correlation_id();
                    channel.close(code, "sync session was superseded", &correlation_id, Some(Instant::now() + SESSION_CLOSE_DEADLINE)).await;
                    if code == SyncCloseCode::DuplicateSession {
                        disposition = ConnectionDisposition::SuppressedByActiveSession(remote_node_id);
                    }
                    break;
                }
            }
            changed = async {
                if mode == SessionReplicaMode::Waiting {
                    replica_status.changed().await
                } else {
                    std::future::pending().await
                }
            } => {
                if changed.is_err() {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::InternalError, "local replica status notification closed", &correlation_id, Some(Instant::now() + SESSION_CLOSE_DEADLINE)).await;
                    break;
                }
                let status = *replica_status.borrow_and_update();
                if !matches!(status, ReplicaStatus::Uninitialized) {
                    let correlation_id = new_correlation_id();
                    log_replica_available(runtime, &correlation_id, &replica_status);
                    channel.close(SyncCloseCode::ReplicaAvailable, "local replica became available; reconnect required", &correlation_id, Some(Instant::now() + SESSION_CLOSE_DEADLINE)).await;
                    disposition = ConnectionDisposition::ReconnectImmediately;
                    break;
                }
            }
            changed = epoch.changed() => {
                let changed_identity = changed.is_err() || *epoch.borrow_and_update() != bound_epoch;
                if changed_identity {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::Normal, "local identity changed; reconnect required", &correlation_id, Some(Instant::now() + SESSION_CLOSE_DEADLINE)).await;
                    break;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    SessionCommand::Ping { correlation_id, response } => {
                        let nonce = match random_ping_nonce() {
                            Ok(nonce) => nonce,
                            Err(_) => {
                                let _ = response.send(Err(SyncError::Internal("cannot generate sync ping nonce".to_owned())));
                                continue;
                            }
                        };
                        let message_id = match channel.send(
                            sync_envelope::Payload::Ping(SyncPing {
                                nonce,
                                sent_at: Some(system_timestamp()),
                            }),
                            &correlation_id,
                            None,
                            Some(Instant::now() + PING_RESPONSE_DEADLINE),
                        ).await {
                            Ok(message_id) => message_id,
                            Err(error) => {
                                let _ = response.send(Err(SyncError::SessionLost(error)));
                                break;
                            }
                        };
                        let replaced_response = pings.insert(nonce, PendingPing {
                            sent_message_id: message_id,
                            started: Instant::now(),
                            deadline: Instant::now() + PING_RESPONSE_DEADLINE,
                            correlation_id,
                            response: Some(response),
                        }).and_then(|replaced| replaced.response);
                        if let Some(response) = replaced_response {
                            let _ = response.send(Err(SyncError::Internal("sync ping nonce collision".to_owned())));
                        }
                    }
                    SessionCommand::Synchronize { correlation_id, response } => {
                        if mode == SessionReplicaMode::Waiting {
                            let _ = response.send(Err(SyncError::FailedPrecondition(
                                "both peers are waiting for a local replica".to_owned(),
                            )));
                            continue;
                        }
                        make_pending_pings_transparent(&mut channel, &mut pings);
                        let local_node_id = runtime.identities.node_id().await;
                        let observation = SyncObservation {
                            connection_id: session_id,
                            peer_node_id: remote_node_id,
                            direction: direction_name(direction),
                            correlation_id: &correlation_id,
                        };
                        let result = request_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            local_node_id,
                            observation,
                            max_chunk_bytes,
                        )
                        .await;
                        let session_failure = result.is_err();
                        if let Err(error) = &result {
                            log_round_session_failure(runtime, observation, error);
                        }
                        if session_failure {
                            runtime.remove_session(remote_node_id, session_id).await;
                        }
                        let _ = response.send(result);
                        if session_failure {
                            channel.close(
                                SyncCloseCode::InternalError,
                                "sync round invalidated this session",
                                &correlation_id,
                                Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                            ).await;
                            break;
                        }
                    }
                }
            }
            _ = ping_timeout => {
                let now = Instant::now();
                let expired = pings
                    .iter()
                    .filter_map(|(nonce, pending)| (pending.deadline <= now).then_some(*nonce))
                    .collect::<Vec<_>>();
                let mut heartbeat_failure = None;
                for nonce in expired {
                    if let Some(pending) = pings.remove(&nonce) {
                        if let Some(response) = pending.response {
                            channel.track_transparent_ping(
                                nonce,
                                pending.sent_message_id,
                                Some(Instant::now() + PING_RESPONSE_DEADLINE),
                            );
                            let _ = response.send(Err(SyncError::Unavailable(
                                "sync ping timed out".to_owned(),
                            )));
                        } else {
                            heartbeat_failure = Some(pending);
                        }
                    }
                }
                if let Some(pending) = heartbeat_failure {
                    log_session_liveness_failure(
                        runtime,
                        SyncObservation {
                            connection_id: session_id,
                            peer_node_id: remote_node_id,
                            direction: direction_name(direction),
                            correlation_id: &pending.correlation_id,
                        },
                        "heartbeat_response",
                        "heartbeat_timeout",
                        IDLE_HEARTBEAT_INTERVAL.saturating_add(pending.started.elapsed()),
                    );
                    runtime.remove_session(remote_node_id, session_id).await;
                    channel.close(
                        SyncCloseCode::InternalError,
                        "sync session heartbeat timed out",
                        &pending.correlation_id,
                        Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                    ).await;
                    break 'session;
                }
            }
            _ = &mut heartbeat_idle, if pings.is_empty() => {
                let correlation_id = new_correlation_id();
                let nonce = match random_ping_nonce() {
                    Ok(nonce) => nonce,
                    Err(error) => {
                        log_session_liveness_failure(
                            runtime,
                            SyncObservation {
                                connection_id: session_id,
                                peer_node_id: remote_node_id,
                                direction: direction_name(direction),
                                correlation_id: &correlation_id,
                            },
                            "heartbeat_send",
                            "heartbeat_nonce_generation_failed",
                            Duration::ZERO,
                        );
                        runtime.remove_session(remote_node_id, session_id).await;
                        let _ = error;
                        break 'session;
                    }
                };
                let deadline = Instant::now() + HEARTBEAT_RESPONSE_DEADLINE;
                let message_id = match channel.send(
                    sync_envelope::Payload::Ping(SyncPing {
                        nonce,
                        sent_at: Some(system_timestamp()),
                    }),
                    &correlation_id,
                    None,
                    Some(deadline),
                ).await {
                    Ok(message_id) => message_id,
                    Err(_) => {
                        log_session_liveness_failure(
                            runtime,
                            SyncObservation {
                                connection_id: session_id,
                                peer_node_id: remote_node_id,
                                direction: direction_name(direction),
                                correlation_id: &correlation_id,
                            },
                            "heartbeat_send",
                            "heartbeat_send_failed",
                            Duration::ZERO,
                        );
                        runtime.remove_session(remote_node_id, session_id).await;
                        channel.close(
                            SyncCloseCode::InternalError,
                            "sync session heartbeat send failed",
                            &correlation_id,
                            Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                        ).await;
                        break 'session;
                    }
                };
                if pings.insert(nonce, PendingPing {
                    sent_message_id: message_id,
                    started: Instant::now(),
                    deadline,
                    correlation_id: correlation_id.clone(),
                    response: None,
                }).is_some() {
                    log_session_liveness_failure(
                        runtime,
                        SyncObservation {
                            connection_id: session_id,
                            peer_node_id: remote_node_id,
                            direction: direction_name(direction),
                            correlation_id: &correlation_id,
                        },
                        "heartbeat_send",
                        "heartbeat_nonce_collision",
                        Duration::ZERO,
                    );
                    runtime.remove_session(remote_node_id, session_id).await;
                    break 'session;
                }
            }
            received = channel.receive(None) => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::ReplicaAvailable,
                        ..
                    }) if mode == SessionReplicaMode::Waiting => {
                        disposition = ConnectionDisposition::ReconnectImmediately;
                        break;
                    }
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::DuplicateSession,
                        ..
                    }) => {
                        disposition = ConnectionDisposition::SuppressedByActiveSession(remote_node_id);
                        break;
                    }
                    Err(SessionError::RemoteClosed { .. }) => break,
                    Err(_) => {
                        let correlation_id = new_correlation_id();
                        channel.close(
                            SyncCloseCode::ProtocolViolation,
                            "invalid ready-session message",
                            &correlation_id,
                            Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                        ).await;
                        break;
                    }
                };
                match channel.consume_transparent_pong(&envelope) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(_) => {
                        channel.close(
                            SyncCloseCode::ProtocolViolation,
                            "sync keepalive reply is invalid",
                            &envelope.correlation_id,
                            Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                        ).await;
                        break;
                    }
                }
                match envelope.payload {
                    Some(sync_envelope::Payload::Ping(ping)) => {
                        if channel.send(
                            sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                            &envelope.correlation_id,
                            Some(envelope.message_id),
                            Some(Instant::now() + HEARTBEAT_RESPONSE_DEADLINE),
                        ).await.is_err() {
                            break;
                        }
                    }
                    Some(sync_envelope::Payload::Pong(pong)) => {
                        let Some(pending) = pings.remove(&pong.nonce) else {
                            channel.close(
                                SyncCloseCode::ProtocolViolation,
                                "received an unknown SyncPong",
                                &envelope.correlation_id,
                                Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                            ).await;
                            break;
                        };
                        if envelope.reply_to != Some(pending.sent_message_id) {
                            if let Some(response) = pending.response {
                                let _ = response.send(Err(SyncError::Protocol("SyncPong reply_to does not name its request".to_owned())));
                            }
                            channel.close(
                                SyncCloseCode::ProtocolViolation,
                                "SyncPong reply_to is invalid",
                                &envelope.correlation_id,
                                Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                            ).await;
                            break;
                        }
                        if let Some(response) = pending.response {
                            let _ = response.send(Ok(pending.started.elapsed()));
                        } else {
                            runtime.logger.emit(
                                LogLevel::Trace,
                                "oll::sync",
                                "sync_session_heartbeat_succeeded",
                                &pending.correlation_id,
                                json!({
                                    "connection_id": session_id.to_string(),
                                    "peer_node_id": remote_node_id.to_string(),
                                    "direction": direction_name(direction),
                                    "duration_ms": u64::try_from(pending.started.elapsed().as_millis())
                                        .unwrap_or(u64::MAX),
                                }),
                            );
                        }
                    }
                    Some(sync_envelope::Payload::RoundRequest(_)) => {
                        if mode == SessionReplicaMode::Waiting
                            || envelope.reply_to.is_some()
                        {
                            channel.close(
                                SyncCloseCode::ProtocolViolation,
                                "unexpected sync round request",
                                &envelope.correlation_id,
                                Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                            ).await;
                            break;
                        }
                        make_pending_pings_transparent(&mut channel, &mut pings);
                        runtime.logger.emit(
                            LogLevel::Info,
                            "oll::sync",
                            "sync_round_request_received",
                            &envelope.correlation_id,
                            json!({
                                "connection_id": session_id.to_string(),
                                "peer_node_id": remote_node_id.to_string(),
                                "message_id": envelope.message_id,
                            }),
                        );
                        let observation = SyncObservation {
                            connection_id: session_id,
                            peer_node_id: remote_node_id,
                            direction: direction_name(direction),
                            correlation_id: &envelope.correlation_id,
                        };
                        let result = source_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            observation,
                            envelope.message_id,
                            max_chunk_bytes,
                        )
                        .await;
                        if let Err(error) = &result {
                            log_round_session_failure(runtime, observation, error);
                        }
                        if result.is_err() {
                            runtime.remove_session(remote_node_id, session_id).await;
                            channel.close(
                                SyncCloseCode::InternalError,
                                "sync round invalidated this session",
                                &envelope.correlation_id,
                                Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                            ).await;
                            break;
                        }
                    }
                    _ => {
                        channel.close(
                            SyncCloseCode::ProtocolViolation,
                            "message is not valid in a ready idle session",
                            &envelope.correlation_id,
                            Some(Instant::now() + SESSION_CLOSE_DEADLINE),
                        ).await;
                        break;
                    }
                }
            }
        }
    }
    for (_, pending) in pings {
        if let Some(response) = pending.response {
            let _ = response.send(Err(SyncError::Unavailable(
                "sync session closed during ping".to_owned(),
            )));
        }
    }
    disposition
}

fn log_replica_available(
    runtime: &SyncRuntime,
    correlation_id: &str,
    replica_status: &watch::Receiver<ReplicaStatus>,
) {
    let replica_id = match *replica_status.borrow() {
        ReplicaStatus::Uninitialized => None,
        ReplicaStatus::InitializedEmpty { replica_id }
        | ReplicaStatus::InitializedPopulated { replica_id } => Some(replica_id.to_string()),
    };
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_replica_available",
        correlation_id,
        json!({ "replica_id": replica_id }),
    );
}
