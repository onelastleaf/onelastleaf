use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_ready_session(
    runtime: &SyncRuntime,
    mut channel: SessionChannel<TcpStream>,
    bound_epoch: u64,
    remote_node_id: Uuid,
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
    loop {
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
                ConnectionDisposition::RetryWithBackoff,
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
            channel.close(code, message, &correlation_id, None).await;
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
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let stopping = changed.is_err() || *shutdown.borrow_and_update();
                if stopping {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::ShuttingDown, "local daemon is shutting down", &correlation_id, None).await;
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
                    channel.close(code, "sync session was superseded", &correlation_id, None).await;
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
                    channel.close(SyncCloseCode::InternalError, "local replica status notification closed", &correlation_id, None).await;
                    break;
                }
                let status = *replica_status.borrow_and_update();
                if !matches!(status, ReplicaStatus::Uninitialized) {
                    let correlation_id = new_correlation_id();
                    log_replica_available(runtime, &correlation_id, &replica_status);
                    channel.close(SyncCloseCode::ReplicaAvailable, "local replica became available; reconnect required", &correlation_id, None).await;
                    disposition = ConnectionDisposition::ReconnectImmediately;
                    break;
                }
            }
            changed = epoch.changed() => {
                let changed_identity = changed.is_err() || *epoch.borrow_and_update() != bound_epoch;
                if changed_identity {
                    let correlation_id = new_correlation_id();
                    channel.close(SyncCloseCode::Normal, "local identity changed; reconnect required", &correlation_id, None).await;
                    break;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    SessionCommand::Ping { correlation_id, response } => {
                        let mut nonce_bytes = [0_u8; 8];
                        if fill_random(&mut nonce_bytes).is_err() {
                            let _ = response.send(Err(SyncError::Internal("cannot generate sync ping nonce".to_owned())));
                            continue;
                        }
                        let nonce = u64::from_be_bytes(nonce_bytes);
                        let message_id = match channel.send(
                            sync_envelope::Payload::Ping(SyncPing {
                                nonce,
                                sent_at: Some(system_timestamp()),
                            }),
                            &correlation_id,
                            None,
                            None,
                        ).await {
                            Ok(message_id) => message_id,
                            Err(error) => {
                                let _ = response.send(Err(SyncError::Protocol(error.to_string())));
                                break;
                            }
                        };
                        if let Some(replaced) = pings.insert(nonce, PendingPing {
                            sent_message_id: message_id,
                            started: Instant::now(),
                            deadline: Instant::now() + PING_RESPONSE_DEADLINE,
                            response,
                        }) {
                            let _ = replaced.response.send(Err(SyncError::Internal("sync ping nonce collision".to_owned())));
                        }
                    }
                    SessionCommand::Synchronize { correlation_id, response } => {
                        if mode == SessionReplicaMode::Waiting {
                            let _ = response.send(Err(SyncError::FailedPrecondition(
                                "both peers are waiting for a local replica".to_owned(),
                            )));
                            continue;
                        }
                        if !pings.is_empty() {
                            let _ = response.send(Err(SyncError::Unavailable(
                                "another sync request is in flight on this session".to_owned(),
                            )));
                            continue;
                        }
                        let local_node_id = runtime.identities.node_id().await;
                        let mut round = Box::pin(request_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            local_node_id,
                            remote_node_id,
                            &correlation_id,
                            max_chunk_bytes,
                        ));
                        let (result, close) = tokio::select! {
                            biased;
                            _ = shutdown.changed() => (
                                None,
                                Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down")),
                            ),
                            changed = cancel.changed() => {
                                let code = if changed.is_err() {
                                    SyncCloseCode::Normal
                                } else {
                                    (*cancel.borrow_and_update()).unwrap_or(SyncCloseCode::Normal)
                                };
                                (None, Some((code, "sync session was superseded")))
                            }
                            _ = epoch.changed() => (
                                None,
                                Some((SyncCloseCode::Normal, "local identity changed; reconnect required")),
                            ),
                            result = &mut round => (Some(result), None),
                        };
                        drop(round);
                        if let Some((code, message)) = close {
                            let _ = response.send(Err(SyncError::Unavailable(message.to_owned())));
                            channel.close(code, message, &correlation_id, None).await;
                            break;
                        }
                        let result = result.expect("a completed round returns its result");
                        let protocol_failure = matches!(
                            result,
                            Err(SyncError::Protocol(_)) | Err(SyncError::Internal(_))
                        );
                        let _ = response.send(result);
                        if protocol_failure {
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
                for nonce in expired {
                    if let Some(pending) = pings.remove(&nonce) {
                        let _ = pending.response.send(Err(SyncError::Unavailable(
                            "sync ping timed out".to_owned(),
                        )));
                    }
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
                    Err(SessionError::RemoteClosed { .. }) => break,
                    Err(_) => {
                        let correlation_id = new_correlation_id();
                        channel.close(SyncCloseCode::ProtocolViolation, "invalid ready-session message", &correlation_id, None).await;
                        break;
                    }
                };
                match envelope.payload {
                    Some(sync_envelope::Payload::Ping(ping)) => {
                        if channel.send(
                            sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                            &envelope.correlation_id,
                            Some(envelope.message_id),
                            None,
                        ).await.is_err() {
                            break;
                        }
                    }
                    Some(sync_envelope::Payload::Pong(pong)) => {
                        let Some(pending) = pings.remove(&pong.nonce) else {
                            channel.close(SyncCloseCode::ProtocolViolation, "received an unknown SyncPong", &envelope.correlation_id, None).await;
                            break;
                        };
                        if envelope.reply_to != Some(pending.sent_message_id) {
                            let _ = pending.response.send(Err(SyncError::Protocol("SyncPong reply_to does not name its request".to_owned())));
                            channel.close(SyncCloseCode::ProtocolViolation, "SyncPong reply_to is invalid", &envelope.correlation_id, None).await;
                            break;
                        }
                        let _ = pending.response.send(Ok(pending.started.elapsed()));
                    }
                    Some(sync_envelope::Payload::RoundRequest(_)) => {
                        if mode == SessionReplicaMode::Waiting
                            || !pings.is_empty()
                            || envelope.reply_to.is_some()
                        {
                            channel.close(SyncCloseCode::ProtocolViolation, "unexpected sync round request", &envelope.correlation_id, None).await;
                            break;
                        }
                        let mut round = Box::pin(source_bidirectional_round(
                            &mut channel,
                            &runtime.replica,
                            &envelope.correlation_id,
                            envelope.message_id,
                            max_chunk_bytes,
                        ));
                        let (result, close) = tokio::select! {
                            biased;
                            _ = shutdown.changed() => (
                                None,
                                Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down")),
                            ),
                            changed = cancel.changed() => {
                                let code = if changed.is_err() {
                                    SyncCloseCode::Normal
                                } else {
                                    (*cancel.borrow_and_update()).unwrap_or(SyncCloseCode::Normal)
                                };
                                (None, Some((code, "sync session was superseded")))
                            }
                            _ = epoch.changed() => (
                                None,
                                Some((SyncCloseCode::Normal, "local identity changed; reconnect required")),
                            ),
                            result = &mut round => (Some(result), None),
                        };
                        drop(round);
                        if let Some((code, message)) = close {
                            channel.close(code, message, &envelope.correlation_id, None).await;
                            break;
                        }
                        if result
                            .expect("a completed reverse round returns its result")
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ => {
                        channel.close(SyncCloseCode::ProtocolViolation, "message is not valid in a ready idle session", &envelope.correlation_id, None).await;
                        break;
                    }
                }
            }
        }
    }
    for (_, pending) in pings {
        let _ = pending.response.send(Err(SyncError::Unavailable(
            "sync session closed during ping".to_owned(),
        )));
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
