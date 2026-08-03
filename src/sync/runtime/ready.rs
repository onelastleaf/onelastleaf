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
) {
    if mode != SessionReplicaMode::Normal {
        let correlation_id = new_correlation_id();
        channel
            .close(
                SyncCloseCode::InternalError,
                "bootstrap session is not ready for transfer",
                &correlation_id,
                None,
            )
            .await;
        return;
    }
    let mut shutdown = runtime.shutdown.subscribe();
    let mut epoch = runtime.identities.subscribe_epoch();
    let mut pings = HashMap::<u64, PendingPing>::new();
    loop {
        let immediate_close = if *shutdown.borrow() {
            Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"))
        } else if let Some(code) = *cancel.borrow() {
            Some((code, "sync session was superseded"))
        } else if *epoch.borrow() != bound_epoch {
            Some((
                SyncCloseCode::Normal,
                "local identity changed; reconnect required",
            ))
        } else {
            None
        };
        if let Some((code, message)) = immediate_close {
            let correlation_id = new_correlation_id();
            channel.close(code, message, &correlation_id, None).await;
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
                        if !pings.is_empty() || envelope.reply_to.is_some() {
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
}
