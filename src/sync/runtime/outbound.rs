use super::*;

pub(super) async fn run_outbound(
    runtime: std::sync::Weak<SyncRuntime>,
    target: ConnectUrl,
    mut shutdown: watch::Receiver<bool>,
) {
    let target_string = target.to_string();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let Some(runtime) = runtime.upgrade() else {
            break;
        };
        if *shutdown.borrow() {
            break;
        }
        let correlation_id = new_correlation_id();
        runtime
            .target_states
            .write()
            .await
            .insert(target_string.clone(), PeerConnectionState::Connecting);
        runtime.session_changed.notify_waiters();
        let connection = timeout_at(
            Instant::now() + CONNECT_DEADLINE,
            TcpStream::connect((target.host(), target.port())),
        )
        .await;
        let disposition = match connection {
            Ok(Ok(stream)) => {
                backoff = INITIAL_BACKOFF;
                run_connection(
                    Arc::clone(&runtime),
                    stream,
                    Direction::Outbound,
                    Some(target_string.clone()),
                    correlation_id.clone(),
                )
                .await
            }
            Ok(Err(error)) => {
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_connect_failed",
                    &correlation_id,
                    json!({
                        "connect_target": &target_string,
                        "error_kind": format!("{:?}", error.kind()),
                    }),
                );
                ConnectionDisposition::RetryWithBackoff
            }
            Err(_) => {
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_connect_failed",
                    &correlation_id,
                    json!({
                        "connect_target": &target_string,
                        "error_kind": "timeout",
                    }),
                );
                ConnectionDisposition::RetryWithBackoff
            }
        };
        if *shutdown.borrow() {
            break;
        }
        match disposition {
            ConnectionDisposition::ReconnectImmediately => {
                runtime
                    .target_states
                    .write()
                    .await
                    .insert(target_string.clone(), PeerConnectionState::Connecting);
                runtime.session_changed.notify_waiters();
                runtime.logger.emit(
                    LogLevel::Info,
                    "oll::sync",
                    "sync_replica_renegotiation_started",
                    &correlation_id,
                    json!({ "connect_target": &target_string }),
                );
                continue;
            }
            ConnectionDisposition::SuppressedByActiveSession(remote_node_id) => {
                let mut suppression_logged = false;
                loop {
                    let notified = runtime.session_changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if !runtime.sessions.lock().await.contains_key(&remote_node_id) {
                        break;
                    }
                    if !suppression_logged {
                        runtime.logger.emit(
                            LogLevel::Info,
                            "oll::sync",
                            "sync_duplicate_outbound_suppressed",
                            &correlation_id,
                            json!({
                                "connect_target": &target_string,
                                "peer_node_id": remote_node_id.to_string(),
                            }),
                        );
                        suppression_logged = true;
                    }
                    tokio::select! {
                        _ = &mut notified => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow_and_update() {
                                break;
                            }
                        }
                    }
                    if *shutdown.borrow() {
                        break;
                    }
                }
                if *shutdown.borrow() {
                    break;
                }
                backoff = INITIAL_BACKOFF;
                continue;
            }
            ConnectionDisposition::RetryWithBackoff => {}
        }
        runtime
            .target_states
            .write()
            .await
            .insert(target_string.clone(), PeerConnectionState::Backoff);
        runtime.session_changed.notify_waiters();
        let delay = jittered(backoff);
        backoff = backoff.saturating_mul(2).min(MAXIMUM_BACKOFF);
        runtime.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_reconnect_scheduled",
            &correlation_id,
            json!({
                "connect_target": &target_string,
                "backoff_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            }),
        );
        tokio::select! {
            _ = sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
            }
        }
    }
}
