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
        match connection {
            Ok(Ok(stream)) => {
                backoff = INITIAL_BACKOFF;
                run_connection(
                    Arc::clone(&runtime),
                    stream,
                    Direction::Outbound,
                    Some(target_string.clone()),
                    correlation_id.clone(),
                )
                .await;
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
            }
        }
        if *shutdown.borrow() {
            break;
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
