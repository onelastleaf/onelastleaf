use super::*;

pub(super) async fn run_listener(
    runtime: std::sync::Weak<SyncRuntime>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let stopping = changed.is_err() || *shutdown.borrow_and_update();
                if stopping {
                    break;
                }
                continue;
            }
            accepted = listener.accept() => accepted,
        };
        let Some(runtime) = runtime.upgrade() else {
            break;
        };
        match accepted {
            Ok((stream, _)) => {
                let connection_runtime = Arc::clone(&runtime);
                let correlation_id = new_correlation_id();
                runtime.spawn(async move {
                    run_connection(
                        connection_runtime,
                        stream,
                        Direction::Inbound,
                        None,
                        correlation_id,
                    )
                    .await;
                });
            }
            Err(error) => {
                let correlation_id = new_correlation_id();
                runtime.logger.emit(
                    LogLevel::Warn,
                    "oll::sync",
                    "sync_listener_accept_failed",
                    &correlation_id,
                    json!({ "error_kind": format!("{:?}", error.kind()) }),
                );
            }
        }
    }
}
