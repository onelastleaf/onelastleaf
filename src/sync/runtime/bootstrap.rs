use super::*;

pub(super) async fn run_bootstrap_session(
    runtime: &SyncRuntime,
    mut pending: PendingSession<TcpStream>,
    bound_epoch: u64,
    bootstrap_claim: Option<BootstrapClaim>,
    mut bootstrap_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    correlation_id: &str,
) {
    let replica_id = pending
        .replica_id
        .expect("bootstrap sessions always negotiate a ReplicaId");
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_bootstrap_started",
        correlation_id,
        json!({
            "source_node_id": match pending.mode {
                SessionReplicaMode::BootstrapSource => runtime.identities.node_id().await,
                SessionReplicaMode::BootstrapReceiver => pending.remote.node_id(),
                SessionReplicaMode::Waiting | SessionReplicaMode::Normal => {
                    unreachable!("ready sessions use the ready-session loop")
                }
            }.to_string(),
            "replica_id": replica_id.to_string(),
        }),
    );
    let mut shutdown = runtime.shutdown.subscribe();
    let mut epoch = runtime.identities.subscribe_epoch();
    // A receiver holds the identity commit gate for the whole transfer. Its own
    // successful activation advances this epoch before projection finishes;
    // external identity replacements cannot pass the gate until the guard drops.
    let watch_identity_changes_during_transfer =
        pending.mode == SessionReplicaMode::BootstrapSource;
    let mut cancellation = if *shutdown.borrow() {
        Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"))
    } else if *epoch.borrow() != bound_epoch {
        Some((
            SyncCloseCode::Normal,
            "local identity changed; reconnect required",
        ))
    } else {
        None
    };
    let mut work = Box::pin(async {
        match pending.mode {
            SessionReplicaMode::BootstrapSource => {
                match runtime.replica.capture_bootstrap_source().await {
                    Ok(source) if source.inventory.replica_id == replica_id => {
                        send_bootstrap_round(
                            &mut pending.channel,
                            &runtime.replica,
                            &source,
                            correlation_id,
                            pending.max_chunk_bytes,
                        )
                        .await
                        .map_err(round_error_to_sync)
                    }
                    Ok(_) => Err(SyncError::Protocol(
                        "local ReplicaId changed after bootstrap negotiation".to_owned(),
                    )),
                    Err(error) => Err(round_error_to_sync(RoundError::Replica(error))),
                }
            }
            SessionReplicaMode::BootstrapReceiver => {
                let received = pending.channel.receive(None).await;
                match received {
                    Ok(envelope)
                        if envelope.correlation_id == correlation_id
                            && envelope.reply_to.is_none() =>
                    {
                        match envelope.payload {
                            Some(sync_envelope::Payload::RoundStart(start)) => {
                                let claim = bootstrap_claim.as_ref().expect(
                                    "bootstrap receiver acquired its claim before SyncReady",
                                );
                                let guard = bootstrap_guard.as_ref().expect(
                                    "bootstrap receiver acquired its commit guard before SyncReady",
                                );
                                let writer_node_id = runtime.identities.node_id().await;
                                receive_bootstrap_round(
                                    &mut pending.channel,
                                    &runtime.replica,
                                    envelope.message_id,
                                    start,
                                    correlation_id,
                                    pending.max_chunk_bytes,
                                    claim.claim_id,
                                    replica_id,
                                    guard,
                                    writer_node_id,
                                )
                                .await
                                .map_err(round_error_to_sync)
                            }
                            _ => Err(SyncError::Protocol(
                                "bootstrap source did not begin a bootstrap round".to_owned(),
                            )),
                        }
                    }
                    Ok(_) => Err(SyncError::Protocol(
                        "bootstrap round metadata differs from its inherited correlation"
                            .to_owned(),
                    )),
                    Err(error) => Err(SyncError::Unavailable(error.to_string())),
                }
            }
            SessionReplicaMode::Waiting | SessionReplicaMode::Normal => {
                unreachable!("ready sessions use the ready-session loop")
            }
        }
    });
    let result = if cancellation.is_some() {
        Err(SyncError::Unavailable(
            "bootstrap session was cancelled".to_owned(),
        ))
    } else {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                cancellation = Some((SyncCloseCode::ShuttingDown, "local daemon is shutting down"));
                Err(SyncError::Unavailable("bootstrap session was cancelled by shutdown".to_owned()))
            }
            _ = async {
                if watch_identity_changes_during_transfer {
                    let _ = epoch.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                cancellation = Some((SyncCloseCode::Normal, "local identity changed; reconnect required"));
                Err(SyncError::Unavailable("bootstrap session was cancelled by an identity change".to_owned()))
            }
            result = &mut work => result,
        }
    };
    drop(work);

    if let Some(claim) = bootstrap_claim
        && runtime
            .replica
            .release_bootstrap_claim(claim.claim_id)
            .await
            .is_err()
    {
        runtime.logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "sync_bootstrap_claim_release_failed",
            correlation_id,
            json!({ "claim_id": claim.claim_id.to_string() }),
        );
    }
    drop(bootstrap_guard.take());

    if let Some((code, message)) = cancellation {
        runtime.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_bootstrap_cancelled",
            correlation_id,
            json!({
                "reason": if code == SyncCloseCode::ShuttingDown {
                    "shutdown"
                } else {
                    "identity_changed"
                },
            }),
        );
        pending
            .channel
            .close(code, message, correlation_id, None)
            .await;
        return;
    }

    match result {
        Ok(result) => {
            runtime.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_bootstrap_completed",
                correlation_id,
                json!({
                    "replica_id": replica_id.to_string(),
                    "object_count": result.object_count,
                    "blob_count": result.blob_count,
                    "bytes": result.transferred_bytes,
                }),
            );
            pending
                .channel
                .close(
                    SyncCloseCode::Normal,
                    "bootstrap completed; reconnect for normal sync",
                    correlation_id,
                    None,
                )
                .await;
        }
        Err(error) => {
            runtime.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_bootstrap_failed",
                correlation_id,
                json!({ "error_code": sync_error_name(&error) }),
            );
            let close_code = match error {
                SyncError::Protocol(_) => SyncCloseCode::ProtocolViolation,
                _ => SyncCloseCode::InternalError,
            };
            pending
                .channel
                .close(
                    close_code,
                    "bootstrap did not complete",
                    correlation_id,
                    None,
                )
                .await;
        }
    }
}
