use super::*;

pub(super) async fn run_connection(
    runtime: Arc<SyncRuntime>,
    stream: TcpStream,
    direction: Direction,
    connect_target: Option<String>,
    correlation_id: String,
) {
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_started",
        &correlation_id,
        json!({
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
            "connect_target": connect_target.as_deref(),
        }),
    );
    let Some(psk) = runtime.psk.as_ref() else {
        return;
    };
    let _ = stream.set_nodelay(true);
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let transport = match direction {
        Direction::Outbound => NoiseTransport::connect(stream, psk, deadline).await,
        Direction::Inbound => NoiseTransport::accept(stream, psk, deadline).await,
    };
    let transport = match transport {
        Ok(transport) => transport,
        Err(error) => {
            runtime.log_session_failure(
                &correlation_id,
                direction,
                "transport_handshake",
                connect_target.as_deref(),
                &SessionError::Transport(error),
            );
            return;
        }
    };
    let bound_epoch = runtime.identities.epoch();
    let local_identity = runtime.identities.node().await;
    let mut pending = match PendingSession::begin(
        transport,
        &local_identity,
        runtime.replica.status().await,
        &correlation_id,
        deadline,
    )
    .await
    {
        Ok(pending) => pending,
        Err(error) => {
            runtime.log_session_failure(
                &correlation_id,
                direction,
                "sync_hello",
                connect_target.as_deref(),
                &error,
            );
            return;
        }
    };
    if runtime.identities.epoch() != bound_epoch {
        pending
            .channel
            .close(
                SyncCloseCode::Normal,
                "local identity changed during handshake",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    let operation_correlation_id = pending
        .bootstrap_correlation_id
        .clone()
        .unwrap_or_else(|| correlation_id.clone());
    if let Err(error) = runtime
        .replica
        .bind_sync_peer(&pending.remote, connect_target.as_deref())
        .await
    {
        let code = if matches!(error, ReplicaError::RevisionConflict(_)) {
            SyncCloseCode::IdentityCollision
        } else {
            SyncCloseCode::InternalError
        };
        pending
            .channel
            .close(
                code,
                "remote identity binding was rejected",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    let mut bootstrap_claim = None;
    let mut bootstrap_guard = None;
    if pending.mode == SessionReplicaMode::BootstrapReceiver {
        let claim = BootstrapClaim {
            claim_id: Uuid::new_v4(),
            source_node_id: pending.remote.node_id(),
            correlation_id: operation_correlation_id.clone(),
        };
        match runtime.replica.acquire_bootstrap_claim(&claim).await {
            Ok(true) => {
                runtime.logger.emit(
                    LogLevel::Info,
                    "oll::sync",
                    "sync_bootstrap_claim_acquired",
                    &operation_correlation_id,
                    json!({
                        "source_node_id": pending.remote.node_id().to_string(),
                        "claim_id": claim.claim_id.to_string(),
                    }),
                );
            }
            Ok(false) => {
                let (code, message) = match runtime.replica.status().await {
                    ReplicaStatus::Uninitialized => (
                        SyncCloseCode::BootstrapInProgress,
                        "another authenticated source is bootstrapping this replica",
                    ),
                    ReplicaStatus::InitializedEmpty { replica_id }
                    | ReplicaStatus::InitializedPopulated { replica_id }
                        if replica_id == pending.replica_id =>
                    {
                        (
                            SyncCloseCode::Normal,
                            "replica became initialized; reconnect for normal sync",
                        )
                    }
                    _ => (
                        SyncCloseCode::ReplicaMismatch,
                        "local replica changed while bootstrap was negotiated",
                    ),
                };
                pending
                    .channel
                    .close(code, message, &operation_correlation_id, Some(deadline))
                    .await;
                return;
            }
            Err(_) => {
                pending
                    .channel
                    .close(
                        SyncCloseCode::InternalError,
                        "bootstrap claim could not be persisted",
                        &operation_correlation_id,
                        Some(deadline),
                    )
                    .await;
                return;
            }
        }
        let guard = match timeout_at(deadline, runtime.identities.commit_guard_owned()).await {
            Ok(guard) => guard,
            Err(_) => {
                let _ = runtime
                    .replica
                    .release_bootstrap_claim(claim.claim_id)
                    .await;
                pending
                    .channel
                    .close(
                        SyncCloseCode::InternalError,
                        "bootstrap could not pause local commits before the handshake deadline",
                        &operation_correlation_id,
                        Some(deadline),
                    )
                    .await;
                return;
            }
        };
        if !matches!(runtime.replica.status().await, ReplicaStatus::Uninitialized) {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
            pending
                .channel
                .close(
                    SyncCloseCode::Normal,
                    "replica became initialized; reconnect for normal sync",
                    &operation_correlation_id,
                    Some(deadline),
                )
                .await;
            return;
        }
        bootstrap_claim = Some(claim);
        bootstrap_guard = Some(guard);
    }
    if runtime.refresh_bindings().await.is_err() {
        if let Some(claim) = bootstrap_claim.as_ref() {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
        }
        pending
            .channel
            .close(
                SyncCloseCode::InternalError,
                "peer directory reload failed",
                &correlation_id,
                Some(deadline),
            )
            .await;
        return;
    }
    if let Err(error) = pending.exchange_ready(&correlation_id, deadline).await {
        if let Some(claim) = bootstrap_claim.as_ref() {
            let _ = runtime
                .replica
                .release_bootstrap_claim(claim.claim_id)
                .await;
        }
        runtime.log_session_failure(
            &correlation_id,
            direction,
            "sync_ready",
            connect_target.as_deref(),
            &error,
        );
        return;
    }
    let remote = pending.remote.clone();
    let mode = pending.mode;
    let max_chunk_bytes = pending.max_chunk_bytes;
    let handshake_hash = *pending.channel.handshake_hash();
    if mode != SessionReplicaMode::Normal {
        run_bootstrap_session(
            &runtime,
            pending,
            bound_epoch,
            bootstrap_claim,
            bootstrap_guard,
            &operation_correlation_id,
        )
        .await;
        return;
    }
    let (commands_tx, commands_rx) = mpsc::channel(8);
    let (cancel_tx, cancel_rx) = watch::channel(None);
    let session_id = match runtime
        .register_session(
            remote.clone(),
            direction,
            connect_target.clone(),
            handshake_hash,
            commands_tx,
            cancel_tx,
        )
        .await
    {
        Ok(session_id) => session_id,
        Err(_) => {
            pending
                .channel
                .close(
                    SyncCloseCode::DuplicateSession,
                    "duplicate sync session lost arbitration",
                    &correlation_id,
                    None,
                )
                .await;
            return;
        }
    };
    if let Some(target) = connect_target.as_ref() {
        runtime
            .target_states
            .write()
            .await
            .insert(target.clone(), PeerConnectionState::Ready);
    }
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_ready",
        &correlation_id,
        json!({
            "connection_id": session_id.to_string(),
            "remote_node_id": remote.node_id().to_string(),
            "remote_node_name": remote.node_name().as_str(),
            "replica_id": pending.replica_id.to_string(),
            "connect_target": connect_target.as_deref(),
            "max_chunk_bytes": max_chunk_bytes,
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
        }),
    );
    run_ready_session(
        &runtime,
        pending.channel,
        bound_epoch,
        remote.node_id(),
        mode,
        max_chunk_bytes,
        commands_rx,
        cancel_rx,
    )
    .await;
    runtime.remove_session(remote.node_id(), session_id).await;
    runtime.logger.emit(
        LogLevel::Info,
        "oll::sync",
        "sync_session_closed",
        &correlation_id,
        json!({
            "connection_id": session_id.to_string(),
            "remote_node_id": remote.node_id().to_string(),
            "remote_node_name": remote.node_name().as_str(),
            "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
        }),
    );
}
