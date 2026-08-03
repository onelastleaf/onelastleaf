use super::*;

impl SyncRuntime {
    pub(crate) async fn ping(
        &self,
        node_name: &NodeName,
        correlation_id: &str,
    ) -> Result<(NodeIdentity, Duration), SyncError> {
        let started = Instant::now();
        self.logger.emit(
            LogLevel::Info,
            "oll::sync",
            "sync_ping_started",
            correlation_id,
            json!({ "peer_node_name": node_name.as_str() }),
        );
        let result = async {
            let binding = self
                .bindings
                .read()
                .await
                .iter()
                .find(|binding| binding.identity.node_name() == node_name)
                .cloned()
                .ok_or_else(|| {
                    SyncError::NotFound("sync node name has not been authenticated".to_owned())
                })?;
            let deadline = Instant::now() + PING_CALL_DEADLINE;
            let commands = loop {
                let notified = self.session_changed.notified();
                if let Some(commands) = self
                    .sessions
                    .lock()
                    .await
                    .get(&binding.identity.node_id())
                    .map(|session| session.commands.clone())
                {
                    break commands;
                }
                timeout_at(deadline, notified).await.map_err(|_| {
                    SyncError::Unavailable("authenticated sync peer is not connected".to_owned())
                })?;
            };
            let (response, receiver) = oneshot::channel();
            commands
                .send(SessionCommand::Ping {
                    correlation_id: correlation_id.to_owned(),
                    response,
                })
                .await
                .map_err(|_| {
                    SyncError::Unavailable("sync peer session closed before ping".to_owned())
                })?;
            let duration = timeout_at(deadline, receiver)
                .await
                .map_err(|_| SyncError::Unavailable("sync ping timed out".to_owned()))?
                .map_err(|_| {
                    SyncError::Unavailable("sync peer session closed during ping".to_owned())
                })??;
            Ok((binding.identity, duration))
        }
        .await;
        match &result {
            Ok((identity, duration)) => self.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_ping_completed",
                correlation_id,
                json!({
                    "peer_node_id": identity.node_id().to_string(),
                    "peer_node_name": identity.node_name().as_str(),
                    "round_trip_ms": u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            ),
            Err(error) => self.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_ping_failed",
                correlation_id,
                json!({
                    "peer_node_name": node_name.as_str(),
                    "error_code": sync_error_name(error),
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            ),
        }
        result
    }

    pub(crate) async fn synchronize(
        &self,
        node_name: Option<&NodeName>,
        total_attempts: u32,
        correlation_id: &str,
    ) -> Result<Vec<PeerSyncResult>, SyncError> {
        if total_attempts == 0 {
            return Err(SyncError::FailedPrecondition(
                "synchronization attempts must be greater than zero".to_owned(),
            ));
        }
        let bindings = self.bindings.read().await.clone();
        let targets = if let Some(node_name) = node_name {
            let binding = bindings
                .iter()
                .find(|binding| binding.identity.node_name() == node_name)
                .ok_or_else(|| {
                    SyncError::NotFound("sync node name has not been authenticated".to_owned())
                })?;
            let connect_target = binding
                .connect_targets
                .iter()
                .find(|candidate| {
                    self.configured_targets
                        .iter()
                        .any(|configured| configured.to_string() == candidate.as_str())
                })
                .cloned();
            vec![(connect_target, Some(binding.identity.clone()))]
        } else {
            if self.configured_targets.is_empty() {
                return Err(SyncError::FailedPrecondition(
                    "no configured sync peers are available".to_owned(),
                ));
            }
            self.configured_targets
                .iter()
                .map(|target| {
                    let target = target.to_string();
                    let identity = bindings
                        .iter()
                        .find(|binding| {
                            binding.connect_targets.iter().any(|known| known == &target)
                        })
                        .map(|binding| binding.identity.clone());
                    (Some(target), identity)
                })
                .collect()
        };

        let mut results = Vec::with_capacity(targets.len());
        for (connect_target, mut identity) in targets {
            self.logger.emit(
                LogLevel::Info,
                "oll::sync",
                "sync_round_started",
                correlation_id,
                json!({
                    "connect_target": connect_target,
                    "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                    "total_attempts": total_attempts,
                }),
            );
            let mut last_error = None;
            let mut success = None;
            let mut attempts_used = 0;
            for attempt in 1..=total_attempts {
                attempts_used = attempt;
                match self
                    .synchronize_once(connect_target.as_deref(), &mut identity, correlation_id)
                    .await
                {
                    Ok(result) => {
                        success = Some(result);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            let result = match success {
                Some(round) => {
                    let outcome = if round == RoundResult::default() {
                        PeerSyncOutcome::AlreadySatisfied
                    } else {
                        PeerSyncOutcome::Synchronized
                    };
                    self.logger.emit(
                        LogLevel::Info,
                        "oll::sync",
                        "sync_round_completed",
                        correlation_id,
                        json!({
                            "connect_target": connect_target,
                            "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                            "attempts_used": attempts_used,
                            "object_count": round.object_count,
                            "blob_count": round.blob_count,
                            "bytes": round.transferred_bytes,
                        }),
                    );
                    PeerSyncResult {
                        connect_target,
                        node: identity.as_ref().map(NodeIdentity::to_proto),
                        attempts_used,
                        outcome: outcome as i32,
                        object_count: round.object_count,
                        blob_count: round.blob_count,
                        transferred_bytes: round.transferred_bytes,
                        error_code: ErrorCode::Unspecified as i32,
                        error_message: String::new(),
                    }
                }
                None => {
                    let error = last_error.unwrap_or_else(|| {
                        SyncError::Unavailable("sync attempt did not run".to_owned())
                    });
                    self.logger.emit(
                        LogLevel::Warn,
                        "oll::sync",
                        "sync_round_failed",
                        correlation_id,
                        json!({
                            "connect_target": connect_target,
                            "peer_node_id": identity.as_ref().map(|peer| peer.node_id().to_string()),
                            "attempts_used": attempts_used,
                            "error_code": sync_error_name(&error),
                        }),
                    );
                    PeerSyncResult {
                        connect_target,
                        node: identity.as_ref().map(NodeIdentity::to_proto),
                        attempts_used,
                        outcome: PeerSyncOutcome::Failed as i32,
                        object_count: 0,
                        blob_count: 0,
                        transferred_bytes: 0,
                        error_code: sync_error_code(&error) as i32,
                        error_message: error.to_string(),
                    }
                }
            };
            results.push(result);
        }
        Ok(results)
    }

    async fn synchronize_once(
        &self,
        connect_target: Option<&str>,
        identity: &mut Option<NodeIdentity>,
        correlation_id: &str,
    ) -> Result<RoundResult, SyncError> {
        let deadline = Instant::now() + SESSION_WAIT_DEADLINE;
        let commands = loop {
            let notified = self.session_changed.notified();
            if identity.is_none() {
                *identity = self
                    .bindings
                    .read()
                    .await
                    .iter()
                    .find(|binding| {
                        connect_target.is_some_and(|target| {
                            binding.connect_targets.iter().any(|known| known == target)
                        })
                    })
                    .map(|binding| binding.identity.clone());
            }
            if let Some(identity) = identity
                && let Some(commands) = self
                    .sessions
                    .lock()
                    .await
                    .get(&identity.node_id())
                    .map(|session| session.commands.clone())
            {
                break commands;
            }
            timeout_at(deadline, notified).await.map_err(|_| {
                SyncError::Unavailable("authenticated sync peer is not connected".to_owned())
            })?;
        };
        let (response, receiver) = oneshot::channel();
        commands
            .send(SessionCommand::Synchronize {
                correlation_id: correlation_id.to_owned(),
                response,
            })
            .await
            .map_err(|_| {
                SyncError::Unavailable("sync peer session closed before the round".to_owned())
            })?;
        receiver.await.map_err(|_| {
            SyncError::Unavailable("sync peer session closed during the round".to_owned())
        })?
    }
}
