use super::*;

impl SyncRuntime {
    pub(super) async fn register_session(
        &self,
        remote: NodeIdentity,
        direction: Direction,
        connect_target: Option<String>,
        handshake_hash: [u8; 32],
        commands: mpsc::Sender<SessionCommand>,
        cancel: watch::Sender<Option<SyncCloseCode>>,
    ) -> Result<Uuid, SyncError> {
        let local_id = self.identities.node_id().await;
        let preferred_direction = match direction {
            Direction::Outbound => local_id < remote.node_id(),
            Direction::Inbound => remote.node_id() < local_id,
        };
        let session_id = Uuid::new_v4();
        let mut sessions = self.sessions.lock().await;
        if let Some(existing) = sessions.get(&remote.node_id()) {
            let existing_rank = (!existing.preferred_direction, existing.handshake_hash);
            let new_rank = (!preferred_direction, handshake_hash);
            if existing_rank <= new_rank {
                return Err(SyncError::Protocol(
                    "duplicate sync session lost arbitration".to_owned(),
                ));
            }
            let _ = existing.cancel.send(Some(SyncCloseCode::DuplicateSession));
        }
        sessions.insert(
            remote.node_id(),
            ActiveSession {
                session_id,
                direction,
                connect_target,
                preferred_direction,
                handshake_hash,
                commands,
                cancel,
            },
        );
        drop(sessions);
        self.session_changed.notify_waiters();
        Ok(session_id)
    }

    pub(super) async fn remove_session(&self, remote_id: Uuid, session_id: Uuid) {
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(&remote_id)
            .is_some_and(|session| session.session_id == session_id)
        {
            sessions.remove(&remote_id);
        }
        drop(sessions);
        self.session_changed.notify_waiters();
    }

    pub(super) async fn refresh_bindings(&self) -> Result<(), SyncError> {
        *self.bindings.write().await = self
            .replica
            .sync_peer_bindings()
            .await
            .map_err(|_| SyncError::Store)?;
        self.session_changed.notify_waiters();
        Ok(())
    }

    pub(super) fn log_transport_failure(
        &self,
        correlation_id: &str,
        direction: Direction,
        error: &SessionError,
    ) {
        self.logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "sync_session_failed",
            correlation_id,
            json!({
                "direction": match direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" },
                "error_code": session_error_code(error),
            }),
        );
    }
}
