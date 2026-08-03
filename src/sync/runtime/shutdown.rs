use super::*;

impl SyncRuntime {
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<(), SyncError> {
        self.accepting_tasks.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
        for session in self.sessions.lock().await.values() {
            let _ = session.cancel.send(Some(SyncCloseCode::ShuttingDown));
        }
        self.session_changed.notify_waiters();
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .expect("sync task registry lock is poisoned"),
        );
        for mut task in tasks {
            match timeout_at(deadline, &mut task).await {
                Ok(_) => {}
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        let sessions_exceeded_deadline = {
            let mut sessions = self.sessions.lock().await;
            let exceeded = !sessions.is_empty();
            sessions.clear();
            exceeded
        };
        self.session_changed.notify_waiters();
        if sessions_exceeded_deadline {
            return Err(SyncError::Unavailable(
                "sync sessions exceeded the node shutdown deadline".to_owned(),
            ));
        }
        Ok(())
    }
}
