use tokio::time::{Instant, timeout_at};

use super::{super::ReplicaError, types::ReplicaRuntime};

impl ReplicaRuntime {
    pub async fn shutdown(&self, deadline: Instant) -> Result<(), ReplicaError> {
        self.take_watcher();
        let _ = self.event_shutdown.send(true);
        let Some(mut task) = self.event_task.lock().await.take() else {
            return Ok(());
        };
        match timeout_at(deadline, &mut task).await {
            Ok(result) => result.map_err(|error| {
                ReplicaError::Internal(format!("filesystem watcher task failed: {error}"))
            }),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(ReplicaError::Internal(
                    "filesystem watcher exceeded the graceful shutdown deadline".to_owned(),
                ))
            }
        }
    }
}
