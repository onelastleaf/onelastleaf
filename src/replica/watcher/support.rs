use serde_json::json;

use crate::node::logging::LogLevel;

use super::{super::ReplicaError, types::ReplicaRuntime};

impl ReplicaRuntime {
    pub(super) fn take_watcher(&self) {
        if let Ok(mut watcher) = self.watcher.lock() {
            watcher.take();
        }
    }

    pub(crate) fn log_failure(&self, event: &str, correlation_id: &str, error: &ReplicaError) {
        self.logger.emit(
            LogLevel::Error,
            "oll::replica",
            event,
            correlation_id,
            json!({ "error_code": error.code() }),
        )
    }
}

pub(super) fn elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
