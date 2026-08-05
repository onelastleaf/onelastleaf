use tokio::time::Instant;

use super::{SESSION_FAILURE_DEADLINE, SESSION_FAILURE_GRACE};

pub(in crate::plugin::runtime) struct SessionOutcome {
    pub(in crate::plugin::runtime) failure: Option<String>,
    pub(in crate::plugin::runtime) correlation_id: String,
    pub(in crate::plugin::runtime) graceful_deadline: Instant,
    pub(in crate::plugin::runtime) absolute_deadline: Instant,
}

impl SessionOutcome {
    pub(in crate::plugin::runtime) fn failed(failure: String, correlation_id: String) -> Self {
        let now = Instant::now();
        Self {
            failure: Some(failure),
            correlation_id,
            graceful_deadline: now,
            absolute_deadline: now + SESSION_FAILURE_DEADLINE,
        }
    }

    pub(in crate::plugin::runtime) fn failed_after_shutdown(
        failure: String,
        correlation_id: String,
    ) -> Self {
        let now = Instant::now();
        Self {
            failure: Some(failure),
            correlation_id,
            graceful_deadline: now + SESSION_FAILURE_GRACE,
            absolute_deadline: now + SESSION_FAILURE_DEADLINE,
        }
    }

    pub(in crate::plugin::runtime) fn stopped(
        correlation_id: String,
        graceful_deadline: Instant,
        absolute_deadline: Instant,
        failure: Option<String>,
    ) -> Self {
        Self {
            failure,
            correlation_id,
            graceful_deadline,
            absolute_deadline,
        }
    }
}
