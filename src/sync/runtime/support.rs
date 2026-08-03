use super::*;

pub(super) fn round_error_to_sync(error: RoundError) -> SyncError {
    match error {
        RoundError::Session(error) => SyncError::Unavailable(error.to_string()),
        RoundError::Replica(ReplicaError::RevisionConflict(message)) => {
            SyncError::Unavailable(message)
        }
        RoundError::Replica(ReplicaError::Uninitialized) => {
            SyncError::FailedPrecondition("no local replica yet".to_owned())
        }
        RoundError::Replica(_) => SyncError::Store,
        RoundError::Protocol(message) => SyncError::Protocol(message.to_owned()),
        RoundError::Rejected(message) => SyncError::Unavailable(message),
    }
}

pub(super) fn sync_error_code(error: &SyncError) -> ErrorCode {
    match error {
        SyncError::NotFound(_) => ErrorCode::NotFound,
        SyncError::FailedPrecondition(_) => ErrorCode::FailedPrecondition,
        SyncError::Unavailable(_) => ErrorCode::Unavailable,
        SyncError::Protocol(_) => ErrorCode::ProtocolMismatch,
        SyncError::Store | SyncError::Internal(_) => ErrorCode::Internal,
    }
}

pub(super) fn sync_error_name(error: &SyncError) -> &'static str {
    match sync_error_code(error) {
        ErrorCode::NotFound => "not_found",
        ErrorCode::FailedPrecondition => "failed_precondition",
        ErrorCode::Unavailable => "unavailable",
        ErrorCode::ProtocolMismatch => "protocol",
        _ => "internal",
    }
}

pub(super) fn system_timestamp() -> prost_types::Timestamp {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        },
    }
}

pub(super) fn jittered(base: Duration) -> Duration {
    let mut bytes = [0_u8; 4];
    if fill_random(&mut bytes).is_err() {
        return base;
    }
    let upper_millis = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
    if upper_millis == 0 {
        return base;
    }
    base.saturating_add(Duration::from_millis(
        u64::from(u32::from_be_bytes(bytes)) % upper_millis,
    ))
}

pub(super) fn session_error_code(error: &SessionError) -> &'static str {
    match error {
        SessionError::Transport(_) => "transport",
        SessionError::LocalProtocol { .. } => "protocol",
        SessionError::RemoteClosed { .. } => "remote_close",
    }
}
