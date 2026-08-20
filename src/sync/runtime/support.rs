use super::*;

pub(super) fn round_error_to_sync(error: RoundError) -> SyncError {
    match error {
        RoundError::Session(SessionError::ProgressDeadlineExceeded { failure_stage }) => {
            SyncError::ProgressTimeout { failure_stage }
        }
        RoundError::Session(error) => SyncError::SessionLost(error),
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
        SyncError::Unavailable(_)
        | SyncError::SessionLost(_)
        | SyncError::ProgressTimeout { .. } => ErrorCode::Unavailable,
        SyncError::Protocol(_) => ErrorCode::ProtocolMismatch,
        SyncError::Store | SyncError::Internal(_) => ErrorCode::Internal,
    }
}

pub(super) fn sync_error_name(error: &SyncError) -> &'static str {
    if matches!(error, SyncError::ProgressTimeout { .. }) {
        return "round_progress_timeout";
    }
    if matches!(error, SyncError::SessionLost(_)) {
        return "session_lost";
    }
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

pub(super) fn session_failure_fields(
    error: &SessionError,
    direction: &'static str,
    failure_stage: &'static str,
    connect_target: Option<&str>,
) -> serde_json::Value {
    let close_code_name = |code| match code {
        SyncCloseCode::Unspecified => "unspecified",
        SyncCloseCode::Normal => "normal",
        SyncCloseCode::ShuttingDown => "shutting_down",
        SyncCloseCode::ProtocolViolation => "protocol_violation",
        SyncCloseCode::IdentityCollision => "identity_collision",
        SyncCloseCode::SelfConnection => "self_connection",
        SyncCloseCode::DuplicateSession => "duplicate_session",
        SyncCloseCode::ReplicaMismatch => "replica_mismatch",
        SyncCloseCode::ReplicaAvailable => "replica_available",
        SyncCloseCode::BootstrapInProgress => "bootstrap_in_progress",
        SyncCloseCode::NegotiationFailed => "negotiation_failed",
        SyncCloseCode::ResourceExhausted => "resource_exhausted",
        SyncCloseCode::InternalError => "internal_error",
    };
    match error {
        SessionError::ProgressDeadlineExceeded { failure_stage } => json!({
            "direction": direction,
            "failure_stage": failure_stage,
            "failure_source": if failure_stage.contains("inventory_capture") {
                "local_store"
            } else {
                "transport"
            },
            "error_code": "round_progress_timeout",
            "message": error.to_string(),
            "connect_target": connect_target,
        }),
        SessionError::Transport(error) => {
            let (error_code, io_error_kind) = match error {
                TransportError::Io(kind) => ("transport_io", Some(format!("{kind:?}"))),
                TransportError::DeadlineExceeded => ("handshake_deadline_exceeded", None),
                TransportError::InvalidPreface => ("invalid_preface", None),
                TransportError::InvalidFrameLength => ("invalid_frame_length", None),
                TransportError::NoiseHandshake => ("noise_handshake_failed", None),
                TransportError::NoiseTransport => ("noise_transport_authentication_failed", None),
                TransportError::EnvelopeTooLarge => ("envelope_too_large", None),
                TransportError::InvalidEnvelope => ("invalid_protobuf_envelope", None),
            };
            let mut fields = json!({
                "direction": direction,
                "failure_stage": failure_stage,
                "failure_source": "transport",
                "error_code": error_code,
                "message": error.to_string(),
                "connect_target": connect_target,
            });
            if let Some(kind) = io_error_kind {
                fields["io_error_kind"] = kind.into();
            }
            fields
        }
        SessionError::LocalProtocol {
            code,
            error_code,
            message,
        } => json!({
            "direction": direction,
            "failure_stage": failure_stage,
            "failure_source": "local_validation",
            "error_code": error_code,
            "sync_close_code": close_code_name(*code),
            "message": message,
            "connect_target": connect_target,
        }),
        SessionError::RemoteClosed { code, .. } => {
            let code = close_code_name(*code);
            json!({
                "direction": direction,
                "failure_stage": failure_stage,
                "failure_source": "remote_close",
                "error_code": code,
                "sync_close_code": code,
                "connect_target": connect_target,
            })
        }
    }
}
