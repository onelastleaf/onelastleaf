use super::super::ReplicaError;

pub(super) fn validate_sha256(value: &str) -> Result<(), ReplicaError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplicaError::InvalidSnapshot(
            "SHA-256 must be 64 lower-case hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn loro_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::CorruptStore(format!("Loro operation failed: {error}"))
}

pub(super) fn loro_encode_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::Internal(format!("cannot encode Loro snapshot: {error}"))
}

pub(super) fn snapshot_from_store_error(error: ReplicaError) -> ReplicaError {
    ReplicaError::InvalidSnapshot(error.to_string())
}
