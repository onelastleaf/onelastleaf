use uuid::Uuid;

use super::super::{ReplicaError, types::parse_uuid_v4};

pub(super) fn parse_optional_uuid(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<Uuid>, ReplicaError> {
    value.map(|value| parse_uuid_v4(&value, field)).transpose()
}

pub(super) fn revision_array(
    bytes: Vec<u8>,
    field: &'static str,
) -> Result<[u8; 32], ReplicaError> {
    bytes
        .try_into()
        .map_err(|_| ReplicaError::CorruptStore(format!("{field} is not 32 bytes")))
}

pub(super) fn parse_bool(value: i64, field: &'static str) -> Result<bool, ReplicaError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ReplicaError::CorruptStore(format!(
            "{field} is not boolean"
        ))),
    }
}

pub(super) fn parse_u64(value: &str, field: &'static str) -> Result<u64, ReplicaError> {
    value
        .parse()
        .map_err(|_| ReplicaError::CorruptStore(format!("{field} is not a u64")))
}

pub(super) fn validate_blob_hash(value: &str) -> Result<(), ReplicaError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ReplicaError::CorruptStore(
            "blob content address is not lower-case SHA-256".to_owned(),
        ))
    }
}

pub(super) fn kind_fields_error() -> ReplicaError {
    ReplicaError::CorruptStore("catalog entry fields do not match its kind".to_owned())
}

pub(super) fn store_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::Store(format!("replica-store operation failed: {error}"))
}
