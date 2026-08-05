use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

use time::OffsetDateTime;
use uuid::Uuid;

use super::super::PluginError;

pub(super) fn parse_uuid(value: &str, field: &'static str) -> Result<Uuid, PluginError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| PluginError::CorruptStore(format!("{field} is not a UUID v4")))?;
    if parsed.get_version_num() != 4 || parsed.to_string() != value {
        return Err(PluginError::CorruptStore(format!(
            "{field} is not a canonical UUID v4"
        )));
    }
    Ok(parsed)
}

pub(super) fn hash_array(bytes: Vec<u8>, field: &'static str) -> Result<[u8; 32], PluginError> {
    bytes
        .try_into()
        .map_err(|_| PluginError::CorruptStore(format!("{field} is not 32 bytes")))
}

pub(super) fn timestamp_parts(value: OffsetDateTime) -> (i64, i64) {
    (value.unix_timestamp(), i64::from(value.nanosecond()))
}

pub(super) fn parse_timestamp(
    seconds: i64,
    nanos: i64,
    field: &'static str,
) -> Result<OffsetDateTime, PluginError> {
    let nanos = u32::try_from(nanos)
        .ok()
        .filter(|value| *value < 1_000_000_000)
        .ok_or_else(|| PluginError::CorruptStore(format!("{field} has invalid nanoseconds")))?;
    OffsetDateTime::from_unix_timestamp(seconds)
        .and_then(|value| value.replace_nanosecond(nanos))
        .map_err(|_| PluginError::CorruptStore(format!("{field} is out of range")))
}

pub(super) fn optional_timestamp(
    seconds: Option<i64>,
    nanos: Option<i64>,
    field: &'static str,
) -> Result<Option<OffsetDateTime>, PluginError> {
    match (seconds, nanos) {
        (None, None) => Ok(None),
        (Some(seconds), Some(nanos)) => parse_timestamp(seconds, nanos, field).map(Some),
        _ => Err(PluginError::CorruptStore(format!("{field} is incomplete"))),
    }
}

pub(super) fn path_bytes(path: &std::path::Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    path.as_os_str().as_bytes().to_vec()
}

pub(super) fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, PluginError> {
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(PluginError::CorruptStore(
            "stored plugin path is empty or contains NUL".to_owned(),
        ));
    }
    let path = PathBuf::from(OsString::from_vec(bytes));
    if !path.is_absolute() {
        return Err(PluginError::CorruptStore(
            "stored plugin path is not absolute".to_owned(),
        ));
    }
    Ok(path)
}

pub(super) fn encode_arguments(arguments: &[String]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(
        &u64::try_from(arguments.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for argument in arguments {
        output.extend_from_slice(
            &u64::try_from(argument.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        output.extend_from_slice(argument.as_bytes());
    }
    output
}

pub(super) fn decode_arguments(bytes: &[u8]) -> Result<Vec<String>, PluginError> {
    fn read_u64(bytes: &[u8], offset: &mut usize) -> Option<u64> {
        let end = offset.checked_add(8)?;
        let value = u64::from_be_bytes(bytes.get(*offset..end)?.try_into().ok()?);
        *offset = end;
        Some(value)
    }

    let mut offset = 0;
    let count = read_u64(bytes, &mut offset)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| PluginError::CorruptStore("job arguments are malformed".to_owned()))?;
    let mut arguments = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let length = read_u64(bytes, &mut offset)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| PluginError::CorruptStore("job arguments are malformed".to_owned()))?;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| PluginError::CorruptStore("job arguments are malformed".to_owned()))?;
        let value = std::str::from_utf8(&bytes[offset..end])
            .map_err(|_| PluginError::CorruptStore("job argument is not UTF-8".to_owned()))?;
        arguments.push(value.to_owned());
        offset = end;
    }
    if offset != bytes.len() {
        return Err(PluginError::CorruptStore(
            "job arguments have trailing bytes".to_owned(),
        ));
    }
    Ok(arguments)
}

pub(super) fn u64_text(value: u64) -> String {
    value.to_string()
}

pub(super) fn parse_u64_text(value: &str, field: &'static str) -> Result<u64, PluginError> {
    value
        .parse()
        .map_err(|_| PluginError::CorruptStore(format!("{field} is not a u64")))
}
