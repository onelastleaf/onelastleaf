use crate::{plugin::PluginError, protocol::oll};

// ConfigValue depth is zero-based. Map values consume the tightest share of
// prost's default decode-recursion budget because each level includes a map
// entry message, so 33 is the common safe wire limit for every value shape.
pub(crate) const MAXIMUM_VALUE_DEPTH: usize = 33;
const PROTOBUF_TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800;
const PROTOBUF_TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799;
const PROTOBUF_DURATION_MAX_SECONDS: i64 = 315_576_000_000;

pub(crate) fn valid_timestamp(value: &prost_types::Timestamp) -> bool {
    (PROTOBUF_TIMESTAMP_MIN_SECONDS..=PROTOBUF_TIMESTAMP_MAX_SECONDS).contains(&value.seconds)
        && (0..=999_999_999).contains(&value.nanos)
}

pub(crate) fn valid_duration(value: &prost_types::Duration) -> bool {
    (-PROTOBUF_DURATION_MAX_SECONDS..=PROTOBUF_DURATION_MAX_SECONDS).contains(&value.seconds)
        && (-999_999_999..=999_999_999).contains(&value.nanos)
        && !(value.seconds > 0 && value.nanos < 0)
        && !(value.seconds < 0 && value.nanos > 0)
}

/// Validates a ConfigValue before it enters durable job state or JSON logs.
/// Session-scoped function handles cannot outlive the stream and are therefore
/// deliberately excluded from both boundaries.
pub(crate) fn validate_serializable_config_value(
    value: &oll::ConfigValue,
) -> Result<(), PluginError> {
    validate(value, 0, None)
}

/// Validates function-call arguments before they enter the serialized Lua
/// owner. A same-session handle is permitted here; the Lua registry lookup
/// remains the authority for whether that handle still exists.
pub(crate) fn validate_config_function_arguments(
    values: &[oll::ConfigValue],
    session_id: &str,
) -> Result<(), PluginError> {
    for value in values {
        validate(value, 0, Some(session_id))?;
    }
    Ok(())
}

fn validate(
    value: &oll::ConfigValue,
    depth: usize,
    function_session: Option<&str>,
) -> Result<(), PluginError> {
    use oll::config_value::Kind;

    if depth > MAXIMUM_VALUE_DEPTH {
        return Err(PluginError::InvalidArgument(
            "ConfigValue nesting exceeds the supported limit".to_owned(),
        ));
    }
    match value.kind.as_ref() {
        Some(Kind::NullValue(value)) if *value == prost_types::NullValue::NullValue as i32 => {
            Ok(())
        }
        Some(Kind::NullValue(_)) => Err(PluginError::InvalidArgument(
            "ConfigValue contains an unknown null value".to_owned(),
        )),
        Some(Kind::BoolValue(_))
        | Some(Kind::IntegerValue(_))
        | Some(Kind::StringValue(_))
        | Some(Kind::BytesValue(_)) => Ok(()),
        Some(Kind::NumberValue(value)) if value.is_finite() => Ok(()),
        Some(Kind::NumberValue(_)) => Err(PluginError::InvalidArgument(
            "ConfigValue numbers must be finite".to_owned(),
        )),
        Some(Kind::ListValue(list)) => {
            for value in &list.values {
                validate(value, depth + 1, function_session)?;
            }
            Ok(())
        }
        Some(Kind::MapValue(map)) => {
            for value in map.entries.values() {
                validate(value, depth + 1, function_session)?;
            }
            Ok(())
        }
        Some(Kind::FunctionValue(function)) => match function_session {
            Some(session_id)
                if function.session_id == session_id && !function.function_id.is_empty() =>
            {
                Ok(())
            }
            Some(session_id) if function.session_id != session_id => {
                Err(PluginError::FailedPrecondition(
                    "configuration function belongs to another plugin session".to_owned(),
                ))
            }
            Some(_) => Err(PluginError::InvalidArgument(
                "configuration function ID must not be empty".to_owned(),
            )),
            None => Err(PluginError::InvalidArgument(
                "session-scoped configuration functions cannot be stored or logged".to_owned(),
            )),
        },
        Some(Kind::TimestampValue(value)) => {
            if valid_timestamp(value) {
                Ok(())
            } else {
                Err(PluginError::InvalidArgument(
                    "ConfigValue timestamp is outside the protobuf Timestamp domain".to_owned(),
                ))
            }
        }
        Some(Kind::DurationValue(value)) => {
            if !valid_duration(value) {
                return Err(PluginError::InvalidArgument(
                    "ConfigValue duration is outside the protobuf Duration domain".to_owned(),
                ));
            }
            Ok(())
        }
        None => Err(PluginError::InvalidArgument(
            "ConfigValue kind is required".to_owned(),
        )),
    }
}
