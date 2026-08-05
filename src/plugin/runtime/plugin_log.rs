use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use serde_json::{Map, Number, Value, json};

use crate::{
    node::logging::{LogLevel, NodeLogger},
    plugin::{InstalledPlugin, PluginInstanceId, protocol},
    protocol::oll::{self, config_value},
};

use super::{
    host::protocol_error,
    trace::insert_trace_fields,
    value::{MAXIMUM_VALUE_DEPTH, validate_serializable_config_value},
};

pub(super) fn emit_plugin_record(
    logger: &Arc<NodeLogger>,
    plugin: &InstalledPlugin,
    instance_id: PluginInstanceId,
    trace: &oll::TraceContext,
    record: oll::LogRecord,
) -> Result<(), oll::ProtocolError> {
    let timestamp =
        protocol::decode_required_timestamp(record.timestamp.as_ref(), "LogRecord.timestamp")
            .map_err(invalid_log)?;
    let level = oll::LogLevel::try_from(record.level)
        .ok()
        .and_then(LogLevel::from_proto)
        .ok_or_else(|| {
            protocol_error(
                oll::ErrorCode::InvalidArgument,
                "LogRecord.level is unspecified or unknown",
                false,
            )
        })?;
    if record.target.is_empty() {
        return Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "LogRecord.target must not be empty",
            false,
        ));
    }

    let mut fields = Map::new();
    for (key, value) in record.fields {
        validate_serializable_config_value(&value).map_err(invalid_log)?;
        fields.insert(key, config_value_to_json(&value, 0)?);
    }
    fields.insert("message".to_owned(), Value::String(record.message));
    fields.insert(
        "plugin_id".to_owned(),
        Value::String(plugin.plugin_id.to_string()),
    );
    fields.insert(
        "plugin_name".to_owned(),
        Value::String(plugin.plugin_name.to_string()),
    );
    fields.insert(
        "plugin_instance_id".to_owned(),
        Value::String(instance_id.to_string()),
    );
    insert_trace_fields(&mut fields, trace);
    logger.emit_plugin(
        level,
        &record.target,
        "plugin_log_record",
        &trace.correlation_id,
        timestamp,
        Value::Object(fields),
    );
    Ok(())
}

fn config_value_to_json(
    value: &oll::ConfigValue,
    depth: usize,
) -> Result<Value, oll::ProtocolError> {
    if depth > MAXIMUM_VALUE_DEPTH {
        return Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "LogRecord field nesting exceeds the supported limit",
            false,
        ));
    }
    let Some(kind) = value.kind.as_ref() else {
        return Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "LogRecord field ConfigValue kind is required",
            false,
        ));
    };
    match kind {
        config_value::Kind::NullValue(value)
            if *value == prost_types::NullValue::NullValue as i32 =>
        {
            Ok(Value::Null)
        }
        config_value::Kind::NullValue(_) => Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "LogRecord field has an unknown null value",
            false,
        )),
        config_value::Kind::BoolValue(value) => Ok(Value::Bool(*value)),
        config_value::Kind::IntegerValue(value) => Ok(Value::Number((*value).into())),
        config_value::Kind::NumberValue(value) => {
            Number::from_f64(*value).map(Value::Number).ok_or_else(|| {
                protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "LogRecord numeric fields must be finite",
                    false,
                )
            })
        }
        config_value::Kind::StringValue(value) => Ok(Value::String(value.clone())),
        config_value::Kind::BytesValue(value) => Ok(json!({
            "encoding": "base64",
            "value": STANDARD_NO_PAD.encode(value),
        })),
        config_value::Kind::ListValue(list) => list
            .values
            .iter()
            .map(|value| config_value_to_json(value, depth + 1))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        config_value::Kind::MapValue(map) => map
            .entries
            .iter()
            .map(|(key, value)| {
                config_value_to_json(value, depth + 1).map(|value| (key.clone(), value))
            })
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        config_value::Kind::FunctionValue(_) => Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "LogRecord fields cannot contain function handles",
            false,
        )),
        config_value::Kind::TimestampValue(timestamp) => Ok(json!({
            "seconds": timestamp.seconds,
            "nanos": timestamp.nanos,
        })),
        config_value::Kind::DurationValue(duration) => Ok(json!({
            "seconds": duration.seconds,
            "nanos": duration.nanos,
        })),
    }
}

fn invalid_log(error: crate::plugin::PluginError) -> oll::ProtocolError {
    protocol_error(oll::ErrorCode::InvalidArgument, error.to_string(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_list(depth: usize) -> oll::ConfigValue {
        let mut value = oll::ConfigValue {
            kind: Some(config_value::Kind::BoolValue(true)),
        };
        for _ in 0..depth {
            value = oll::ConfigValue {
                kind: Some(config_value::Kind::ListValue(oll::ConfigList {
                    values: vec![value],
                })),
            };
        }
        value
    }

    #[test]
    fn log_conversion_uses_the_common_zero_based_value_depth_limit() {
        let maximum = nested_list(MAXIMUM_VALUE_DEPTH);
        assert!(validate_serializable_config_value(&maximum).is_ok());
        assert!(config_value_to_json(&maximum, 0).is_ok());

        let too_deep = nested_list(MAXIMUM_VALUE_DEPTH + 1);
        assert!(validate_serializable_config_value(&too_deep).is_err());
        let error = config_value_to_json(&too_deep, 0).unwrap_err();
        assert_eq!(error.code, oll::ErrorCode::InvalidArgument as i32);
    }

    #[test]
    fn log_conversion_defensively_rejects_function_handles() {
        let function = oll::ConfigValue {
            kind: Some(config_value::Kind::FunctionValue(oll::ConfigFunctionRef {
                session_id: "session".to_owned(),
                function_id: "function".to_owned(),
            })),
        };

        let error = config_value_to_json(&function, 0).unwrap_err();
        assert_eq!(error.code, oll::ErrorCode::InvalidArgument as i32);
        assert_eq!(
            error.message,
            "LogRecord fields cannot contain function handles"
        );
    }
}
