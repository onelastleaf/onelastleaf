use std::collections::{HashMap, HashSet};

use mlua::{Function, Lua, RegistryKey, UserData, UserDataFields, Value};

use crate::plugin::runtime::{MAXIMUM_VALUE_DEPTH, valid_duration, valid_timestamp};
use crate::protocol::oll::{ConfigFunctionRef, ConfigList, ConfigMap, ConfigValue, config_value};

use super::{PluginConfigError, PluginSession, markers, path::TableShape, path::table_shape};

struct PendingFunctions {
    next_function_id: u64,
    entries: Vec<(String, RegistryKey)>,
}

pub(super) fn convert_lua_output(
    lua: &Lua,
    session_id: &str,
    session: &mut PluginSession,
    value: Value,
) -> Result<ConfigValue, PluginConfigError> {
    let mut values = convert_lua_outputs(lua, session_id, session, std::iter::once(value))?;
    Ok(values.remove(0))
}

pub(super) fn convert_lua_outputs(
    lua: &Lua,
    session_id: &str,
    session: &mut PluginSession,
    values: impl IntoIterator<Item = Value>,
) -> Result<Vec<ConfigValue>, PluginConfigError> {
    let mut pending = PendingFunctions {
        next_function_id: session.next_function_id,
        entries: Vec::new(),
    };
    let mut active_tables = HashSet::new();
    let result = values
        .into_iter()
        .map(|value| lua_to_config(lua, session_id, value, 0, &mut active_tables, &mut pending))
        .collect::<Result<Vec<_>, _>>();
    match result {
        Ok(values) => {
            session.next_function_id = pending.next_function_id;
            session.functions.extend(pending.entries);
            Ok(values)
        }
        Err(error) => {
            for (_, key) in pending.entries {
                let _ = lua.remove_registry_value(key);
            }
            Err(error)
        }
    }
}

fn lua_to_config(
    lua: &Lua,
    session_id: &str,
    value: Value,
    depth: usize,
    active_tables: &mut HashSet<usize>,
    pending: &mut PendingFunctions,
) -> Result<ConfigValue, PluginConfigError> {
    if depth > MAXIMUM_VALUE_DEPTH {
        return Err(PluginConfigError::UnsupportedValue(
            "nesting beyond the supported limit",
        ));
    }
    let kind = match value {
        Value::Nil => config_value::Kind::NullValue(prost_types::NullValue::NullValue as i32),
        Value::Boolean(value) => config_value::Kind::BoolValue(value),
        Value::Integer(value) => config_value::Kind::IntegerValue(value),
        Value::Number(value) if value.is_finite() => config_value::Kind::NumberValue(value),
        Value::Number(_) => return Err(PluginConfigError::UnsupportedValue("non-finite number")),
        Value::String(value) => match value.to_str() {
            Ok(value) => config_value::Kind::StringValue(value.to_owned()),
            Err(_) => config_value::Kind::BytesValue(value.as_bytes().to_vec()),
        },
        Value::LightUserData(value) if value.0.is_null() => {
            config_value::Kind::NullValue(prost_types::NullValue::NullValue as i32)
        }
        Value::Table(table) => {
            let pointer = table.to_pointer() as usize;
            if !active_tables.insert(pointer) {
                return Err(PluginConfigError::CyclicValue);
            }
            let converted = match table_shape(lua, &table)? {
                TableShape::List(length) => {
                    let mut values = Vec::with_capacity(length);
                    for index in 1..=length {
                        let value = table
                            .raw_get::<Value>(index)
                            .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
                        values.push(lua_to_config(
                            lua,
                            session_id,
                            value,
                            depth + 1,
                            active_tables,
                            pending,
                        )?);
                    }
                    config_value::Kind::ListValue(ConfigList { values })
                }
                TableShape::Map => {
                    let mut entries = HashMap::new();
                    for pair in table.pairs::<Value, Value>() {
                        let (key, value) =
                            pair.map_err(|_| PluginConfigError::RuntimeUnavailable)?;
                        let Value::String(key) = key else {
                            return Err(PluginConfigError::UnsupportedValue("map key type"));
                        };
                        let key = key
                            .to_str()
                            .map_err(|_| PluginConfigError::UnsupportedValue("non-UTF-8 map key"))?
                            .to_owned();
                        entries.insert(
                            key,
                            lua_to_config(
                                lua,
                                session_id,
                                value,
                                depth + 1,
                                active_tables,
                                pending,
                            )?,
                        );
                    }
                    config_value::Kind::MapValue(ConfigMap { entries })
                }
            };
            active_tables.remove(&pointer);
            converted
        }
        Value::Function(function) => {
            let function_id = pending.next_function_id.to_string();
            pending.next_function_id = pending
                .next_function_id
                .checked_add(1)
                .ok_or(PluginConfigError::RuntimeUnavailable)?;
            let key = lua
                .create_registry_value(function)
                .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            pending.entries.push((function_id.clone(), key));
            config_value::Kind::FunctionValue(ConfigFunctionRef {
                session_id: session_id.to_owned(),
                function_id,
            })
        }
        Value::UserData(value) if value.is::<LuaTimestamp>() => {
            let value = value
                .borrow::<LuaTimestamp>()
                .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            if !valid_timestamp(&value.0) {
                return Err(PluginConfigError::UnsupportedValue(
                    "timestamp outside the protobuf domain",
                ));
            }
            config_value::Kind::TimestampValue(value.0)
        }
        Value::UserData(value) if value.is::<LuaDuration>() => {
            let value = value
                .borrow::<LuaDuration>()
                .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            if !valid_duration(&value.0) {
                return Err(PluginConfigError::UnsupportedValue(
                    "duration outside the protobuf domain",
                ));
            }
            config_value::Kind::DurationValue(value.0)
        }
        unsupported => return Err(PluginConfigError::UnsupportedValue(unsupported.type_name())),
    };
    Ok(ConfigValue { kind: Some(kind) })
}

pub(super) fn config_to_lua(
    lua: &Lua,
    session_id: &str,
    session: &PluginSession,
    value: &ConfigValue,
    depth: usize,
) -> Result<Value, PluginConfigError> {
    if depth > MAXIMUM_VALUE_DEPTH {
        return Err(PluginConfigError::UnsupportedValue(
            "nesting beyond the supported limit",
        ));
    }
    match value.kind.as_ref() {
        Some(config_value::Kind::NullValue(value))
            if *value == prost_types::NullValue::NullValue as i32 =>
        {
            Ok(Value::NULL)
        }
        Some(config_value::Kind::NullValue(_)) => {
            Err(PluginConfigError::UnsupportedValue("unknown null value"))
        }
        Some(config_value::Kind::BoolValue(value)) => Ok(Value::Boolean(*value)),
        Some(config_value::Kind::IntegerValue(value)) => Ok(Value::Integer(*value)),
        Some(config_value::Kind::NumberValue(value)) if value.is_finite() => {
            Ok(Value::Number(*value))
        }
        Some(config_value::Kind::NumberValue(_)) => {
            Err(PluginConfigError::UnsupportedValue("non-finite number"))
        }
        Some(config_value::Kind::StringValue(value)) => lua
            .create_string(value)
            .map(Value::String)
            .map_err(|_| PluginConfigError::RuntimeUnavailable),
        Some(config_value::Kind::BytesValue(value)) => lua
            .create_string(value)
            .map(Value::String)
            .map_err(|_| PluginConfigError::RuntimeUnavailable),
        Some(config_value::Kind::ListValue(value)) => {
            let table = lua
                .create_table_with_capacity(value.values.len(), 0)
                .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            markers::mark_list(lua, &table).map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            for (index, value) in value.values.iter().enumerate() {
                table
                    .raw_set(
                        index + 1,
                        config_to_lua(lua, session_id, session, value, depth + 1)?,
                    )
                    .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            }
            Ok(Value::Table(table))
        }
        Some(config_value::Kind::MapValue(value)) => {
            let table = lua
                .create_table_with_capacity(0, value.entries.len())
                .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            for (key, value) in &value.entries {
                table
                    .raw_set(
                        key.as_str(),
                        config_to_lua(lua, session_id, session, value, depth + 1)?,
                    )
                    .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
            }
            Ok(Value::Table(table))
        }
        Some(config_value::Kind::FunctionValue(value)) => {
            if value.session_id != session_id {
                return Err(PluginConfigError::FunctionSessionMismatch);
            }
            let key = session
                .functions
                .get(&value.function_id)
                .ok_or(PluginConfigError::FunctionNotFound)?;
            lua.registry_value::<Function>(key)
                .map(Value::Function)
                .map_err(|_| PluginConfigError::RuntimeUnavailable)
        }
        Some(config_value::Kind::TimestampValue(value)) if valid_timestamp(value) => lua
            .create_userdata(LuaTimestamp(*value))
            .map(Value::UserData)
            .map_err(|_| PluginConfigError::RuntimeUnavailable),
        Some(config_value::Kind::TimestampValue(_)) => Err(PluginConfigError::UnsupportedValue(
            "timestamp outside the protobuf domain",
        )),
        Some(config_value::Kind::DurationValue(value)) if valid_duration(value) => lua
            .create_userdata(LuaDuration(*value))
            .map(Value::UserData)
            .map_err(|_| PluginConfigError::RuntimeUnavailable),
        Some(config_value::Kind::DurationValue(_)) => Err(PluginConfigError::UnsupportedValue(
            "duration outside the protobuf domain",
        )),
        None => Err(PluginConfigError::UnsupportedValue(
            "ConfigValue without a kind",
        )),
    }
}

#[derive(Clone)]
struct LuaTimestamp(prost_types::Timestamp);

impl UserData for LuaTimestamp {
    fn add_fields<F: UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("seconds", |_, value| Ok(value.0.seconds));
        fields.add_field_method_get("nanos", |_, value| Ok(value.0.nanos));
    }
}

#[derive(Clone)]
struct LuaDuration(prost_types::Duration);

impl UserData for LuaDuration {
    fn add_fields<F: UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("seconds", |_, value| Ok(value.0.seconds));
        fields.add_field_method_get("nanos", |_, value| Ok(value.0.nanos));
    }
}
