use mlua::{Lua, Table, Value};

use crate::protocol::oll::{ConfigPath, config_path_segment};

use super::{PluginConfigError, markers};

pub(super) enum TableShape {
    List(usize),
    Map,
}

pub(super) fn table_shape(lua: &Lua, table: &Table) -> Result<TableShape, PluginConfigError> {
    let mut integer_keys = Vec::new();
    let mut saw_string = false;
    for pair in table.clone().pairs::<Value, Value>() {
        let (key, _) = pair.map_err(|_| PluginConfigError::RuntimeUnavailable)?;
        match key {
            Value::Integer(index) if index > 0 && !saw_string => integer_keys.push(index as u64),
            Value::String(key) if integer_keys.is_empty() => {
                key.to_str()
                    .map_err(|_| PluginConfigError::UnsupportedValue("non-UTF-8 map key"))?;
                saw_string = true;
            }
            Value::Integer(_) | Value::String(_) => {
                return Err(PluginConfigError::UnsupportedValue(
                    "mixed or non-positive table key",
                ));
            }
            _ => return Err(PluginConfigError::UnsupportedValue("table key type")),
        }
    }
    if integer_keys.is_empty()
        && !saw_string
        && markers::is_marked_list(lua, table).map_err(|_| PluginConfigError::RuntimeUnavailable)?
    {
        return Ok(TableShape::List(0));
    }
    if saw_string || integer_keys.is_empty() {
        return Ok(TableShape::Map);
    }
    integer_keys.sort_unstable();
    for (offset, key) in integer_keys.iter().enumerate() {
        if *key != u64::try_from(offset + 1).unwrap_or(u64::MAX) {
            return Err(PluginConfigError::UnsupportedValue("sparse list table"));
        }
    }
    Ok(TableShape::List(integer_keys.len()))
}

pub(super) fn select_path(
    lua: &Lua,
    mut value: Value,
    path: &ConfigPath,
) -> Result<Value, PluginConfigError> {
    for segment in &path.segments {
        let Value::Table(table) = value else {
            return Err(PluginConfigError::InvalidPath(
                "segment applied to a non-container value",
            ));
        };
        value = match segment.kind.as_ref() {
            Some(config_path_segment::Kind::Key(key)) => {
                if !matches!(table_shape(lua, &table)?, TableShape::Map) {
                    return Err(PluginConfigError::InvalidPath("map key applied to a list"));
                }
                let selected = table
                    .raw_get::<Value>(key.as_str())
                    .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
                if selected.is_nil() {
                    return Err(PluginConfigError::ValueNotFound);
                }
                selected
            }
            Some(config_path_segment::Kind::Index(index)) => {
                let TableShape::List(length) = table_shape(lua, &table)? else {
                    return Err(PluginConfigError::InvalidPath(
                        "list index applied to a map",
                    ));
                };
                let index = usize::try_from(*index)
                    .ok()
                    .filter(|index| *index < length)
                    .ok_or(PluginConfigError::ValueNotFound)?;
                table
                    .raw_get::<Value>(index + 1)
                    .map_err(|_| PluginConfigError::RuntimeUnavailable)?
            }
            None => {
                return Err(PluginConfigError::InvalidPath(
                    "path segment kind is missing",
                ));
            }
        };
    }
    Ok(value)
}
