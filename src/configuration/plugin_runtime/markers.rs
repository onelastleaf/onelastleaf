use mlua::{Lua, Table, Value};

const LIST_MARKERS: &str = "oll.config.list-markers";

pub(in crate::configuration) fn initialize(lua: &Lua) -> mlua::Result<()> {
    let markers = lua.create_table()?;
    let metatable = lua.create_table()?;
    metatable.raw_set("__mode", "k")?;
    markers.set_metatable(Some(metatable))?;
    lua.set_named_registry_value(LIST_MARKERS, markers)
}

pub(super) fn mark_list(lua: &Lua, table: &Table) -> mlua::Result<()> {
    let markers: Table = lua.named_registry_value(LIST_MARKERS)?;
    markers.raw_set(table.clone(), true)
}

pub(super) fn is_marked_list(lua: &Lua, table: &Table) -> mlua::Result<bool> {
    let markers: Table = lua.named_registry_value(LIST_MARKERS)?;
    Ok(matches!(
        markers.raw_get::<Value>(table.clone())?,
        Value::Boolean(true)
    ))
}
