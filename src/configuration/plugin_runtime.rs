mod error;
pub(super) mod markers;
mod path;
mod value;

use std::{collections::HashMap, fs, io};

use mlua::{Function, MultiValue, RegistryKey, chunk::ChunkMode};

use crate::{
    plugin::PluginId,
    protocol::oll::{ConfigFunctionRef, ConfigPath, ConfigValue},
};

use super::runtime::{ConfigRuntime, RuntimeState};
pub use error::{PluginConfigError, PluginConfigErrorKind};
use path::select_path;
use value::{config_to_lua, convert_lua_output, convert_lua_outputs};

const PLUGIN_DIRECTORY: &str = "plugins";

pub(super) struct PluginSession {
    plugin_id: PluginId,
    functions: HashMap<String, RegistryKey>,
    next_function_id: u64,
}

impl ConfigRuntime {
    pub fn begin_plugin_session(
        &self,
        session_id: &str,
        plugin_id: &str,
    ) -> Result<(), PluginConfigError> {
        if session_id.is_empty() {
            return Err(PluginConfigError::InvalidSessionId);
        }
        let plugin_id = plugin_id
            .parse::<PluginId>()
            .map_err(|_| PluginConfigError::InvalidPluginId)?;

        let mut runtime = self
            .inner
            .lock()
            .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
        if runtime.plugin_sessions.contains_key(session_id) {
            return Err(PluginConfigError::SessionAlreadyExists);
        }
        runtime.plugin_sessions.insert(
            session_id.to_owned(),
            PluginSession {
                plugin_id,
                functions: HashMap::new(),
                next_function_id: 1,
            },
        );
        Ok(())
    }

    pub fn get_plugin_config(
        &self,
        session_id: &str,
        path: &ConfigPath,
    ) -> Result<ConfigValue, PluginConfigError> {
        let mut runtime = self
            .inner
            .lock()
            .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
        let plugin_id = runtime
            .plugin_sessions
            .get(session_id)
            .ok_or(PluginConfigError::SessionNotActive)?
            .plugin_id
            .clone();
        let configured_path = runtime
            .canonical_root
            .join(PLUGIN_DIRECTORY)
            .join(format!("{plugin_id}.lua"));
        let canonical_path = match fs::canonicalize(&configured_path) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(PluginConfigError::ConfigNotFound {
                    path: configured_path,
                });
            }
            Err(error) => {
                return Err(PluginConfigError::Read {
                    path: configured_path,
                    kind: error.kind(),
                });
            }
        };
        if !canonical_path.starts_with(&runtime.canonical_root) {
            return Err(PluginConfigError::InvalidPath(
                "plugin configuration escapes the config root",
            ));
        }
        let metadata = fs::metadata(&canonical_path).map_err(|error| PluginConfigError::Read {
            path: canonical_path.clone(),
            kind: error.kind(),
        })?;
        if !metadata.is_file() {
            return Err(PluginConfigError::InvalidPath(
                "plugin configuration must be a regular file",
            ));
        }
        let bytes = fs::read(&canonical_path).map_err(|error| PluginConfigError::Read {
            path: canonical_path.clone(),
            kind: error.kind(),
        })?;
        let source = String::from_utf8(bytes).map_err(|_| PluginConfigError::InvalidUtf8 {
            path: canonical_path.clone(),
        })?;
        let values: MultiValue = runtime
            .lua
            .load(source)
            .set_name(format!("@{}", canonical_path.to_string_lossy()))
            .set_mode(ChunkMode::Text)
            .eval()
            .map_err(|_| PluginConfigError::Evaluation {
                path: canonical_path.clone(),
            })?;
        if values.len() != 1 {
            return Err(PluginConfigError::InvalidPath(
                "plugin module must return exactly one value",
            ));
        }
        let value = values
            .into_iter()
            .next()
            .expect("one checked Lua return value");
        let value = select_path(&runtime.lua, value, path)?;

        let RuntimeState {
            lua,
            plugin_sessions,
            ..
        } = &mut *runtime;
        let session = plugin_sessions
            .get_mut(session_id)
            .ok_or(PluginConfigError::SessionNotActive)?;
        convert_lua_output(lua, session_id, session, value)
    }

    pub fn invoke_plugin_config_function(
        &self,
        session_id: &str,
        function_ref: &ConfigFunctionRef,
        arguments: &[ConfigValue],
    ) -> Result<Vec<ConfigValue>, PluginConfigError> {
        if function_ref.session_id != session_id {
            return Err(PluginConfigError::FunctionSessionMismatch);
        }
        if function_ref.function_id.is_empty() {
            return Err(PluginConfigError::FunctionNotFound);
        }

        let mut runtime = self
            .inner
            .lock()
            .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
        let RuntimeState {
            lua,
            plugin_sessions,
            ..
        } = &mut *runtime;
        let session = plugin_sessions
            .get_mut(session_id)
            .ok_or(PluginConfigError::SessionNotActive)?;
        let function = session
            .functions
            .get(&function_ref.function_id)
            .ok_or(PluginConfigError::FunctionNotFound)
            .and_then(|key| {
                lua.registry_value::<Function>(key)
                    .map_err(|_| PluginConfigError::RuntimeUnavailable)
            })?;
        let lua_arguments = arguments
            .iter()
            .map(|value| config_to_lua(lua, session_id, session, value, 0))
            .collect::<Result<Vec<_>, _>>()?;
        let results = function
            .call::<MultiValue>(MultiValue::from_vec(lua_arguments))
            .map_err(|_| PluginConfigError::Invocation)?;
        convert_lua_outputs(lua, session_id, session, results)
    }

    pub fn end_plugin_session(&self, session_id: &str) -> Result<(), PluginConfigError> {
        let mut runtime = self
            .inner
            .lock()
            .map_err(|_| PluginConfigError::RuntimeUnavailable)?;
        let Some(session) = runtime.plugin_sessions.remove(session_id) else {
            return Ok(());
        };
        let mut cleanup_failed = false;
        for key in session.functions.into_values() {
            if runtime.lua.remove_registry_value(key).is_err() {
                cleanup_failed = true;
            }
        }
        if cleanup_failed {
            return Err(PluginConfigError::RuntimeUnavailable);
        }
        Ok(())
    }
}
