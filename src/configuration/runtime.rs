use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    fmt, fs,
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use mlua::{Function, Lua, LuaOptions, MultiValue, StdLib, Value, chunk::ChunkMode};

use super::{
    CONFIG_FILENAME, ConfigError, ROOT_REGISTRY_KEY, ResolvedNodeConfig,
    plugin_runtime::{PluginSession, markers},
    schema::decode_root,
};

type EnvironmentLookup = Arc<dyn Fn(&str) -> Result<Option<String>, ()> + Send + Sync>;

pub(super) struct RuntimeState {
    pub(super) lua: Lua,
    pub(super) canonical_root: PathBuf,
    pub(super) plugin_sessions: HashMap<String, PluginSession>,
}

#[cfg(test)]
impl std::ops::Deref for RuntimeState {
    type Target = Lua;

    fn deref(&self) -> &Self::Target {
        &self.lua
    }
}

#[derive(Clone)]
pub struct ConfigRuntime {
    pub(super) inner: Arc<Mutex<RuntimeState>>,
}

impl fmt::Debug for ConfigRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConfigRuntime { .. }")
    }
}

impl ConfigRuntime {
    pub fn load(config_root: &Path) -> Result<(Self, ResolvedNodeConfig), ConfigError> {
        let lookup: EnvironmentLookup = Arc::new(|name| match env::var_os(name) {
            None => Ok(None),
            Some(value) => value.into_string().map(Some).map_err(|_| ()),
        });
        Self::load_with_environment(config_root, lookup)
    }

    pub(super) fn load_with_environment(
        config_root: &Path,
        environment: EnvironmentLookup,
    ) -> Result<(Self, ResolvedNodeConfig), ConfigError> {
        let config_path = config_root.join(CONFIG_FILENAME);
        let source = read_utf8(&config_path)?;
        let canonical_root = fs::canonicalize(config_root).map_err(|error| ConfigError::Read {
            path: config_root.to_owned(),
            kind: error.kind(),
        })?;

        let libraries = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::BIT | StdLib::JIT;
        let lua = Lua::new_with(libraries, LuaOptions::default())
            .map_err(|_| ConfigError::RuntimeInitialization)?;
        install_environment_helper(&lua, environment)
            .map_err(|_| ConfigError::RuntimeInitialization)?;
        install_module_loader(&lua, canonical_root.clone())
            .map_err(|_| ConfigError::RuntimeInitialization)?;
        markers::initialize(&lua).map_err(|_| ConfigError::RuntimeInitialization)?;

        let values: MultiValue = lua
            .load(source)
            .set_name(format!("@{}", config_path.to_string_lossy()))
            .set_mode(ChunkMode::Text)
            .eval()
            .map_err(|_| ConfigError::Evaluation {
                path: config_path.clone(),
            })?;

        if values.len() != 1 {
            return Err(ConfigError::Schema {
                path: config_path,
                field: "return value",
                problem: "must contain exactly one value",
            });
        }
        let Some(Value::Table(root)) = values.into_iter().next() else {
            return Err(ConfigError::Schema {
                path: config_path,
                field: "return value",
                problem: "must be a table",
            });
        };

        let node = decode_root(&root, config_root, &config_path)?;
        lua.set_named_registry_value(ROOT_REGISTRY_KEY, root)
            .map_err(|_| ConfigError::RuntimeInitialization)?;

        Ok((
            Self {
                inner: Arc::new(Mutex::new(RuntimeState {
                    lua,
                    canonical_root,
                    plugin_sessions: HashMap::new(),
                })),
            },
            node,
        ))
    }

    #[cfg(test)]
    pub(super) fn lua(&self) -> std::sync::MutexGuard<'_, RuntimeState> {
        self.inner
            .lock()
            .expect("configuration runtime lock poisoned")
    }
}

fn install_environment_helper(lua: &Lua, environment: EnvironmentLookup) -> mlua::Result<()> {
    let getenv = lua.create_function(move |_, name: String| {
        if name.is_empty() || name.bytes().any(|byte| byte == b'=' || byte == 0) {
            return Err(mlua::Error::runtime("invalid environment variable name"));
        }
        environment(&name)
            .map_err(|()| mlua::Error::runtime("environment value is not valid UTF-8"))
    })?;
    let read_network_key = lua.create_function(|lua, path: mlua::LuaString| {
        let bytes = path.as_bytes();
        if bytes.is_empty() || bytes.contains(&0) {
            return Err(mlua::Error::runtime(
                "network-key path must be nonempty and NUL-free",
            ));
        }
        let path = PathBuf::from(OsString::from_vec(bytes.to_vec()));
        if !path.is_absolute() {
            return Err(mlua::Error::runtime("network-key path must be absolute"));
        }
        let metadata = fs::metadata(&path)
            .map_err(|_| mlua::Error::runtime("network-key file cannot be read"))?;
        if !metadata.is_file() {
            return Err(mlua::Error::runtime(
                "network-key path must name a regular file",
            ));
        }
        let bytes =
            fs::read(path).map_err(|_| mlua::Error::runtime("network-key file cannot be read"))?;
        lua.create_string(&bytes)
    })?;
    lua.globals().set("_oll_getenv", getenv)?;
    lua.globals()
        .set("_oll_read_network_key", read_network_key)?;
    lua.load(
        r#"
        local helpers = {
            getenv = _oll_getenv,
            read_network_key = _oll_read_network_key,
        }
        _oll_getenv = nil
        _oll_read_network_key = nil
        oll = setmetatable({}, {
            __index = helpers,
            __newindex = function() error("oll helpers are read-only", 2) end,
            __metatable = false,
        })
        "#,
    )
    .set_name("=oll environment helper")
    .set_mode(ChunkMode::Text)
    .exec()
}

fn install_module_loader(lua: &Lua, canonical_root: PathBuf) -> mlua::Result<()> {
    let load_module = lua.create_function(move |lua, name: String| -> mlua::Result<Function> {
        let relative = module_path(&name)
            .ok_or_else(|| mlua::Error::runtime("invalid configuration module name"))?;
        let requested = canonical_root.join(relative);
        let canonical = fs::canonicalize(&requested)
            .map_err(|_| mlua::Error::runtime("configuration module cannot be read"))?;
        if !canonical.starts_with(&canonical_root) {
            return Err(mlua::Error::runtime(
                "configuration module escapes the config root",
            ));
        }
        let source = read_utf8_for_lua(&canonical)?;
        lua.load(source)
            .set_name(format!("@{}", canonical.to_string_lossy()))
            .set_mode(ChunkMode::Text)
            .into_function()
    })?;
    lua.globals().set("_oll_load_module", load_module)?;
    lua.load(
        r#"
        local load_module = _oll_load_module
        _oll_load_module = nil
        local loaded = {}
        local loading = {}

        function require(name)
            if type(name) ~= "string" then
                error("configuration module name must be a string", 2)
            end
            if loaded[name] ~= nil then
                return loaded[name]
            end
            if loading[name] then
                error("cyclic configuration module dependency", 2)
            end

            loading[name] = true
            local chunk = load_module(name)
            local ok, result = pcall(chunk)
            loading[name] = nil
            if not ok then
                error(result, 2)
            end
            if result == nil then
                result = true
            end
            loaded[name] = result
            return result
        end
        "#,
    )
    .set_name("=oll module loader")
    .set_mode(ChunkMode::Text)
    .exec()?;

    lua.globals().set("dofile", Value::Nil)?;
    lua.globals().set("loadfile", Value::Nil)?;
    Ok(())
}

fn module_path(name: &str) -> Option<PathBuf> {
    let mut path = PathBuf::new();
    for segment in name.split('.') {
        if segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return None;
        }
        path.push(segment);
    }
    path.set_extension("lua");
    Some(path)
}

fn read_utf8(path: &Path) -> Result<String, ConfigError> {
    let bytes = fs::read(path).map_err(|error| ConfigError::Read {
        path: path.to_owned(),
        kind: error.kind(),
    })?;
    String::from_utf8(bytes).map_err(|_| ConfigError::InvalidUtf8 {
        path: path.to_owned(),
    })
}

fn read_utf8_for_lua(path: &Path) -> mlua::Result<String> {
    let bytes =
        fs::read(path).map_err(|_| mlua::Error::runtime("configuration module cannot be read"))?;
    String::from_utf8(bytes)
        .map_err(|_| mlua::Error::runtime("configuration module is not valid UTF-8"))
}
