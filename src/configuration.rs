use std::{
    env, fmt, fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use mlua::{Function, Lua, LuaOptions, MultiValue, StdLib, Table, Value, chunk::ChunkMode};
use url::Url;

const CONFIG_FILENAME: &str = "config.lua";
const ROOT_REGISTRY_KEY: &str = "oll.config.root";

type EnvironmentLookup = Arc<dyn Fn(&str) -> Result<Option<String>, ()> + Send + Sync>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectUrl(Url);

impl ConnectUrl {
    pub fn as_url(&self) -> &Url {
        &self.0
    }
}

impl fmt::Display for ConnectUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConnectUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(input).map_err(|error| error.to_string())?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("connect URL scheme must be http or https".to_owned());
        }
        if url.host().is_none() {
            return Err("connect URL must include a host".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PostgresUrl(Url);

impl PostgresUrl {
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for PostgresUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgresUrl(REDACTED)")
    }
}

impl FromStr for PostgresUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(input).map_err(|error| error.to_string())?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err("replica store URL scheme must be postgres or postgresql".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaStoreConfig {
    Sqlite { path: PathBuf },
    Postgres { url: PostgresUrl },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedNodeConfig {
    pub replica_root: PathBuf,
    pub replica_store: ReplicaStoreConfig,
    pub log_dir: PathBuf,
    pub listen: Option<SocketAddr>,
    pub connect: Vec<ConnectUrl>,
}

pub struct ConfigRuntime {
    _lua: Lua,
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

    fn load_with_environment(
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
        install_module_loader(&lua, canonical_root)
            .map_err(|_| ConfigError::RuntimeInitialization)?;

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

        Ok((Self { _lua: lua }, node))
    }

    #[cfg(test)]
    fn lua(&self) -> &Lua {
        &self._lua
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
    lua.globals().set("_oll_getenv", getenv)?;
    lua.load(
        r#"
        local helpers = { getenv = _oll_getenv }
        _oll_getenv = nil
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

fn decode_root(
    root: &Table,
    config_root: &Path,
    config_path: &Path,
) -> Result<ResolvedNodeConfig, ConfigError> {
    ensure_plain_table(root, config_path, "return value")?;
    ensure_fields(
        root,
        &["format_version", "node"],
        config_path,
        "return value",
    )?;

    match raw_value(root, "format_version", config_path, "format_version")? {
        Value::Integer(1) => {}
        Value::Integer(_) => {
            return Err(schema_error(
                config_path,
                "format_version",
                "is not a supported version",
            ));
        }
        _ => {
            return Err(schema_error(
                config_path,
                "format_version",
                "must be integer 1",
            ));
        }
    }

    let node = match raw_value(root, "node", config_path, "node")? {
        Value::Table(table) => table,
        _ => return Err(schema_error(config_path, "node", "must be a table")),
    };
    ensure_plain_table(&node, config_path, "node")?;
    ensure_fields(
        &node,
        &[
            "replica_root",
            "replica_store",
            "log_dir",
            "listen",
            "connect",
        ],
        config_path,
        "node",
    )?;

    let replica_root = required_path(
        &node,
        "replica_root",
        "node.replica_root",
        config_root,
        config_path,
    )?;
    let replica_store = replica_store(&node, config_root, config_path)?;
    let log_dir = required_path(&node, "log_dir", "node.log_dir", config_root, config_path)?;
    let listen = optional_listen(&node, config_path)?;
    let connect = connect_urls(&node, config_path)?;

    Ok(ResolvedNodeConfig {
        replica_root,
        replica_store,
        log_dir,
        listen,
        connect,
    })
}

fn ensure_plain_table(
    table: &Table,
    config_path: &Path,
    field: &'static str,
) -> Result<(), ConfigError> {
    if table.metatable().is_some() {
        return Err(schema_error(
            config_path,
            field,
            "must not have a metatable",
        ));
    }
    Ok(())
}

fn ensure_fields(
    table: &Table,
    allowed: &[&str],
    config_path: &Path,
    field: &'static str,
) -> Result<(), ConfigError> {
    for pair in table.clone().pairs::<Value, Value>() {
        let (key, _) = pair.map_err(|_| ConfigError::Evaluation {
            path: config_path.to_owned(),
        })?;
        let Value::String(key) = key else {
            return Err(schema_error(
                config_path,
                field,
                "contains a non-string or unknown field",
            ));
        };
        let Ok(key) = key.to_str() else {
            return Err(schema_error(
                config_path,
                field,
                "contains a non-UTF-8 or unknown field",
            ));
        };
        if !allowed.contains(&key.as_ref()) {
            return Err(schema_error(
                config_path,
                field,
                "contains an unknown field",
            ));
        }
    }
    Ok(())
}

fn raw_value(
    table: &Table,
    key: &'static str,
    config_path: &Path,
    field: &'static str,
) -> Result<Value, ConfigError> {
    table.raw_get(key).map_err(|_| ConfigError::Schema {
        path: config_path.to_owned(),
        field,
        problem: "cannot be read",
    })
}

fn required_string(
    table: &Table,
    key: &'static str,
    field: &'static str,
    config_path: &Path,
) -> Result<String, ConfigError> {
    let Value::String(value) = raw_value(table, key, config_path, field)? else {
        return Err(schema_error(
            config_path,
            field,
            "must be a non-empty string",
        ));
    };
    let value = value
        .to_str()
        .map_err(|_| schema_error(config_path, field, "must be valid UTF-8"))?;
    if value.is_empty() {
        return Err(schema_error(
            config_path,
            field,
            "must be a non-empty string",
        ));
    }
    Ok(value.to_owned())
}

fn required_path(
    table: &Table,
    key: &'static str,
    field: &'static str,
    config_root: &Path,
    config_path: &Path,
) -> Result<PathBuf, ConfigError> {
    let path = PathBuf::from(required_string(table, key, field, config_path)?);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(config_root.join(path))
    }
}

fn replica_store(
    node: &Table,
    config_root: &Path,
    config_path: &Path,
) -> Result<ReplicaStoreConfig, ConfigError> {
    let store = match raw_value(node, "replica_store", config_path, "node.replica_store")? {
        Value::Table(table) => table,
        _ => {
            return Err(schema_error(
                config_path,
                "node.replica_store",
                "must be a table",
            ));
        }
    };
    ensure_plain_table(&store, config_path, "node.replica_store")?;
    ensure_fields(
        &store,
        &["driver", "path", "url"],
        config_path,
        "node.replica_store",
    )?;

    let driver = required_string(&store, "driver", "node.replica_store.driver", config_path)?;
    match driver.as_str() {
        "sqlite" => {
            if !matches!(
                raw_value(&store, "url", config_path, "node.replica_store.url")?,
                Value::Nil
            ) {
                return Err(schema_error(
                    config_path,
                    "node.replica_store.url",
                    "is not valid for the sqlite driver",
                ));
            }
            let path = required_path(
                &store,
                "path",
                "node.replica_store.path",
                config_root,
                config_path,
            )?;
            Ok(ReplicaStoreConfig::Sqlite { path })
        }
        "postgres" => {
            if !matches!(
                raw_value(&store, "path", config_path, "node.replica_store.path")?,
                Value::Nil
            ) {
                return Err(schema_error(
                    config_path,
                    "node.replica_store.path",
                    "is not valid for the postgres driver",
                ));
            }
            let value = required_string(&store, "url", "node.replica_store.url", config_path)?;
            let url = value.parse().map_err(|_| {
                schema_error(
                    config_path,
                    "node.replica_store.url",
                    "must be a PostgreSQL connection URL",
                )
            })?;
            Ok(ReplicaStoreConfig::Postgres { url })
        }
        _ => Err(schema_error(
            config_path,
            "node.replica_store.driver",
            "must be sqlite or postgres",
        )),
    }
}

fn optional_listen(table: &Table, config_path: &Path) -> Result<Option<SocketAddr>, ConfigError> {
    match raw_value(table, "listen", config_path, "node.listen")? {
        Value::Nil => Ok(None),
        Value::String(value) => {
            let value = value
                .to_str()
                .map_err(|_| schema_error(config_path, "node.listen", "must be valid UTF-8"))?;
            value
                .parse()
                .map(Some)
                .map_err(|_| schema_error(config_path, "node.listen", "must be a socket address"))
        }
        _ => Err(schema_error(
            config_path,
            "node.listen",
            "must be a socket address string or nil",
        )),
    }
}

fn connect_urls(table: &Table, config_path: &Path) -> Result<Vec<ConnectUrl>, ConfigError> {
    let connect = match raw_value(table, "connect", config_path, "node.connect")? {
        Value::Table(table) => table,
        _ => {
            return Err(schema_error(
                config_path,
                "node.connect",
                "must be an array",
            ));
        }
    };
    ensure_plain_table(&connect, config_path, "node.connect")?;

    let len = connect.raw_len();
    let mut urls = Vec::with_capacity(len);
    for index in 1..=len {
        let value: Value = connect
            .raw_get(index)
            .map_err(|_| schema_error(config_path, "node.connect", "must be a contiguous array"))?;
        let Value::String(value) = value else {
            return Err(schema_error(
                config_path,
                "node.connect",
                "entries must be URL strings",
            ));
        };
        let value = value.to_str().map_err(|_| {
            schema_error(config_path, "node.connect", "entries must be valid UTF-8")
        })?;
        urls.push(
            value.parse().map_err(|_| {
                schema_error(config_path, "node.connect", "contains an invalid URL")
            })?,
        );
    }

    for pair in connect.pairs::<Value, Value>() {
        let (key, _) = pair.map_err(|_| ConfigError::Evaluation {
            path: config_path.to_owned(),
        })?;
        let Value::Integer(index) = key else {
            return Err(schema_error(
                config_path,
                "node.connect",
                "must contain only contiguous integer indexes",
            ));
        };
        if index < 1 || usize::try_from(index).ok().is_none_or(|index| index > len) {
            return Err(schema_error(
                config_path,
                "node.connect",
                "must contain only contiguous integer indexes",
            ));
        }
    }
    Ok(urls)
}

fn schema_error(path: &Path, field: &'static str, problem: &'static str) -> ConfigError {
    ConfigError::Schema {
        path: path.to_owned(),
        field,
        problem,
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ConfigError {
    Read {
        path: PathBuf,
        kind: io::ErrorKind,
    },
    InvalidUtf8 {
        path: PathBuf,
    },
    RuntimeInitialization,
    Evaluation {
        path: PathBuf,
    },
    Schema {
        path: PathBuf,
        field: &'static str,
        problem: &'static str,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, kind } => {
                write!(formatter, "cannot read {}: {kind}", path.display())
            }
            Self::InvalidUtf8 { path } => {
                write!(
                    formatter,
                    "{} is not valid UTF-8 Lua source",
                    path.display()
                )
            }
            Self::RuntimeInitialization => {
                formatter.write_str("cannot initialize the embedded LuaJIT configuration runtime")
            }
            Self::Evaluation { path } => write!(
                formatter,
                "cannot evaluate {}; check its Lua syntax and runtime operations",
                path.display()
            ),
            Self::Schema {
                path,
                field,
                problem,
            } => write!(
                formatter,
                "invalid configuration in {}: {field} {problem}",
                path.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "oll-config-test-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, relative: &str, source: &str) {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, source).unwrap();
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn literal_config(replica: &str, log: &str) -> String {
        format!(
            r#"
            return {{
                format_version = 1,
                node = {{
                    replica_root = "{replica}",
                    replica_store = {{
                        driver = "sqlite",
                        path = "store/replica.sqlite3",
                    }},
                    log_dir = "{log}",
                    listen = nil,
                    connect = {{}},
                }},
            }}
            "#
        )
    }

    fn config_with_store(store: &str) -> String {
        format!(
            r#"
            return {{
                format_version = 1,
                node = {{
                    replica_root = "replica",
                    replica_store = {store},
                    log_dir = "log",
                    listen = nil,
                    connect = {{}},
                }},
            }}
            "#
        )
    }

    fn load_with_environment(
        directory: &TestDirectory,
        lookup: impl Fn(&str) -> Result<Option<String>, ()> + Send + Sync + 'static,
    ) -> Result<(ConfigRuntime, ResolvedNodeConfig), ConfigError> {
        ConfigRuntime::load_with_environment(directory.path(), Arc::new(lookup))
    }

    #[test]
    fn evaluates_computed_configuration_with_modules_and_getenv() {
        let directory = TestDirectory::new();
        directory.write(
            "paths.lua",
            r#"
            return {
                replica = "data/replica",
                endpoint = "https://node-a.example.com",
            }
            "#,
        );
        directory.write(
            CONFIG_FILENAME,
            r#"
            local paths = require("paths")
            local duplicate = require("paths")
            assert(paths == duplicate)
            assert(jit.status())

            return {
                format_version = 1,
                node = {
                    replica_root = paths.replica,
                    replica_store = {
                        driver = "postgres",
                        url = oll.getenv("OLL_TEST_POSTGRES"),
                    },
                    log_dir = oll.getenv("OLL_TEST_LOG"),
                    listen = "127.0.0.1:7443",
                    connect = { paths.endpoint, "https://node-b.example.com" },
                },
            }
            "#,
        );

        let (runtime, config) = load_with_environment(&directory, |name| match name {
            "OLL_TEST_POSTGRES" => Ok(Some("postgresql://user:secret@localhost/oll".to_owned())),
            "OLL_TEST_LOG" => Ok(Some("state/log".to_owned())),
            _ => panic!("unexpected environment lookup: {name}"),
        })
        .unwrap();

        assert_eq!(config.replica_root, directory.path().join("data/replica"));
        assert!(matches!(
            config.replica_store,
            ReplicaStoreConfig::Postgres { .. }
        ));
        assert_eq!(config.log_dir, directory.path().join("state/log"));
        assert_eq!(config.listen, Some("127.0.0.1:7443".parse().unwrap()));
        assert_eq!(
            config
                .connect
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["https://node-a.example.com/", "https://node-b.example.com/"]
        );

        let globals = runtime.lua().globals();
        for unavailable in ["ffi", "debug", "io", "os", "package", "dofile", "loadfile"] {
            assert!(matches!(
                globals.get::<Value>(unavailable).unwrap(),
                Value::Nil
            ));
        }
        assert!(matches!(
            globals.get::<Value>("jit").unwrap(),
            Value::Table(_)
        ));
        assert!(
            runtime
                .lua()
                .named_registry_value::<Table>(ROOT_REGISTRY_KEY)
                .is_ok()
        );
    }

    #[test]
    fn requires_exactly_one_plain_versioned_table() {
        for (source, expected) in [
            ("return", "must contain exactly one value"),
            ("return {}, {}", "must contain exactly one value"),
            ("return 1", "must be a table"),
            (
                r#"return setmetatable({ format_version = 1, node = {} }, {})"#,
                "must not have a metatable",
            ),
            (
                &literal_config("replica", "log").replace(
                    "format_version = 1,",
                    "format_version = 1, surprise = true,",
                ),
                "contains an unknown field",
            ),
            (
                &literal_config("replica", "log").replace(
                    "connect = {},",
                    "connect = { [1] = \"https://a.example\", [3] = \"https://b.example\" },",
                ),
                "contiguous integer indexes",
            ),
        ] {
            let directory = TestDirectory::new();
            directory.write(CONFIG_FILENAME, source);
            let error = ConfigRuntime::load(directory.path()).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn replica_store_is_a_strict_tagged_configuration() {
        for store in [
            r#"{ driver = "sqlite" }"#,
            r#"{ driver = "sqlite", path = "store.sqlite3", url = "postgresql://secret@host/db" }"#,
            r#"{ driver = "sqlite", path = "store.sqlite3", extra = true }"#,
            r#"{ driver = "postgres" }"#,
            r#"{ driver = "postgres", url = "postgresql://secret@host/db", path = "store.sqlite3" }"#,
            r#"{ driver = "postgres", url = "not-a-postgres-url" }"#,
            r#"{ driver = "unknown", path = "store.sqlite3" }"#,
        ] {
            let directory = TestDirectory::new();
            directory.write(CONFIG_FILENAME, &config_with_store(store));
            let error = ConfigRuntime::load(directory.path()).unwrap_err();
            let diagnostic = error.to_string();
            assert!(diagnostic.contains("node.replica_store"));
            assert!(!diagnostic.contains("secret"));
            assert!(!diagnostic.contains("not-a-postgres-url"));
        }
    }

    #[test]
    fn rejects_invalid_module_names_cycles_and_symlink_escapes() {
        let invalid = TestDirectory::new();
        invalid.write(CONFIG_FILENAME, "return require('../outside')");
        assert!(matches!(
            ConfigRuntime::load(invalid.path()),
            Err(ConfigError::Evaluation { .. })
        ));

        let cyclic = TestDirectory::new();
        cyclic.write(CONFIG_FILENAME, "return require('a')");
        cyclic.write("a.lua", "return require('b')");
        cyclic.write("b.lua", "return require('a')");
        assert!(matches!(
            ConfigRuntime::load(cyclic.path()),
            Err(ConfigError::Evaluation { .. })
        ));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let escaped = TestDirectory::new();
            let outside = TestDirectory::new();
            outside.write("secret.lua", &literal_config("replica", "log"));
            symlink(
                outside.path().join("secret.lua"),
                escaped.path().join("link.lua"),
            )
            .unwrap();
            escaped.write(CONFIG_FILENAME, "return require('link')");
            assert!(matches!(
                ConfigRuntime::load(escaped.path()),
                Err(ConfigError::Evaluation { .. })
            ));
        }
    }

    #[test]
    fn evaluation_diagnostics_do_not_echo_lua_values() {
        let directory = TestDirectory::new();
        directory.write(CONFIG_FILENAME, "error('TOP_SECRET_VALUE')");

        let error = ConfigRuntime::load(directory.path()).unwrap_err();
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("cannot evaluate"));
        assert!(!diagnostic.contains("TOP_SECRET_VALUE"));
    }

    #[test]
    fn non_utf8_environment_values_are_configuration_errors() {
        let directory = TestDirectory::new();
        directory.write(
            CONFIG_FILENAME,
            r#"
            return {
                format_version = 1,
                node = {
                    replica_root = "replica",
                    replica_store = {
                        driver = "sqlite",
                        path = "store/replica.sqlite3",
                    },
                    log_dir = oll.getenv("NON_UTF8"),
                    listen = nil,
                    connect = {},
                },
            }
            "#,
        );
        let error = load_with_environment(&directory, |_| Err(())).unwrap_err();
        assert!(matches!(error, ConfigError::Evaluation { .. }));
    }
}
