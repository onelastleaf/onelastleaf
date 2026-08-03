use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use mlua::{Table, Value};

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
                endpoint = "oll://node-a.example.com:17384",
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
                    connect = { paths.endpoint, "oll://node-b.example.com:17384" },
                    network_key = "test-network-key-with-thirty-two-bytes",
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
        [
            "oll://node-a.example.com:17384",
            "oll://node-b.example.com:17384"
        ]
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
                "connect = { [1] = \"oll://a.example:17384\", [3] = \"oll://b.example:17384\" },",
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
fn storage_layout_rejects_every_working_tree_ancestor_relationship() {
    let directory = TestDirectory::new();
    let base = directory.path();
    let safe_store = ReplicaStoreConfig::Sqlite {
        path: base.join("store/replica.sqlite3"),
    };

    for (config_root, replica_root, log_dir, store, expected) in [
        (
            base.join("replica/config"),
            base.join("replica"),
            base.join("log"),
            safe_store.clone(),
            "config_root",
        ),
        (
            base.join("config"),
            base.join("config/replica"),
            base.join("log"),
            safe_store.clone(),
            "config_root",
        ),
        (
            base.join("replica"),
            base.join("replica"),
            base.join("log"),
            safe_store.clone(),
            "config_root",
        ),
        (
            base.join("config"),
            base.join("replica"),
            base.join("replica/log"),
            safe_store.clone(),
            "node.log_dir",
        ),
        (
            base.join("config"),
            base.join("replica"),
            base.join("replica"),
            safe_store.clone(),
            "node.log_dir",
        ),
        (
            base.join("config"),
            base.join("log/replica"),
            base.join("log"),
            safe_store.clone(),
            "node.log_dir",
        ),
        (
            base.join("config"),
            base.join("store/working"),
            base.join("log"),
            safe_store.clone(),
            "management directory",
        ),
        (
            base.join("config"),
            base.join("replica"),
            base.join("log"),
            ReplicaStoreConfig::Sqlite {
                path: base.join("replica/store/replica.sqlite3"),
            },
            "management directory",
        ),
        (
            base.join("config"),
            base.join("replica"),
            base.join("log"),
            ReplicaStoreConfig::Sqlite {
                path: base.join("replica/replica.sqlite3"),
            },
            "management directory",
        ),
    ] {
        let error =
            validate_storage_layout(&config_root, &replica_root, &log_dir, &store).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected storage-layout error: {error}"
        );
    }
}

#[cfg(unix)]
#[test]
fn storage_layout_resolves_existing_symlinked_ancestors() {
    use std::os::unix::fs::symlink;

    let directory = TestDirectory::new();
    let base = directory.path();
    fs::create_dir_all(base.join("real/replica")).unwrap();
    symlink(base.join("real"), base.join("alias")).unwrap();

    let error = validate_storage_layout(
        &base.join("config"),
        &base.join("real/replica"),
        &base.join("alias/replica/logs"),
        &ReplicaStoreConfig::Sqlite {
            path: base.join("store/replica.sqlite3"),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("node.log_dir"));

    let error = validate_storage_layout(
        &base.join("alias/replica/config"),
        &base.join("real/replica"),
        &base.join("log"),
        &ReplicaStoreConfig::Sqlite {
            path: base.join("store/replica.sqlite3"),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("config_root"));

    let error = validate_storage_layout(
        &base.join("config"),
        &base.join("real/replica"),
        &base.join("log"),
        &ReplicaStoreConfig::Sqlite {
            path: base.join("alias/replica/store/replica.sqlite3"),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("management directory"));
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
