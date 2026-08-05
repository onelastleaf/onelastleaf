use std::{collections::HashMap, fs};

use tempfile::TempDir;

use crate::protocol::oll::{
    ConfigList, ConfigMap, ConfigPath, ConfigPathSegment, ConfigValue, config_path_segment,
    config_value,
};

use super::{ConfigRuntime, PluginConfigError, PluginConfigErrorKind};

fn runtime() -> (TempDir, ConfigRuntime) {
    let directory = TempDir::new().unwrap();
    fs::write(
        directory.path().join("config.lua"),
        r#"
            return {
                format_version = 1,
                node = {
                    replica_root = "replica",
                    replica_store = {
                        driver = "sqlite",
                        path = "store/replica.sqlite3",
                    },
                    log_dir = "log",
                    artifact_download_dir = "artifacts",
                    listen = nil,
                    connect = {},
                },
            }
        "#,
    )
    .unwrap();
    fs::create_dir(directory.path().join("plugins")).unwrap();
    let (runtime, _) = ConfigRuntime::load(directory.path()).unwrap();
    (directory, runtime)
}

fn key(value: &str) -> ConfigPathSegment {
    ConfigPathSegment {
        kind: Some(config_path_segment::Kind::Key(value.to_owned())),
    }
}

fn index(value: u64) -> ConfigPathSegment {
    ConfigPathSegment {
        kind: Some(config_path_segment::Kind::Index(value)),
    }
}

fn path(segments: Vec<ConfigPathSegment>) -> ConfigPath {
    ConfigPath { segments }
}

fn integer(value: i64) -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::IntegerValue(value)),
    }
}

fn list(values: Vec<ConfigValue>) -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::ListValue(ConfigList { values })),
    }
}

fn function_value(function: crate::protocol::oll::ConfigFunctionRef) -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::FunctionValue(function)),
    }
}

fn null() -> ConfigValue {
    ConfigValue {
        kind: Some(config_value::Kind::NullValue(
            prost_types::NullValue::NullValue as i32,
        )),
    }
}

#[test]
fn top_level_reads_reopen_the_file_and_apply_typed_zero_based_paths() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    let plugin = directory.path().join("plugins/oll.example.lua");
    fs::write(
        &plugin,
        r#"return { value = "first", list = { "zero", "one" }, empty = {} }"#,
    )
    .unwrap();

    let selected = runtime
        .get_plugin_config("session-1", &path(vec![key("list"), index(1)]))
        .unwrap();
    assert_eq!(
        selected.kind,
        Some(config_value::Kind::StringValue("one".to_owned()))
    );
    let empty = runtime
        .get_plugin_config("session-1", &path(vec![key("empty")]))
        .unwrap();
    assert_eq!(
        empty.kind,
        Some(config_value::Kind::MapValue(ConfigMap {
            entries: HashMap::new(),
        }))
    );

    let error = runtime
        .get_plugin_config("session-1", &path(vec![key("list"), key("bad")]))
        .unwrap_err();
    assert_eq!(error.kind(), PluginConfigErrorKind::InvalidArgument);
    let error = runtime
        .get_plugin_config("session-1", &path(vec![key("missing")]))
        .unwrap_err();
    assert_eq!(error.kind(), PluginConfigErrorKind::NotFound);

    fs::write(&plugin, r#"return { value = "second" }"#).unwrap();
    let selected = runtime
        .get_plugin_config("session-1", &path(vec![key("value")]))
        .unwrap();
    assert_eq!(
        selected.kind,
        Some(config_value::Kind::StringValue("second".to_owned()))
    );
}

#[test]
fn recursively_converts_values_and_keeps_function_handles_session_scoped() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    runtime
        .begin_plugin_session("session-2", "oll.second")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        r#"
            return {
                call = function(number, payload, timestamp, duration)
                    return number + 1, payload, timestamp, duration
                end,
            }
        "#,
    )
    .unwrap();
    fs::write(directory.path().join("plugins/oll.second.lua"), "return {}").unwrap();

    let callback = runtime
        .get_plugin_config("session-1", &path(vec![key("call")]))
        .unwrap();
    let Some(config_value::Kind::FunctionValue(function)) = callback.kind else {
        panic!("expected a function handle");
    };
    assert_eq!(function.session_id, "session-1");
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        "return { call = function() return 999 end }",
    )
    .unwrap();

    let payload = ConfigValue {
        kind: Some(config_value::Kind::MapValue(ConfigMap {
            entries: HashMap::from([
                ("nothing".to_owned(), null()),
                (
                    "bytes".to_owned(),
                    ConfigValue {
                        kind: Some(config_value::Kind::BytesValue(vec![0xff, 0x00])),
                    },
                ),
                (
                    "list".to_owned(),
                    ConfigValue {
                        kind: Some(config_value::Kind::ListValue(ConfigList {
                            values: vec![integer(1), integer(2)],
                        })),
                    },
                ),
            ]),
        })),
    };
    let timestamp = prost_types::Timestamp {
        seconds: 1_700_000_000,
        nanos: 123,
    };
    let duration = prost_types::Duration {
        seconds: 12,
        nanos: 456,
    };
    let results = runtime
        .invoke_plugin_config_function(
            "session-1",
            &function,
            &[
                integer(41),
                payload.clone(),
                ConfigValue {
                    kind: Some(config_value::Kind::TimestampValue(timestamp)),
                },
                ConfigValue {
                    kind: Some(config_value::Kind::DurationValue(duration)),
                },
            ],
        )
        .unwrap();
    assert_eq!(results[0], integer(42));
    assert_eq!(results[1], payload);
    assert_eq!(
        results[2].kind,
        Some(config_value::Kind::TimestampValue(timestamp))
    );
    assert_eq!(
        results[3].kind,
        Some(config_value::Kind::DurationValue(duration))
    );

    let error = runtime
        .invoke_plugin_config_function("session-2", &function, &[])
        .unwrap_err();
    assert!(matches!(error, PluginConfigError::FunctionSessionMismatch));
    runtime.end_plugin_session("session-1").unwrap();
    let error = runtime
        .invoke_plugin_config_function("session-1", &function, &[])
        .unwrap_err();
    assert!(matches!(error, PluginConfigError::SessionNotActive));
    runtime.end_plugin_session("session-1").unwrap();
}

#[test]
fn preserves_wire_list_identity_without_reclassifying_lua_empty_tables() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        r#"
            return {
                identity = function(value) return value end,
                clear = function(value)
                    for index = #value, 1, -1 do
                        value[index] = nil
                    end
                    return value
                end,
                literal = function() return {} end,
            }
        "#,
    )
    .unwrap();

    let mut functions = HashMap::new();
    for name in ["identity", "clear", "literal"] {
        let value = runtime
            .get_plugin_config("session-1", &path(vec![key(name)]))
            .unwrap();
        let Some(config_value::Kind::FunctionValue(function)) = value.kind else {
            panic!("expected a function handle");
        };
        functions.insert(name, function);
    }

    let empty_list = runtime
        .invoke_plugin_config_function(
            "session-1",
            functions.get("identity").unwrap(),
            &[list(Vec::new())],
        )
        .unwrap();
    assert_eq!(empty_list, vec![list(Vec::new())]);

    let cleared_list = runtime
        .invoke_plugin_config_function(
            "session-1",
            functions.get("clear").unwrap(),
            &[list(vec![integer(1)])],
        )
        .unwrap();
    assert_eq!(cleared_list, vec![list(Vec::new())]);

    let literal = runtime
        .invoke_plugin_config_function("session-1", functions.get("literal").unwrap(), &[])
        .unwrap();
    assert_eq!(
        literal,
        vec![ConfigValue {
            kind: Some(config_value::Kind::MapValue(ConfigMap {
                entries: HashMap::new(),
            })),
        }]
    );
}

#[test]
fn serializes_concurrent_invocations_on_the_shared_lua_registry() {
    use std::sync::{Arc, Barrier};

    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        r#"
            local count = 0
            return {
                next = function()
                    count = count + 1
                    return count
                end,
            }
        "#,
    )
    .unwrap();
    let callback = runtime
        .get_plugin_config("session-1", &path(vec![key("next")]))
        .unwrap();
    let Some(config_value::Kind::FunctionValue(function)) = callback.kind else {
        panic!("expected a function handle");
    };

    let barrier = Arc::new(Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let runtime = runtime.clone();
        let function = function.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let results = runtime
                .invoke_plugin_config_function("session-1", &function, &[])
                .unwrap();
            let Some(config_value::Kind::IntegerValue(value)) = results[0].kind else {
                panic!("expected the closure counter");
            };
            value
        }));
    }
    let mut values = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, (1..=8).collect::<Vec<_>>());
}

#[test]
fn binds_live_files_and_function_handles_to_the_immutable_plugin_id() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.first")
        .unwrap();
    runtime
        .begin_plugin_session("session-2", "oll.second")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.first.lua"),
        "return { value = 'first', call = function() return 'first' end }",
    )
    .unwrap();
    fs::write(
        directory.path().join("plugins/oll.second.lua"),
        "return { value = 'second' }",
    )
    .unwrap();

    let first = runtime
        .get_plugin_config("session-1", &path(vec![key("value")]))
        .unwrap();
    let second = runtime
        .get_plugin_config("session-2", &path(vec![key("value")]))
        .unwrap();
    assert_eq!(
        first.kind,
        Some(config_value::Kind::StringValue("first".to_owned()))
    );
    assert_eq!(
        second.kind,
        Some(config_value::Kind::StringValue("second".to_owned()))
    );

    let callback = runtime
        .get_plugin_config("session-1", &path(vec![key("call")]))
        .unwrap();
    let Some(config_value::Kind::FunctionValue(function)) = callback.kind else {
        panic!("expected a function handle");
    };
    assert!(matches!(
        runtime.invoke_plugin_config_function("session-2", &function, &[]),
        Err(PluginConfigError::FunctionSessionMismatch)
    ));
}

#[test]
fn reopens_the_plugin_file_but_keeps_controlled_require_results_cached() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    let plugin = directory.path().join("plugins/oll.example.lua");
    let module = directory.path().join("plugin_shared.lua");
    fs::write(&plugin, "return { value = require('plugin_shared').value }").unwrap();
    fs::write(&module, "return { value = 'first' }").unwrap();

    let first = runtime
        .get_plugin_config("session-1", &path(vec![key("value")]))
        .unwrap();
    assert_eq!(
        first.kind,
        Some(config_value::Kind::StringValue("first".to_owned()))
    );

    fs::write(&module, "return { value = 'second' }").unwrap();
    let cached = runtime
        .get_plugin_config("session-1", &path(vec![key("value")]))
        .unwrap();
    assert_eq!(
        cached.kind,
        Some(config_value::Kind::StringValue("first".to_owned()))
    );

    fs::write(&plugin, "return { value = 'top-level edit' }").unwrap();
    let edited = runtime
        .get_plugin_config("session-1", &path(vec![key("value")]))
        .unwrap();
    assert_eq!(
        edited.kind,
        Some(config_value::Kind::StringValue("top-level edit".to_owned()))
    );
}

#[test]
fn validates_recursive_arguments_and_handle_ownership_before_calling_lua() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        r#"
            local calls = 0
            return {
                invoke = function(callback)
                    calls = calls + 1
                    return calls, callback()
                end,
                answer = function() return 42 end,
            }
        "#,
    )
    .unwrap();
    let invoke = runtime
        .get_plugin_config("session-1", &path(vec![key("invoke")]))
        .unwrap();
    let answer = runtime
        .get_plugin_config("session-1", &path(vec![key("answer")]))
        .unwrap();
    let Some(config_value::Kind::FunctionValue(invoke)) = invoke.kind else {
        panic!("expected the invoking closure");
    };
    let Some(config_value::Kind::FunctionValue(answer)) = answer.kind else {
        panic!("expected the answer closure");
    };

    for invalid in [
        ConfigValue {
            kind: Some(config_value::Kind::NumberValue(f64::NAN)),
        },
        ConfigValue {
            kind: Some(config_value::Kind::TimestampValue(prost_types::Timestamp {
                seconds: 0,
                nanos: -1,
            })),
        },
        ConfigValue {
            kind: Some(config_value::Kind::DurationValue(prost_types::Duration {
                seconds: 1,
                nanos: -1,
            })),
        },
        ConfigValue { kind: None },
        function_value(crate::protocol::oll::ConfigFunctionRef {
            session_id: "session-1".to_owned(),
            function_id: "missing".to_owned(),
        }),
        function_value(crate::protocol::oll::ConfigFunctionRef {
            session_id: "another-session".to_owned(),
            function_id: answer.function_id.clone(),
        }),
    ] {
        assert!(
            runtime
                .invoke_plugin_config_function("session-1", &invoke, &[invalid])
                .is_err()
        );
    }

    let mut too_deep = null();
    for _ in 0..34 {
        too_deep = list(vec![too_deep]);
    }
    assert!(
        runtime
            .invoke_plugin_config_function("session-1", &invoke, &[too_deep])
            .is_err()
    );

    let results = runtime
        .invoke_plugin_config_function("session-1", &invoke, &[function_value(answer)])
        .unwrap();
    assert_eq!(results, vec![integer(1), integer(42)]);
}

#[test]
fn rejects_non_finite_and_over_nested_lua_results() {
    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        r#"
            return {
                non_finite = function() return 0 / 0 end,
                too_deep = function()
                    local value = true
                    for _ = 1, 34 do
                        value = { value }
                    end
                    return value
                end,
            }
        "#,
    )
    .unwrap();

    for name in ["non_finite", "too_deep"] {
        let callback = runtime
            .get_plugin_config("session-1", &path(vec![key(name)]))
            .unwrap();
        let Some(config_value::Kind::FunctionValue(function)) = callback.kind else {
            panic!("expected a function handle");
        };
        let error = runtime
            .invoke_plugin_config_function("session-1", &function, &[])
            .unwrap_err();
        assert_eq!(error.kind(), PluginConfigErrorKind::InvalidArgument);
    }
}

#[test]
fn rejects_cycles_invalid_modules_and_escaped_plugin_files() {
    let (directory, runtime) = runtime();
    assert_eq!(
        runtime
            .begin_plugin_session("session-1", "invalid")
            .unwrap_err()
            .kind(),
        PluginConfigErrorKind::InvalidArgument
    );
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    let plugin = directory.path().join("plugins/oll.example.lua");
    fs::write(
        &plugin,
        "local value = {}; value.self = value; return value",
    )
    .unwrap();
    let error = runtime
        .get_plugin_config("session-1", &path(vec![]))
        .unwrap_err();
    assert!(matches!(error, PluginConfigError::CyclicValue));

    fs::write(&plugin, "return 1, 2").unwrap();
    let error = runtime
        .get_plugin_config("session-1", &path(vec![]))
        .unwrap_err();
    assert_eq!(error.kind(), PluginConfigErrorKind::InvalidArgument);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let outside = TempDir::new().unwrap();
        let outside_file = outside.path().join("outside.lua");
        fs::write(&outside_file, "return 'secret'").unwrap();
        fs::remove_file(&plugin).unwrap();
        symlink(&outside_file, &plugin).unwrap();
        let error = runtime
            .get_plugin_config("session-1", &path(vec![]))
            .unwrap_err();
        assert_eq!(error.kind(), PluginConfigErrorKind::InvalidArgument);
    }
}

#[test]
fn runtime_is_shareable_between_async_owners() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ConfigRuntime>();

    let (directory, runtime) = runtime();
    runtime
        .begin_plugin_session("session-1", "oll.example")
        .unwrap();
    fs::write(
        directory.path().join("plugins/oll.example.lua"),
        "return { value = 7 }",
    )
    .unwrap();
    let clone = runtime.clone();
    let thread = std::thread::spawn(move || {
        clone
            .get_plugin_config("session-1", &path(vec![key("value")]))
            .unwrap()
    });
    assert_eq!(thread.join().unwrap(), integer(7));
}
