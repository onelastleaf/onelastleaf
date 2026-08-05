use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use mlua::{Table, Value};

use super::{ConfigError, ConnectUrl, NetworkKey, ReplicaStoreConfig, ResolvedNodeConfig};

pub(super) fn decode_root(
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
            "artifact_download_dir",
            "listen",
            "connect",
            "network_key",
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
    let artifact_download_dir = required_path(
        &node,
        "artifact_download_dir",
        "node.artifact_download_dir",
        config_root,
        config_path,
    )?;
    let listen = optional_listen(&node, config_path)?;
    let connect = connect_urls(&node, config_path)?;
    let network_key = optional_network_key(&node, config_path)?;

    let config = ResolvedNodeConfig {
        replica_root,
        replica_store,
        log_dir,
        artifact_download_dir,
        listen,
        connect,
        network_key,
    };
    config
        .validate_sync_topology()
        .map_err(|problem| schema_error(config_path, "node.network_key", problem))?;
    Ok(config)
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
            let address: SocketAddr = value.parse().map_err(|_| {
                schema_error(config_path, "node.listen", "must be a socket address")
            })?;
            if address.port() == 0 {
                return Err(schema_error(
                    config_path,
                    "node.listen",
                    "must use a nonzero port",
                ));
            }
            Ok(Some(address))
        }
        _ => Err(schema_error(
            config_path,
            "node.listen",
            "must be a socket address string or nil",
        )),
    }
}

fn optional_network_key(
    table: &Table,
    config_path: &Path,
) -> Result<Option<NetworkKey>, ConfigError> {
    match raw_value(table, "network_key", config_path, "node.network_key")? {
        Value::Nil => Ok(None),
        Value::String(value) => Ok(Some(NetworkKey::new(value.as_bytes().to_vec()))),
        _ => Err(schema_error(
            config_path,
            "node.network_key",
            "must be a raw Lua byte string or nil",
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
