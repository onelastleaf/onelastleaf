use std::{
    fs,
    io::{self, BufRead, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
};

use crate::cli::{ConnectUrl, PreparedInitIntent};

use super::{
    identity::{NodeIdentity, atomic_write, identity_path},
    lock::DeploymentLock,
    logging::ensure_log_directory,
    runtime::NodeError,
};

const CONFIG_FILENAME: &str = "config.lua";

pub enum InitResult {
    Initialized(NodeIdentity),
    Cancelled,
}

pub fn initialize(intent: PreparedInitIntent) -> Result<InitResult, NodeError> {
    let _lock = DeploymentLock::acquire_for_init(&intent.config_root)?;
    let config_path = intent.config_root.join(CONFIG_FILENAME);
    let node_path = identity_path(&intent.config_root);
    if (path_exists(&config_path)? || path_exists(&node_path)?) && !confirm_replacement()? {
        return Ok(InitResult::Cancelled);
    }

    ensure_directory(&intent.config_root, 0o700, "configuration root")?;
    ensure_directory(&intent.replica_root, 0o700, "replica root")?;
    ensure_log_directory(&intent.log_dir)?;

    let identity = NodeIdentity::generate(intent.node_name);
    let replica_store = intent
        .replica_store_base
        .join("stores")
        .join(identity.node_id().to_string())
        .join("replica.sqlite3");
    let store_parent = replica_store.parent().ok_or_else(|| {
        NodeError::Internal("generated replica store path has no parent".to_owned())
    })?;
    ensure_directory(store_parent, 0o700, "replica store directory")?;
    let source = initial_config(
        &intent.replica_root,
        &replica_store,
        &intent.log_dir,
        intent.listen,
        &intent.connect,
    )?;
    atomic_write(&config_path, source.as_bytes())?;

    NodeIdentity::write(&intent.config_root, &identity)?;
    Ok(InitResult::Initialized(identity))
}

fn path_exists(path: &Path) -> Result<bool, NodeError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(NodeError::io("inspect initialization file", error)),
    }
}

fn confirm_replacement() -> Result<bool, NodeError> {
    let mut stderr = io::stderr().lock();
    writeln!(
        stderr,
        "oll: existing initialization material will be replaced and a new node identity generated."
    )
    .map_err(|error| NodeError::io("write initialization warning", error))?;
    write!(stderr, "oll: replace config.lua and node.json? [y/N] ")
        .map_err(|error| NodeError::io("write initialization prompt", error))?;
    stderr
        .flush()
        .map_err(|error| NodeError::io("flush initialization prompt", error))?;

    let mut answer = String::new();
    let read = io::stdin()
        .lock()
        .read_line(&mut answer)
        .map_err(|error| NodeError::io("read initialization confirmation", error))?;
    if read == 0 {
        return Ok(false);
    }
    Ok(matches!(answer.trim(), "y" | "yes"))
}

fn ensure_directory(path: &Path, mode: u32, name: &'static str) -> Result<(), NodeError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(NodeError::Config(format!(
            "{name} {} is not a directory",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .map_err(|error| NodeError::io("create deployment directory", error))?;
            fs::set_permissions(path, fs::Permissions::from_mode(mode))
                .map_err(|error| NodeError::io("set deployment directory permissions", error))
        }
        Err(error) => Err(NodeError::io("inspect deployment directory", error)),
    }
}

fn initial_config(
    replica_root: &Path,
    replica_store: &Path,
    log_dir: &Path,
    listen: Option<std::net::SocketAddr>,
    connect: &[ConnectUrl],
) -> Result<String, NodeError> {
    let replica_root = lua_string(replica_root.to_str().ok_or_else(|| {
        NodeError::Config("cannot persist replica root: path is not valid UTF-8".to_owned())
    })?);
    let replica_store = lua_string(replica_store.to_str().ok_or_else(|| {
        NodeError::Config("cannot persist replica store: path is not valid UTF-8".to_owned())
    })?);
    let log_dir = lua_string(log_dir.to_str().ok_or_else(|| {
        NodeError::Config("cannot persist log directory: path is not valid UTF-8".to_owned())
    })?);
    let listen = listen.map_or_else(
        || "nil".to_owned(),
        |address| lua_string(&address.to_string()),
    );
    let mut source = format!(
        "return {{\n    format_version = 1,\n    node = {{\n        replica_root = {replica_root},\n        replica_store = {{\n            driver = \"sqlite\",\n            path = {replica_store},\n        }},\n        log_dir = {log_dir},\n        listen = {listen},\n        connect = {{"
    );
    if !connect.is_empty() {
        source.push('\n');
        for url in connect {
            source.push_str("            ");
            source.push_str(&lua_string(&url.to_string()));
            source.push_str(",\n");
        }
        source.push_str("        ");
    }
    source.push_str("},\n    },\n}\n");
    Ok(source)
}

fn lua_string(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 2);
    result.push('"');
    for character in input.chars() {
        match character {
            '\\' => result.push_str("\\\\"),
            '"' => result.push_str("\\\""),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\u{0000}'..='\u{001f}' | '\u{007f}' => {
                result.push_str(&format!("\\{:03}", character as u32));
            }
            _ => result.push(character),
        }
    }
    result.push('"');
    result
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tempfile::TempDir;

    use super::*;

    fn intent(directory: &TempDir) -> PreparedInitIntent {
        PreparedInitIntent {
            node_name: "home-node".parse().unwrap(),
            connect: vec!["https://peer.example".parse().unwrap()],
            listen: Some("127.0.0.1:7443".parse::<SocketAddr>().unwrap()),
            replica_root: directory.path().join("replica"),
            replica_store_base: directory.path().join("data"),
            config_root: directory.path().join("config"),
            log_dir: directory.path().join("log"),
        }
    }

    #[test]
    fn writes_initial_files_and_an_empty_replica_slot() {
        let directory = TempDir::new().unwrap();
        let intent = intent(&directory);
        let config_root = intent.config_root.clone();
        let replica_root = intent.replica_root.clone();
        let result = initialize(intent).unwrap();

        let InitResult::Initialized(identity) = result else {
            panic!("initialization was unexpectedly cancelled")
        };
        assert_eq!(NodeIdentity::load(&config_root).unwrap(), identity);
        assert!(replica_root.is_dir());
        let config = fs::read_to_string(config_root.join(CONFIG_FILENAME)).unwrap();
        assert!(config.contains("format_version = 1"));
        assert!(config.contains("driver = \"sqlite\""));
        assert!(config.contains(&identity.node_id().to_string()));
        assert!(config.contains("https://peer.example/"));
    }

    #[test]
    fn lua_strings_escape_control_bytes_without_json_unicode_escapes() {
        assert_eq!(lua_string("a\n\u{0001}b"), "\"a\\n\\001b\"");
    }
}
