use std::{
    fs, io,
    net::TcpListener,
    os::unix::net::UnixListener,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::TempDir;
use uuid::Uuid;

const EXIT_UNAVAILABLE: i32 = 69;
const EXIT_CONFIG: i32 = 78;

fn oll() -> Command {
    Command::new(env!("CARGO_BIN_EXE_oll"))
}

fn unix_sockets_available(directory: &TempDir) -> bool {
    let path = directory.path().join("capability.sock");
    match UnixListener::bind(path) {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => false,
        Err(error) => panic!("cannot probe Unix socket support: {error}"),
    }
}

fn loopback_available() -> bool {
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => false,
        Err(error) => panic!("cannot probe loopback support: {error}"),
    }
}

fn initialize(directory: &TempDir) {
    let output = oll()
        .env("XDG_DATA_HOME", directory.path().join("data"))
        .args(["init", "home-node", "--config"])
        .arg(directory.path().join("config"))
        .arg("--replica")
        .arg(directory.path().join("replica"))
        .arg("--log-dir")
        .arg(directory.path().join("log"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_run(config_root: &Path) -> Child {
    oll()
        .arg("run")
        .arg("--config")
        .arg(config_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn wait_for_status(config_root: &Path) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let output = oll()
            .env("OLL_CONFIG", config_root)
            .args(["status", "--json"])
            .output()
            .unwrap();
        if output.status.success() {
            return serde_json::from_slice(&output.stdout).unwrap();
        }
        if Instant::now() >= deadline {
            panic!(
                "daemon did not become ready: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn stop(config_root: &Path) {
    let output = oll()
        .env("OLL_CONFIG", config_root)
        .arg("stop")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wait_for_identity(
    config_root: &Path,
    node_id: &str,
    node_name: &str,
    replica_id: Option<&str>,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = wait_for_status(config_root);
        if status["node_id"] == node_id
            && status["node_name"] == node_name
            && replica_id.is_none_or(|replica_id| status["replica_id"] == replica_id)
        {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not adopt the expected identity: {status}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn replace_file_atomically(path: &Path, contents: impl AsRef<[u8]>) {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    fs::write(&temporary, contents).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn wait_for_log_event(path: &Path, event: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fs::read_to_string(path).is_ok_and(|records| {
            records.lines().any(|line| {
                serde_json::from_str::<Value>(line).is_ok_and(|record| record["event"] == event)
            })
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not emit {event} before the deadline"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn foreground_daemon_serves_status_filters_and_graceful_stop() {
    let directory = TempDir::new().unwrap();
    if !unix_sockets_available(&directory) {
        return;
    }
    initialize(&directory);
    let config_root = directory.path().join("config");
    let document = directory.path().join("replica/operations.md");
    fs::write(&document, "operation history").unwrap();
    let mut daemon = ChildGuard(spawn_run(&config_root));

    let status = wait_for_status(&config_root);
    assert_eq!(status["node_name"], "home-node");
    assert_eq!(status["lifecycle"], "running");

    let operations = oll()
        .env("OLL_CONFIG", &config_root)
        .args(["replica", "ops"])
        .arg(&document)
        .output()
        .unwrap();
    assert!(
        operations.status.success(),
        "replica ops failed: {}",
        String::from_utf8_lossy(&operations.stderr)
    );
    let operations = String::from_utf8(operations.stdout).unwrap();
    let operation_id = operations
        .split_whitespace()
        .find_map(|field| field.strip_prefix("operation_id="))
        .unwrap_or_else(|| panic!("text output omitted operation_id: {operations}"));
    assert!(!operation_id.is_empty());

    let filter = oll()
        .env("OLL_CONFIG", &config_root)
        .args(["log", "set", "oll::sync=trace"])
        .output()
        .unwrap();
    assert!(
        filter.status.success(),
        "set filter failed: {}",
        String::from_utf8_lossy(&filter.stderr)
    );

    let second = oll()
        .arg("run")
        .arg("--config")
        .arg(&config_root)
        .output()
        .unwrap();
    assert_eq!(second.status.code(), Some(EXIT_UNAVAILABLE));

    stop(&config_root);
    assert!(daemon.0.wait().unwrap().success());
    assert!(!config_root.join("run/admin.sock").exists());

    let records = fs::read_to_string(directory.path().join("log/oll.log")).unwrap();
    let events = records
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(events.iter().any(|event| event["event"] == "node_ready"));
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "node_shutdown_completed")
    );
    assert!(events.iter().all(|event| {
        event["correlation_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    }));
    let shutdown_requested = events
        .iter()
        .find(|event| event["event"] == "node_shutdown_requested")
        .unwrap()["correlation_id"]
        .as_str()
        .unwrap();
    let shutdown_completed = events
        .iter()
        .find(|event| event["event"] == "node_shutdown_completed")
        .unwrap()["correlation_id"]
        .as_str()
        .unwrap();
    assert_eq!(shutdown_requested, shutdown_completed);
}

#[test]
fn daemon_hot_loads_valid_node_and_replica_identity_replacements() {
    let directory = TempDir::new().unwrap();
    if !unix_sockets_available(&directory) {
        return;
    }
    initialize(&directory);
    let config_root = directory.path().join("config");
    fs::write(directory.path().join("replica/identity.md"), "identity").unwrap();
    let mut daemon = ChildGuard(spawn_run(&config_root));
    let initial = wait_for_status(&config_root);
    let initial_replica_id = initial["replica_id"].as_str().unwrap().to_owned();

    let replacement_node_id = Uuid::new_v4().to_string();
    replace_file_atomically(
        &config_root.join("node.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 1,
            "node_id": replacement_node_id,
            "node_name": "renamed-node",
        }))
        .unwrap(),
    );
    wait_for_identity(
        &config_root,
        &replacement_node_id,
        "renamed-node",
        Some(&initial_replica_id),
    );

    fs::write(config_root.join("node.json"), b"{").unwrap();
    thread::sleep(Duration::from_millis(400));
    let retained = wait_for_status(&config_root);
    assert_eq!(retained["node_id"], replacement_node_id);
    assert_eq!(retained["node_name"], "renamed-node");

    let replacement_replica_id = Uuid::new_v4().to_string();
    replace_file_atomically(
        &config_root.join("replica.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 1,
            "replica_id": replacement_replica_id,
        }))
        .unwrap(),
    );
    wait_for_identity(
        &config_root,
        &replacement_node_id,
        "renamed-node",
        Some(&replacement_replica_id),
    );

    fs::write(config_root.join("replica.json"), b"{").unwrap();
    thread::sleep(Duration::from_millis(400));
    assert_eq!(
        wait_for_status(&config_root)["replica_id"],
        replacement_replica_id
    );

    stop(&config_root);
    assert!(daemon.0.wait().unwrap().success());
}

#[test]
fn identity_watch_initial_reload_precedes_sync_listener_startup() {
    let directory = TempDir::new().unwrap();
    if !unix_sockets_available(&directory) || !loopback_available() {
        return;
    }
    initialize(&directory);
    let config_root = directory.path().join("config");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = listener.local_addr().unwrap();
    let config_path = config_root.join("config.lua");
    let config = fs::read_to_string(&config_path).unwrap();
    let configured = config.replace(
        "        listen = nil,",
        &format!(
            "        listen = \"{listen}\",\n        network_key = \"0123456789abcdef0123456789abcdef\","
        ),
    );
    assert_ne!(configured, config);
    fs::write(config_path, configured).unwrap();

    // Keep replica startup busy until the asynchronous logger has published
    // node_starting, giving this test a deterministic point after the initial
    // node.json load but before the identity watcher is registered.
    fs::write(
        directory.path().join("replica/slow-start.md"),
        vec![b'x'; 32 * 1024 * 1024],
    )
    .unwrap();
    let mut daemon = ChildGuard(spawn_run(&config_root));
    let log_path = directory.path().join("log/oll.log");
    wait_for_log_event(&log_path, "node_starting");

    let replacement_node_id = Uuid::new_v4().to_string();
    replace_file_atomically(
        &config_root.join("node.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 1,
            "node_id": replacement_node_id,
            "node_name": "identity-before-sync",
        }))
        .unwrap(),
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not report the occupied sync listener"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(EXIT_UNAVAILABLE));

    let records = fs::read_to_string(log_path).unwrap();
    let events = records
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let identity_updated = events
        .iter()
        .position(|event| {
            event["event"] == "node_identity_updated" && event["node_id"] == replacement_node_id
        })
        .expect("identity watcher did not publish the replacement identity");
    let sync_failed = events
        .iter()
        .position(|event| {
            event["event"] == "node_start_failed" && event["reason"] == "sync_runtime"
        })
        .expect("daemon did not report sync startup failure");
    assert!(identity_updated < sync_failed);
}

#[test]
fn start_performs_the_nonce_handshake_when_loopback_is_available() {
    let directory = TempDir::new().unwrap();
    if !unix_sockets_available(&directory) || !loopback_available() {
        return;
    }
    initialize(&directory);
    let config_root = directory.path().join("config");

    let started = oll()
        .env("OLL_CONFIG", &config_root)
        .arg("start")
        .output()
        .unwrap();
    assert!(
        started.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    assert_eq!(wait_for_status(&config_root)["node_name"], "home-node");
    stop(&config_root);
}

#[test]
fn daemon_rejects_persisted_and_overridden_working_tree_overlaps_before_startup() {
    let directory = TempDir::new().unwrap();
    initialize(&directory);
    let config_root = directory.path().join("config");
    let replica_root = directory.path().join("replica");
    let log_dir = directory.path().join("log");
    let config_path = config_root.join("config.lua");
    let original = fs::read_to_string(&config_path).unwrap();
    let nested_log = replica_root.join("logs");
    let invalid = original.replace(log_dir.to_str().unwrap(), nested_log.to_str().unwrap());
    assert_ne!(invalid, original);
    fs::write(&config_path, invalid).unwrap();

    let persisted = oll()
        .arg("run")
        .arg("--config")
        .arg(&config_root)
        .output()
        .unwrap();
    assert_eq!(persisted.status.code(), Some(EXIT_CONFIG));
    assert!(!nested_log.join("oll.log").exists());
    assert!(!config_root.join("run/admin.sock").exists());

    fs::write(&config_path, original).unwrap();
    let overridden = oll()
        .arg("run")
        .arg("--config")
        .arg(&config_root)
        .arg("--replica")
        .arg(&log_dir)
        .output()
        .unwrap();
    assert_eq!(overridden.status.code(), Some(EXIT_CONFIG));
    assert!(!config_root.join("run/admin.sock").exists());
}
