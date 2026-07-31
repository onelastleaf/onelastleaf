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
    let mut daemon = ChildGuard(spawn_run(&config_root));

    let status = wait_for_status(&config_root);
    assert_eq!(status["node_name"], "home-node");
    assert_eq!(status["lifecycle"], "running");

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
