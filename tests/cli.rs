use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const EXIT_UNAVAILABLE: i32 = 69;
const EXIT_CONFIG: i32 = 78;

fn oll() -> Command {
    Command::new(env!("CARGO_BIN_EXE_oll"))
}

struct TestDeployment {
    root: PathBuf,
    config: PathBuf,
}

impl TestDeployment {
    fn empty() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "oll-cli-integration-test-{}-{nonce}",
            std::process::id()
        ));
        let config = root.join("config");
        Self { root, config }
    }

    fn new() -> Self {
        let deployment = Self::empty();
        let config = deployment.config.clone();
        fs::create_dir_all(&config).unwrap();
        fs::write(
            config.join("config.lua"),
            r#"
            return {
                format_version = 1,
                node = {
                    replica_root = "replica",
                    log_dir = "log",
                    listen = nil,
                    connect = {},
                },
            }
            "#,
        )
        .unwrap();
        deployment
    }

    fn config(&self) -> &Path {
        &self.config
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn write_identity(&self) {
        fs::write(
            self.config.join("node.json"),
            r#"{"format_version":1,"node_id":"9ba4a1aa-4c7d-4b11-b902-3155cf8ca5f3","node_name":"test-node"}"#,
        )
        .unwrap();
    }
}

impl Drop for TestDeployment {
    fn drop(&mut self) {
        if self.root.exists() {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}

#[test]
fn help_exposes_the_complete_command_surface() {
    let output = oll().arg("--help").output().unwrap();
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    for command in [
        "init", "run", "start", "stop", "status", "log", "replica", "sync", "ping", "plugin", "job",
    ] {
        assert!(
            stdout.contains(command),
            "missing command {command} in:\n{stdout}"
        );
    }
}

#[test]
fn node_name_is_the_human_facing_identity_selector() {
    for arguments in [
        vec!["init", "--help"],
        vec!["sync", "--help"],
        vec!["ping", "--help"],
    ] {
        let output = oll().args(arguments).output().unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.contains("NODE_NAME"),
            "missing NODE_NAME in:\n{stdout}"
        );
        assert!(!stdout.contains("NODE_ID"), "exposed NodeId in:\n{stdout}");
    }
}

#[test]
fn init_and_run_expose_independent_directory_and_topology_flags() {
    let init = oll().args(["init", "--help"]).output().unwrap();
    let run = oll().args(["run", "--help"]).output().unwrap();
    assert!(init.status.success());
    assert!(run.status.success());

    let init_help = String::from_utf8(init.stdout).unwrap();
    let run_help = String::from_utf8(run.stdout).unwrap();
    for option in [
        "--replica",
        "--config",
        "--log-dir",
        "--listen",
        "--connect",
    ] {
        assert!(
            init_help.contains(option),
            "missing {option} in init help:\n{init_help}"
        );
        assert!(
            run_help.contains(option),
            "missing {option} in run help:\n{run_help}"
        );
    }
    assert!(!init_help.contains("--profile"));
    assert!(!run_help.contains("--profile"));
    assert!(!init_help.contains("--log-root"));
    assert!(!run_help.contains("--log-root"));
}

#[test]
fn sync_retry_help_defines_a_total_attempt_limit() {
    let output = oll().args(["sync", "--help"]).output().unwrap();
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("Maximum total attempts, including the initial synchronization attempt"),
        "ambiguous retry help:\n{stdout}"
    );
}

#[test]
fn pingback_is_hidden_but_reaches_node_validation() {
    let deployment = TestDeployment::new();
    let help = oll().args(["run", "--help"]).output().unwrap();
    assert!(help.status.success());
    assert!(!String::from_utf8(help.stdout).unwrap().contains("pingback"));

    let parsed = oll()
        .env("OLL_CONFIG", deployment.config())
        .args(["run", "--pingback", "127.0.0.1:43210"])
        .output()
        .unwrap();
    assert_eq!(parsed.status.code(), Some(EXIT_CONFIG));

    let non_loopback = oll()
        .args(["run", "--pingback", "0.0.0.0:43210"])
        .output()
        .unwrap();
    assert_eq!(non_loopback.status.code(), Some(2));
}

#[test]
fn clap_errors_use_exit_code_two() {
    for arguments in [
        vec!["init", "test-node", "--profile", "server"],
        vec!["run", "--listen", "not-an-address"],
        vec!["run", "--connect", "ftp://example.com"],
        vec!["init"],
        vec!["init", "Home"],
        vec!["replica", "ops", "/note.md", "--limit", "0"],
        vec!["sync", "home.example"],
        vec!["sync", "node-a", "--log"],
        vec!["log", "set", "oll::sync=Trace"],
        vec!["plugin"],
        vec!["plugin", "install", "--release"],
        vec!["plugin", "install", "../local-plugin"],
    ] {
        let output = oll().args(arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(2));
    }
}

#[test]
fn unavailable_commands_fail_without_side_effects() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temporary =
        std::env::temp_dir().join(format!("oll-cli-test-{}-{nonce}", std::process::id()));
    let config = temporary.join("config");

    let output = oll()
        .env("OLL_CONFIG", &config)
        .args(["plugin", "validate"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    assert!(!temporary.exists());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("command is not implemented")
    );
}

#[test]
fn explicit_and_environment_paths_do_not_require_home() {
    let explicit_deployment = TestDeployment::empty();
    let explicit_replica = explicit_deployment.root().join("replica");
    let explicit_log = explicit_deployment.root().join("log");
    let explicit = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env_remove("OLL_CONFIG")
        .env_remove("OLL_REPLICA")
        .env_remove("OLL_LOG_DIR")
        .args(["init", "explicit-node", "--config"])
        .arg(explicit_deployment.config())
        .arg("--replica")
        .arg(&explicit_replica)
        .arg("--log-dir")
        .arg(&explicit_log)
        .output()
        .unwrap();
    assert!(explicit.status.success());

    let environment_deployment = TestDeployment::empty();
    let environment_replica = environment_deployment.root().join("replica");
    let environment_log = environment_deployment.root().join("log");
    let from_environment = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env("OLL_CONFIG", environment_deployment.config())
        .env("OLL_REPLICA", &environment_replica)
        .env("OLL_LOG_DIR", &environment_log)
        .args(["init", "environment-node"])
        .output()
        .unwrap();
    assert!(from_environment.status.success());
}

#[test]
fn run_preparation_requires_only_the_config_root() {
    let deployment = TestDeployment::new();
    let output = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env_remove("OLL_REPLICA")
        .env_remove("OLL_LOG_DIR")
        .env("OLL_CONFIG", deployment.config())
        .arg("run")
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(EXIT_CONFIG),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("node identity"));
    assert!(!stderr.contains("HOME"));
}

#[test]
fn run_defers_lua_evaluation_until_the_node_handler() {
    let deployment = TestDeployment::new();
    fs::write(
        deployment.config().join("config.lua"),
        "error('DO_NOT_PRINT_THIS_SECRET')",
    )
    .unwrap();
    deployment.write_identity();

    let output = oll()
        .env_remove("HOME")
        .env("OLL_CONFIG", deployment.config())
        .arg("run")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_CONFIG));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot evaluate"));
    assert!(!stderr.contains("DO_NOT_PRINT_THIS_SECRET"));
}

#[test]
fn init_creates_and_refuses_to_replace_a_deployment_without_confirmation() {
    let deployment = TestDeployment::empty();
    let replica = deployment.root().join("replica");
    let log = deployment.root().join("log");
    let first = oll()
        .args(["init", "home-node", "--config"])
        .arg(deployment.config())
        .arg("--replica")
        .arg(&replica)
        .arg("--log-dir")
        .arg(&log)
        .output()
        .unwrap();
    assert!(first.status.success());
    let node_before = fs::read(deployment.config().join("node.json")).unwrap();
    assert!(replica.is_dir());
    assert!(log.is_dir());

    let second = oll()
        .args(["init", "other-node", "--config"])
        .arg(deployment.config())
        .arg("--replica")
        .arg(&replica)
        .arg("--log-dir")
        .arg(&log)
        .output()
        .unwrap();
    assert!(second.status.success());
    assert_eq!(
        fs::read(deployment.config().join("node.json")).unwrap(),
        node_before
    );
    assert!(
        String::from_utf8(second.stderr)
            .unwrap()
            .contains("will be replaced")
    );
}

#[test]
fn log_set_is_a_typed_admin_command() {
    let help = oll().args(["log", "set", "--help"]).output().unwrap();
    assert!(help.status.success());
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("TARGET=LEVEL")
    );

    let output = oll()
        .env("OLL_CONFIG", "/tmp/oll-config")
        .args(["log", "set", "oll::sync=trace"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
}

#[test]
fn missing_home_is_a_configuration_error_when_default_is_needed() {
    let output = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env_remove("OLL_CONFIG")
        .env_remove("OLL_REPLICA")
        .env_remove("OLL_LOG_DIR")
        .arg("run")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(EXIT_CONFIG));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("set HOME")
    );
}

#[test]
fn local_snapshot_intents_do_not_require_directory_configuration() {
    for arguments in [
        vec!["replica", "snapshot", "inspect", "file.ollsnap"],
        vec!["replica", "snapshot", "verify", "file.ollsnap"],
    ] {
        let output = oll()
            .env_remove("HOME")
            .env_remove("XDG_STATE_HOME")
            .env_remove("OLL_CONFIG")
            .env_remove("OLL_REPLICA")
            .env_remove("OLL_LOG_DIR")
            .args(&arguments)
            .output()
            .unwrap();

        assert_eq!(
            output.status.code(),
            Some(EXIT_UNAVAILABLE),
            "unexpected result for {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn log_intents_require_only_the_user_log_directory() {
    for arguments in [
        vec!["sync", "--log"],
        vec!["plugin", "--log"],
        vec!["plugin", "--log", "__all__"],
    ] {
        let missing = oll()
            .env_remove("HOME")
            .env_remove("XDG_STATE_HOME")
            .env_remove("OLL_CONFIG")
            .env_remove("OLL_REPLICA")
            .env_remove("OLL_LOG_DIR")
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(missing.status.code(), Some(EXIT_CONFIG));

        let configured = oll()
            .env_remove("HOME")
            .env_remove("XDG_STATE_HOME")
            .env_remove("OLL_CONFIG")
            .env_remove("OLL_REPLICA")
            .env("OLL_LOG_DIR", "/tmp/oll-log")
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(configured.status.code(), Some(EXIT_UNAVAILABLE));
    }
}

#[test]
fn admin_intents_require_config_but_not_a_client_replica_root() {
    for arguments in [
        vec!["replica", "inspect", "/note.md"],
        vec!["replica", "export", "--output", "file.ollsnap"],
        vec!["replica", "import", "file.ollsnap"],
    ] {
        let missing_config = oll()
            .env_remove("HOME")
            .env_remove("XDG_STATE_HOME")
            .env_remove("OLL_CONFIG")
            .env_remove("OLL_REPLICA")
            .env_remove("OLL_LOG_DIR")
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(missing_config.status.code(), Some(EXIT_CONFIG));

        let configured = oll()
            .env_remove("HOME")
            .env_remove("XDG_STATE_HOME")
            .env("OLL_CONFIG", "/tmp/oll-config")
            .env_remove("OLL_REPLICA")
            .env_remove("OLL_LOG_DIR")
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(configured.status.code(), Some(EXIT_UNAVAILABLE));
    }
}

#[test]
fn operational_command_is_unavailable_until_implemented() {
    let output = oll().args(["sync", "node-a", "-n", "3"]).output().unwrap();
    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("command is not implemented")
    );
}

#[test]
fn plugin_call_help_and_argv_use_action_language() {
    let help = oll().args(["plugin", "call", "--help"]).output().unwrap();
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout).unwrap();
    assert!(
        stdout.contains("<ACTION>"),
        "missing action help:\n{stdout}"
    );
    assert!(!stdout.contains("<METHOD>"), "stale method help:\n{stdout}");

    let output = oll()
        .args([
            "plugin",
            "call",
            "oll.example",
            "publish",
            "",
            "--flag",
            "--flag",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
}

#[test]
fn plugin_install_help_and_validation_expose_file_driven_workflow() {
    let install_help = oll()
        .args(["plugin", "install", "--help"])
        .output()
        .unwrap();
    assert!(install_help.status.success());
    let stdout = String::from_utf8(install_help.stdout).unwrap();
    assert!(
        stdout.contains("[GIT_REMOTE]"),
        "missing optional remote:\n{stdout}"
    );
    assert!(
        stdout.contains("plugins.lua"),
        "missing file workflow:\n{stdout}"
    );

    for arguments in [
        vec!["plugin", "install"],
        vec!["plugin", "validate"],
        vec![
            "plugin",
            "install",
            "git@github.com:example/plugin.git",
            "--release",
            "--branch",
            "release/v0.3.1",
        ],
    ] {
        let output = oll()
            .env_remove("HOME")
            .env("OLL_CONFIG", "/tmp/oll-config")
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    }
}
