use std::{
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const EXIT_UNAVAILABLE: i32 = 69;
const EXIT_CONFIG: i32 = 78;

fn oll() -> Command {
    Command::new(env!("CARGO_BIN_EXE_oll"))
}

#[test]
fn help_exposes_the_complete_command_surface() {
    let output = oll().arg("--help").output().unwrap();
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    for command in [
        "init", "run", "start", "stop", "status", "replica", "sync", "ping", "plugin", "job",
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
    assert!(init_help.contains("--profile"));
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
fn pingback_is_internal_but_parseable_by_start() {
    let help = oll().args(["run", "--help"]).output().unwrap();
    assert!(help.status.success());
    assert!(!String::from_utf8(help.stdout).unwrap().contains("pingback"));

    let parsed = oll()
        .args(["run", "--pingback", "127.0.0.1:43210"])
        .output()
        .unwrap();
    assert_eq!(parsed.status.code(), Some(EXIT_UNAVAILABLE));

    let non_loopback = oll()
        .args(["run", "--pingback", "0.0.0.0:43210"])
        .output()
        .unwrap();
    assert_eq!(non_loopback.status.code(), Some(2));
}

#[test]
fn clap_errors_use_exit_code_two() {
    for arguments in [
        vec!["init", "--profile", "authority"],
        vec!["run", "--listen", "not-an-address"],
        vec!["run", "--connect", "ftp://example.com"],
        vec!["init"],
        vec!["init", "Home"],
        vec!["replica", "ops", "/note.md", "--limit", "0"],
        vec!["sync", "home.example"],
        vec!["sync", "node-a", "--log"],
        vec!["plugin"],
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
    let replica = temporary.join("replica");

    let output = oll()
        .args(["init", "test-node", "--replica"])
        .arg(&replica)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    assert!(!replica.exists());
    assert!(!temporary.exists());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("command is not implemented")
    );
}

#[test]
fn explicit_and_environment_paths_do_not_require_home() {
    let explicit = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env_remove("OLL_CONFIG")
        .env_remove("OLL_REPLICA")
        .env_remove("OLL_LOG_DIR")
        .args([
            "run",
            "--config",
            "/tmp/oll-config",
            "--replica",
            "/tmp/oll-replica",
            "--log-dir",
            "/tmp/oll-log",
        ])
        .output()
        .unwrap();
    assert_eq!(explicit.status.code(), Some(EXIT_UNAVAILABLE));

    let from_environment = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env("OLL_CONFIG", "/tmp/oll-env-config")
        .env("OLL_REPLICA", "/tmp/oll-env-replica")
        .env("OLL_LOG_DIR", "/tmp/oll-env-log")
        .arg("run")
        .output()
        .unwrap();
    assert_eq!(from_environment.status.code(), Some(EXIT_UNAVAILABLE));

    let init = oll()
        .env_remove("HOME")
        .env_remove("XDG_STATE_HOME")
        .env("OLL_CONFIG", "/tmp/oll-env-config")
        .args([
            "init",
            "test-node",
            "--replica",
            "/tmp/oll-replica",
            "--log-dir",
            "/tmp/oll-log",
        ])
        .output()
        .unwrap();
    assert_eq!(init.status.code(), Some(EXIT_UNAVAILABLE));
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
