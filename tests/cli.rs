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
fn clap_errors_use_exit_code_two() {
    for arguments in [
        vec!["init", "--profile", "authority"],
        vec!["run", "--listen", "not-an-address"],
        vec!["run", "--connect", "ftp://example.com"],
        vec!["replica", "ops", "/note.md", "--limit", "0"],
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
        .args(["init", "--replica"])
        .arg(&replica)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    assert!(!replica.exists());
    assert!(!temporary.exists());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("replica stage")
    );
}

#[test]
fn explicit_and_environment_paths_do_not_require_home() {
    let explicit = oll()
        .env_remove("HOME")
        .env_remove("OLL_CONFIG")
        .args(["run", "--config", "/tmp/oll-config.lua"])
        .output()
        .unwrap();
    assert_eq!(explicit.status.code(), Some(EXIT_UNAVAILABLE));

    let from_environment = oll()
        .env_remove("HOME")
        .env("OLL_CONFIG", "/tmp/oll-env-config.lua")
        .arg("run")
        .output()
        .unwrap();
    assert_eq!(from_environment.status.code(), Some(EXIT_UNAVAILABLE));

    let init = oll()
        .env_remove("HOME")
        .env("OLL_CONFIG", "/tmp/oll-env-config.lua")
        .args(["init", "--replica", "/tmp/oll-replica"])
        .output()
        .unwrap();
    assert_eq!(init.status.code(), Some(EXIT_UNAVAILABLE));
}

#[test]
fn missing_home_is_a_configuration_error_when_default_is_needed() {
    let output = oll()
        .env_remove("HOME")
        .env_remove("OLL_CONFIG")
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
fn operational_command_reports_its_required_stage() {
    let output = oll().args(["sync", "node-a", "-n", "3"]).output().unwrap();
    assert_eq!(output.status.code(), Some(EXIT_UNAVAILABLE));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("sync stage")
    );
}
