use super::*;

#[test]
fn intent_whitelist_rejects_invalid_programmatic_states() {
    let sync = Cli {
        command: Command::Sync(SyncArgs {
            node_name: Some("node-a".parse().unwrap()),
            retries: None,
            log: true,
        }),
    };
    assert!(sync.into_intent().is_err());

    let install = Cli {
        command: Command::Plugin(PluginArgs {
            command: PluginCommand::Install {
                repository: Some("https://example.com/plugin.git".parse().unwrap()),
                rev: Some("v1".to_owned()),
                branch: Some("main".to_owned()),
                release: Some("release-1".to_owned()),
                source: true,
                json: false,
            },
        }),
    };
    assert!(install.into_intent().is_err());

    let missing_remote = Cli {
        command: Command::Plugin(PluginArgs {
            command: PluginCommand::Install {
                repository: None,
                rev: None,
                branch: Some("main".to_owned()),
                release: None,
                source: false,
                json: false,
            },
        }),
    };
    assert!(missing_remote.into_intent().is_err());

    let invalid_limit = Cli {
        command: Command::Job(JobArgs {
            command: JobCommand::List {
                limit: 1001,
                json: false,
            },
        }),
    };
    assert!(invalid_limit.into_intent().is_err());

    let empty_operation_id = Cli {
        command: Command::Plugin(PluginArgs {
            command: PluginCommand::Call {
                operation_id: Some(String::new()),
                json: false,
                selector: "oll.example".to_owned(),
                action: "run".to_owned(),
                arguments: Vec::new(),
            },
        }),
    };
    assert!(empty_operation_id.into_intent().is_err());

    let empty_action = Cli {
        command: Command::Plugin(PluginArgs {
            command: PluginCommand::Call {
                operation_id: None,
                json: false,
                selector: "oll.example".to_owned(),
                action: String::new(),
                arguments: Vec::new(),
            },
        }),
    };
    assert!(empty_action.into_intent().is_err());
}

#[test]
fn rejects_conflicting_modes() {
    assert!(parse_from(["oll", "sync", "node-a", "--log"]).is_err());
    assert!(
        parse_from([
            "oll",
            "plugin",
            "install",
            "https://example.com/plugin.git",
            "--release",
            "release-1",
            "--source",
        ])
        .is_err()
    );
    assert!(
        parse_from([
            "oll",
            "plugin",
            "install",
            "https://example.com/plugin.git",
            "--rev",
            "v1",
            "--branch",
            "main",
        ])
        .is_err()
    );
    assert!(parse_from(["oll", "plugin"]).is_err());
    assert!(parse_from(["oll", "plugin", "install", "--release"]).is_err());
    assert!(parse_from(["oll", "plugin", "install", "--branch", "main"]).is_err());
}
