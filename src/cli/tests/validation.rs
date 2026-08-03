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
            log: None,
            command: Some(PluginCommand::Install {
                repository: Some("https://example.com/plugin.git".parse().unwrap()),
                rev: Some("v1".to_owned()),
                branch: Some("main".to_owned()),
                release: true,
                source: true,
            }),
        }),
    };
    assert!(install.into_intent().is_err());

    let missing_remote = Cli {
        command: Command::Plugin(PluginArgs {
            log: None,
            command: Some(PluginCommand::Install {
                repository: None,
                rev: None,
                branch: Some("main".to_owned()),
                release: false,
                source: false,
            }),
        }),
    };
    assert!(missing_remote.into_intent().is_err());

    let plugin = Cli {
        command: Command::Plugin(PluginArgs {
            log: Some(None),
            command: Some(PluginCommand::List),
        }),
    };
    assert!(plugin.into_intent().is_err());
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
