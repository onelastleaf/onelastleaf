use super::*;

#[test]
fn parses_documented_plugin_and_job_commands() {
    for arguments in [
        vec!["oll", "plugin", "install"],
        vec!["oll", "plugin", "install", "--json"],
        vec![
            "oll",
            "plugin",
            "install",
            "https://github.com/example/oll-anki.git",
        ],
        vec![
            "oll",
            "plugin",
            "install",
            "git@github.com:example/oll-anki.git",
            "--rev",
            "0123456789abcdef",
        ],
        vec![
            "oll",
            "plugin",
            "install",
            "https://github.com/example/oll-anki.git",
            "--branch",
            "main",
            "--source",
        ],
        vec![
            "oll",
            "plugin",
            "install",
            "https://github.com/example/oll-anki.git",
            "--branch",
            "releases",
            "--release",
            "v0.3.1",
            "--json",
        ],
        vec!["oll", "plugin", "reconcile"],
        vec!["oll", "plugin", "reconcile", "--json"],
        vec!["oll", "plugin", "validate"],
        vec!["oll", "plugin", "list"],
        vec!["oll", "plugin", "list", "--json"],
        vec!["oll", "plugin", "info", "oll.anki", "--json"],
        vec!["oll", "plugin", "releases", "oll.anki", "--json"],
        vec!["oll", "plugin", "start", "oll.anki"],
        vec!["oll", "plugin", "stop", "oll.anki"],
        vec!["oll", "plugin", "restart", "oll.anki"],
        vec!["oll", "plugin", "update", "oll.anki", "--json"],
        vec!["oll", "plugin", "remove", "oll.anki", "--json"],
        vec!["oll", "plugin", "log"],
        vec!["oll", "plugin", "log", "oll.anki"],
        vec![
            "oll",
            "plugin",
            "call",
            "--operation-id",
            "operation-1",
            "--json",
            "oll.anki",
            "create",
            "--",
            "--deck",
            "default",
        ],
        vec!["oll", "job", "list"],
        vec!["oll", "job", "list", "--limit", "1000", "--json"],
        vec!["oll", "job", "info", "job-1", "--json"],
        vec!["oll", "job", "stop", "job-1"],
    ] {
        parse(&arguments);
    }
}

#[test]
fn preserves_plugin_action_argv_and_admission_options() {
    let call_intent = intent(&[
        "oll",
        "plugin",
        "call",
        "--operation-id",
        "operation-1",
        "--json",
        "oll.example",
        "publish",
        "--",
        "",
        "--flag",
        "--flag",
        "value",
    ]);
    let CliIntent::Plugin(PluginIntent::Call {
        selector,
        action,
        arguments,
        operation_id,
        json,
    }) = call_intent
    else {
        panic!()
    };
    assert_eq!(selector, "oll.example");
    assert_eq!(action, "publish");
    assert_eq!(arguments, vec!["", "--flag", "--flag", "value"]);
    assert_eq!(operation_id.as_deref(), Some("operation-1"));
    assert!(json);

    let health_intent = intent(&["oll", "plugin", "call", "oll.example", "health"]);
    let CliIntent::Plugin(PluginIntent::Call {
        arguments,
        operation_id,
        json,
        ..
    }) = health_intent
    else {
        panic!()
    };
    assert!(arguments.is_empty());
    assert!(operation_id.is_none());
    assert!(!json);
}

#[test]
fn converts_plugin_modes_and_local_log_intents() {
    assert_eq!(
        intent(&["oll", "plugin", "log"]),
        CliIntent::Plugin(PluginIntent::ViewLog {
            target: PluginLogTarget::All,
        })
    );
    assert_eq!(
        intent(&["oll", "plugin", "log", "oll.example"]),
        CliIntent::Plugin(PluginIntent::ViewLog {
            target: PluginLogTarget::Plugin("oll.example".to_owned()),
        })
    );

    assert_eq!(
        intent(&["oll", "plugin", "install", "--json"]),
        CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Declared {
            json: true,
        }))
    );

    let CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Remote {
        remote,
        selector,
        mode,
        json,
    })) = intent(&[
        "oll",
        "plugin",
        "install",
        "git@example.com:plugins/example.git",
        "--branch",
        "releases",
        "--release",
        "v0.3.1",
        "--json",
    ])
    else {
        panic!()
    };
    assert_eq!(remote.as_str(), "git@example.com:plugins/example.git");
    assert_eq!(selector, GitSelector::Branch("releases".to_owned()));
    assert_eq!(
        mode,
        PluginInstallMode::Release {
            release_id: "v0.3.1".to_owned(),
        }
    );
    assert!(json);

    let CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Remote {
        selector,
        mode,
        ..
    })) = intent(&["oll", "plugin", "install", "https://example.com/plugin.git"])
    else {
        panic!()
    };
    assert_eq!(selector, GitSelector::Default);
    assert_eq!(mode, PluginInstallMode::Source);
}

#[test]
fn job_list_defaults_and_bounds_are_part_of_the_intent() {
    assert_eq!(
        intent(&["oll", "job", "list"]),
        CliIntent::Job(JobIntent::List {
            limit: 100,
            json: false,
        })
    );
    assert_eq!(
        intent(&["oll", "job", "list", "--limit", "1000", "--json"]),
        CliIntent::Job(JobIntent::List {
            limit: 1000,
            json: true,
        })
    );
    assert!(parse_from(["oll", "job", "list", "--limit", "0"]).is_err());
    assert!(parse_from(["oll", "job", "list", "--limit", "1001"]).is_err());
}

#[test]
fn parses_git_remotes_without_treating_them_as_web_urls() {
    for remote in [
        "https://github.com/example/plugin.git",
        "http://git.example.com/example/plugin.git",
        "ssh://git@gitlab.com/example/plugin.git",
        "git@gitlab.com:example/plugin.git",
        "git://codeberg.org/example/plugin.git",
    ] {
        let parsed: GitRemote = remote.parse().unwrap();
        assert_eq!(parsed.as_str(), remote);
    }

    for invalid in [
        "",
        "https://github.com",
        "git@github.com:",
        "ftp://example.com/plugin.git",
        "../local-plugin",
    ] {
        assert!(
            invalid.parse::<GitRemote>().is_err(),
            "accepted {invalid:?}"
        );
    }

    let credentialed: GitRemote = "https://user:secret@example.com/plugin.git"
        .parse()
        .unwrap();
    assert!(!credentialed.to_string().contains("user"));
    assert!(!credentialed.to_string().contains("secret"));
    assert!(!format!("{credentialed:?}").contains("secret"));

    let username_token: GitRemote = "https://username-token@example.com/plugin.git"
        .parse()
        .unwrap();
    assert!(!username_token.to_string().contains("username-token"));
    assert!(!format!("{username_token:?}").contains("username-token"));

    let ssh_user: GitRemote = "git@gitlab.com:example/plugin.git".parse().unwrap();
    assert_eq!(ssh_user.to_string(), "git@gitlab.com:example/plugin.git");

    let malformed_secret = "https://username:secret@[invalid/plugin.git";
    let error = malformed_secret.parse::<GitRemote>().unwrap_err();
    assert!(!error.contains("username"));
    assert!(!error.contains("secret"));
}
