use super::*;

#[test]
fn parses_plugin_and_job_commands() {
    for arguments in [
        vec!["oll", "plugin", "install"],
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
        ],
        vec![
            "oll",
            "plugin",
            "install",
            "https://github.com/example/oll-anki.git",
            "--rev",
            "v0.3.1",
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
            "release/v0.3.1",
            "--release",
        ],
        vec![
            "oll",
            "plugin",
            "install",
            "https://github.com/example/oll-anki.git",
            "--source",
        ],
        vec!["oll", "plugin", "validate"],
        vec!["oll", "plugin", "list"],
        vec!["oll", "plugin", "info", "oll.anki"],
        vec!["oll", "plugin", "start", "oll.anki"],
        vec!["oll", "plugin", "stop", "oll.anki"],
        vec!["oll", "plugin", "restart", "oll.anki"],
        vec!["oll", "plugin", "update", "oll.anki"],
        vec!["oll", "plugin", "remove", "oll.anki"],
        vec!["oll", "plugin", "--log"],
        vec!["oll", "plugin", "--log", "oll.anki"],
        vec![
            "oll", "plugin", "call", "oll.anki", "create", "--deck", "default",
        ],
        vec!["oll", "job", "list"],
        vec!["oll", "job", "info", "job-1"],
        vec!["oll", "job", "stop", "job-1"],
    ] {
        parse(&arguments);
    }
}

#[test]
fn preserves_plugin_action_argv_verbatim() {
    let call_intent = intent(&[
        "oll",
        "plugin",
        "call",
        "oll.example",
        "publish",
        "",
        "--flag",
        "--flag",
        "value",
    ]);
    let CliIntent::Plugin(PluginIntent::Call {
        plugin_id,
        action,
        arguments,
    }) = call_intent
    else {
        panic!()
    };
    assert_eq!(plugin_id, "oll.example");
    assert_eq!(action, "publish");
    assert_eq!(arguments, vec!["", "--flag", "--flag", "value"]);

    let health_intent = intent(&["oll", "plugin", "call", "oll.example", "health"]);
    let CliIntent::Plugin(PluginIntent::Call { arguments, .. }) = health_intent else {
        panic!()
    };
    assert!(arguments.is_empty());
}

#[test]
fn converts_plugin_modes_without_sentinels_or_boolean_state() {
    assert_eq!(
        intent(&["oll", "plugin", "--log"]),
        CliIntent::Plugin(PluginIntent::ViewLog {
            target: PluginLogTarget::All,
        })
    );
    assert_eq!(
        intent(&["oll", "plugin", "--log", "__all__"]),
        CliIntent::Plugin(PluginIntent::ViewLog {
            target: PluginLogTarget::Plugin("__all__".to_owned()),
        })
    );

    assert_eq!(
        intent(&["oll", "plugin", "install"]),
        CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Declared))
    );

    let CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Release { remote, selector })) =
        intent(&[
            "oll",
            "plugin",
            "install",
            "git@example.com:plugins/example.git",
            "--branch",
            "release/v0.3.1",
            "--release",
        ])
    else {
        panic!()
    };
    assert_eq!(remote.as_str(), "git@example.com:plugins/example.git");
    assert_eq!(selector, GitSelector::Branch("release/v0.3.1".to_owned()));

    let CliIntent::Plugin(PluginIntent::Install(PluginInstallIntent::Source { selector, .. })) =
        intent(&["oll", "plugin", "install", "https://example.com/plugin.git"])
    else {
        panic!()
    };
    assert_eq!(selector, GitSelector::Default);
    assert_eq!(
        intent(&["oll", "plugin", "validate"]),
        CliIntent::Plugin(PluginIntent::Validate)
    );
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
    assert!(!credentialed.to_string().contains("secret"));
    assert!(!format!("{credentialed:?}").contains("secret"));
}
