use std::{
    env,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::CommandFactory;

use crate::configuration::ResolvedNodeConfig;

use super::*;

fn parse(arguments: &[&str]) -> Cli {
    parse_from(arguments).unwrap()
}

fn intent(arguments: &[&str]) -> CliIntent {
    parse(arguments).into_intent().unwrap()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            env::temp_dir().join(format!("oll-cli-unit-test-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn write_config(&self, source: &str) -> PathBuf {
        let root = self.0.join("config");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("config.lua"), source).unwrap();
        root
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn clap_schema_is_internally_consistent() {
    Cli::command().debug_assert();
}

#[test]
fn resolves_path_precedence() {
    let environment = Environment {
        home: Some("/home/test".into()),
        state_home: Some("/state/test".into()),
        config: Some("/env/config".into()),
        replica: Some("/env/replica".into()),
        log_dir: Some("/env/log".into()),
    };
    let CliIntent::Init(init) = intent(&[
        "oll",
        "init",
        "test-node",
        "--replica",
        "/cli/replica",
        "--config",
        "/cli/config",
        "--log-dir",
        "/cli/log",
    ]) else {
        panic!()
    };

    assert_eq!(
        init.replica_root(&environment).unwrap(),
        Path::new("/cli/replica")
    );
    assert_eq!(
        init.config_root(&environment).unwrap(),
        Path::new("/cli/config")
    );
    assert_eq!(init.log_dir(&environment).unwrap(), Path::new("/cli/log"));

    let CliIntent::Run(run) = intent(&["oll", "run"]) else {
        panic!()
    };
    assert_eq!(
        run.config_root(&environment).unwrap(),
        Path::new("/env/config")
    );

    let environment = Environment {
        home: Some("/home/test".into()),
        state_home: Some("/state/test".into()),
        ..Environment::default()
    };
    assert_eq!(
        run.config_root(&environment).unwrap(),
        Path::new("/home/test/.config/oll")
    );
}

#[test]
fn parses_init_and_run_topologies() {
    for arguments in [
        vec!["oll", "init", "test-node"],
        vec![
            "oll",
            "init",
            "test-node",
            "--replica",
            "/path/to/replica/root",
        ],
        vec![
            "oll",
            "init",
            "test-node",
            "--config",
            "/path/to/config/root",
        ],
        vec!["oll", "init", "test-node", "--log-dir", "/path/to/log/dir"],
        vec![
            "oll",
            "init",
            "test-node",
            "--listen",
            "127.0.0.1:7443",
            "--connect",
            "https://oll.example.com",
        ],
        vec!["oll", "run"],
        vec!["oll", "run", "--replica", "/path/to/replica/root"],
        vec!["oll", "run", "--config", "/path/to/config/root"],
        vec!["oll", "run", "--log-dir", "/path/to/log/dir"],
        vec!["oll", "run", "--listen", "127.0.0.1:7443"],
        vec!["oll", "run", "--connect", "https://oll.example.com"],
        vec![
            "oll",
            "run",
            "--listen",
            "127.0.0.1:7443",
            "--connect",
            "https://oll.example.com",
        ],
        vec!["oll", "start"],
        vec!["oll", "stop"],
        vec!["oll", "status"],
        vec!["oll", "status", "--json"],
    ] {
        parse(&arguments);
    }

    let cli = parse(&[
        "oll",
        "init",
        "test-node",
        "--connect",
        "https://oll.example.com",
        "--listen",
        "127.0.0.1:7443",
    ]);
    let Command::Init(args) = cli.command else {
        panic!()
    };
    assert_eq!(args.node_name.as_str(), "test-node");
    assert_eq!(args.connect.len(), 1);
    assert_eq!(args.listen, Some("127.0.0.1:7443".parse().unwrap()));

    let cli = parse(&[
        "oll",
        "run",
        "--listen",
        "127.0.0.1:7443",
        "--connect",
        "https://node-a.example.com",
        "--connect",
        "https://node-b.example.com",
    ]);
    let Command::Run(args) = cli.command else {
        panic!()
    };
    assert_eq!(args.connect.len(), 2);
    assert!(parse_from(["oll", "init", "test-node", "--profile", "client"]).is_err());
}

#[test]
fn validates_node_names_as_lowercase_dns_labels() {
    let name: NodeName = "home-server-2".parse().unwrap();
    assert_eq!(name.as_str(), "home-server-2");
    assert_eq!(name.to_string(), "home-server-2");

    for invalid in [
        "",
        "Home",
        "-home",
        "home-",
        "home_server",
        "home.example",
        "node name",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        assert!(invalid.parse::<NodeName>().is_err(), "accepted {invalid:?}");
    }

    assert!(parse_from(["oll", "init"]).is_err());
    assert!(parse_from(["oll", "init", "Home"]).is_err());
    assert!(parse_from(["oll", "sync", "home.example"]).is_err());
    assert!(parse_from(["oll", "ping", "node name"]).is_err());
}

#[test]
fn parses_replica_commands() {
    for arguments in [
        vec!["oll", "replica", "inspect", "/note.md"],
        vec!["oll", "replica", "ops", "/note.md", "--limit", "50"],
        vec!["oll", "replica", "ops", "/note.md", "--format", "json"],
        vec!["oll", "replica", "export", "-o", "replica.snapshot"],
        vec!["oll", "replica", "import", "replica.snapshot"],
        vec![
            "oll",
            "replica",
            "snapshot",
            "inspect",
            "replica.snapshot",
            "--json",
        ],
        vec!["oll", "replica", "snapshot", "verify", "replica.snapshot"],
    ] {
        parse(&arguments);
    }
}

#[test]
fn resolves_replica_paths_against_client_cwd_without_canonicalizing() {
    let mut intent = parse(&[
        "oll",
        "replica",
        "snapshot",
        "inspect",
        "nested/../backup.ollsnap",
        "--json",
    ])
    .into_intent()
    .unwrap();
    intent.resolve_client_paths(Path::new("/client/work"));
    let CliIntent::Replica(ReplicaIntent::SnapshotInspect { snapshot, .. }) = intent else {
        panic!()
    };
    assert_eq!(
        snapshot,
        PathBuf::from("/client/work/nested/../backup.ollsnap")
    );

    let mut intent = parse(&["oll", "replica", "inspect", "/replica/../note.md"])
        .into_intent()
        .unwrap();
    intent.resolve_client_paths(Path::new("/different/client"));
    let CliIntent::Replica(ReplicaIntent::Inspect { document }) = intent else {
        panic!()
    };
    assert_eq!(document, PathBuf::from("/replica/../note.md"));
}

#[test]
fn resolves_every_snapshot_and_document_path_kind() {
    for (arguments, expected) in [
        (
            vec!["oll", "replica", "inspect", "note.md"],
            PathBuf::from("/client/note.md"),
        ),
        (
            vec!["oll", "replica", "ops", "history.md"],
            PathBuf::from("/client/history.md"),
        ),
        (
            vec!["oll", "replica", "export", "-o", "out.ollsnap"],
            PathBuf::from("/client/out.ollsnap"),
        ),
        (
            vec!["oll", "replica", "import", "in.ollsnap"],
            PathBuf::from("/client/in.ollsnap"),
        ),
        (
            vec!["oll", "replica", "snapshot", "verify", "verify.ollsnap"],
            PathBuf::from("/client/verify.ollsnap"),
        ),
    ] {
        let mut intent = parse(&arguments).into_intent().unwrap();
        intent.resolve_client_paths(Path::new("/client"));
        let CliIntent::Replica(intent) = intent else {
            panic!()
        };
        let path = match intent {
            ReplicaIntent::Inspect { document } | ReplicaIntent::Ops { document, .. } => document,
            ReplicaIntent::Export { output } => output,
            ReplicaIntent::Import { snapshot }
            | ReplicaIntent::SnapshotInspect { snapshot, .. }
            | ReplicaIntent::SnapshotVerify { snapshot } => snapshot,
        };
        assert_eq!(path, expected);
    }
}

#[test]
fn parses_sync_commands() {
    for arguments in [
        vec!["oll", "sync"],
        vec!["oll", "sync", "node-a"],
        vec!["oll", "sync", "-n", "3"],
        vec!["oll", "sync", "node-a", "-n", "3"],
        vec!["oll", "sync", "--log"],
        vec!["oll", "ping", "node-a"],
    ] {
        parse(&arguments);
    }

    let cli = parse(&["oll", "sync", "node-a", "-n", "3"]);
    let Command::Sync(args) = cli.command else {
        panic!()
    };
    assert_eq!(args.node_name.unwrap().as_str(), "node-a");

    let cli = parse(&["oll", "ping", "node-a"]);
    let Command::Ping(args) = cli.command else {
        panic!()
    };
    assert_eq!(args.node_name.as_str(), "node-a");
}

#[test]
fn converts_sync_modes_to_distinct_intents() {
    assert_eq!(
        intent(&["oll", "sync", "--log"]),
        CliIntent::Sync(SyncIntent::ViewLog)
    );

    let CliIntent::Sync(SyncIntent::Synchronize {
        node_name,
        max_attempts,
    }) = intent(&["oll", "sync", "node-a", "--retries", "3"])
    else {
        panic!()
    };
    assert_eq!(node_name.unwrap().as_str(), "node-a");
    assert_eq!(max_attempts.unwrap().get(), 3);
}

#[test]
fn parses_log_filter_directives_into_typed_intents() {
    let CliIntent::Log(LogIntent::Set { target, level }) =
        intent(&["oll", "log", "set", "oll::sync=trace"])
    else {
        panic!()
    };
    assert_eq!(target.as_str(), "oll::sync");
    assert_eq!(level, LogFilterLevel::Trace);

    for directive in [
        "sync=trace",
        "oll:sync=trace",
        "oll::=trace",
        "oll::sync=Trace",
        "oll::sync=trace=debug",
        "oll::sync",
    ] {
        assert!(
            parse_from(["oll", "log", "set", directive]).is_err(),
            "accepted {directive:?}"
        );
    }
}

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
fn prepares_run_overrides_without_evaluating_config() {
    let temporary = TestDirectory::new();
    let config_root = temporary.write_config(
        r#"
            error("run preparation must not evaluate config.lua")
            "#,
    );
    let cwd = temporary.0.join("working");
    std::fs::create_dir_all(&cwd).unwrap();
    let environment = Environment {
        config: Some(config_root.clone()),
        replica: Some("environment/replica".into()),
        log_dir: Some("environment/log".into()),
        ..Environment::default()
    };

    let prepared = intent(&["oll", "run", "--replica", "cli/replica"])
        .prepare(&environment, &cwd)
        .unwrap();
    let PreparedCliIntent::Run(prepared) = prepared else {
        panic!()
    };
    assert_eq!(prepared.config_root, config_root);
    assert_eq!(
        prepared.overrides.replica_root,
        Some(cwd.join("cli/replica"))
    );
    assert_eq!(
        prepared.overrides.log_dir,
        Some(cwd.join("environment/log"))
    );
    assert_eq!(prepared.overrides.listen, None);
    assert_eq!(prepared.overrides.connect, None);

    let prepared = intent(&[
        "oll",
        "run",
        "--log-dir",
        "cli/log",
        "--listen",
        "127.0.0.1:8000",
        "--connect",
        "https://cli.example.com",
    ])
    .prepare(&environment, &cwd)
    .unwrap();
    let PreparedCliIntent::Run(prepared) = prepared else {
        panic!()
    };
    assert_eq!(
        prepared.overrides.replica_root,
        Some(cwd.join("environment/replica"))
    );
    assert_eq!(prepared.overrides.log_dir, Some(cwd.join("cli/log")));
    assert_eq!(
        prepared.overrides.listen,
        Some("127.0.0.1:8000".parse().unwrap())
    );
    assert_eq!(
        prepared.overrides.connect.unwrap()[0].to_string(),
        "https://cli.example.com/"
    );
}

#[test]
fn run_overrides_apply_after_configuration_load() {
    let mut config = ResolvedNodeConfig {
        replica_root: PathBuf::from("/persisted/replica"),
        log_dir: PathBuf::from("/persisted/log"),
        listen: Some("127.0.0.1:7000".parse().unwrap()),
        connect: vec!["https://persisted.example.com".parse().unwrap()],
    };
    let overrides = RunOverrides {
        replica_root: Some(PathBuf::from("/override/replica")),
        log_dir: Some(PathBuf::from("/override/log")),
        listen: Some("127.0.0.1:8000".parse().unwrap()),
        connect: Some(vec!["https://override.example.com".parse().unwrap()]),
    };

    overrides.apply_to(&mut config);

    assert_eq!(config.replica_root, Path::new("/override/replica"));
    assert_eq!(config.log_dir, Path::new("/override/log"));
    assert_eq!(config.listen, Some("127.0.0.1:8000".parse().unwrap()));
    assert_eq!(
        config.connect[0].to_string(),
        "https://override.example.com/"
    );
}

#[test]
fn home_less_run_with_absolute_config_needs_no_other_roots() {
    let config_root = PathBuf::from("/absolute/config");
    let environment = Environment {
        config: Some(config_root.clone()),
        ..Environment::default()
    };

    let prepared = intent(&["oll", "run"])
        .prepare(&environment, Path::new("/unrelated-daemon-cwd"))
        .unwrap();
    let PreparedCliIntent::Run(prepared) = prepared else {
        panic!()
    };
    assert_eq!(prepared.config_root, config_root);
    assert_eq!(prepared.overrides.replica_root, None);
    assert_eq!(prepared.overrides.log_dir, None);
}

#[test]
fn preparation_resolves_only_each_intents_required_resources() {
    let environment = Environment {
        config: Some("relative/config".into()),
        log_dir: Some("relative/log".into()),
        ..Environment::default()
    };
    let cwd = Path::new("/client/cwd");

    let snapshot = intent(&["oll", "replica", "snapshot", "verify", "file.ollsnap"])
        .prepare(&Environment::default(), cwd)
        .unwrap();
    let PreparedCliIntent::Client(snapshot) = snapshot else {
        panic!()
    };
    assert_eq!(snapshot.dependency, ClientDependency::None);
    let CliIntent::Replica(ReplicaIntent::SnapshotVerify { snapshot }) = snapshot.intent else {
        panic!()
    };
    assert_eq!(snapshot, cwd.join("file.ollsnap"));

    let log = intent(&["oll", "sync", "--log"])
        .prepare(&environment, cwd)
        .unwrap();
    let PreparedCliIntent::Client(log) = log else {
        panic!()
    };
    assert_eq!(
        log.dependency,
        ClientDependency::LogDir(cwd.join("relative/log"))
    );

    let admin = intent(&["oll", "status"])
        .prepare(&environment, cwd)
        .unwrap();
    let PreparedCliIntent::Client(admin) = admin else {
        panic!()
    };
    assert_eq!(
        admin.dependency,
        ClientDependency::ConfigRoot(cwd.join("relative/config"))
    );

    let log_set = intent(&["oll", "log", "set", "oll::sync=debug"])
        .prepare(&environment, cwd)
        .unwrap();
    let PreparedCliIntent::Client(log_set) = log_set else {
        panic!()
    };
    assert_eq!(
        log_set.dependency,
        ClientDependency::ConfigRoot(cwd.join("relative/config"))
    );
    assert!(matches!(
        log_set.intent,
        CliIntent::Log(LogIntent::Set { .. })
    ));
}

#[test]
fn init_preparation_makes_persisted_roots_absolute_from_startup_cwd() {
    let cwd = Path::new("/startup/cwd");
    let prepared = intent(&[
        "oll",
        "init",
        "test-node",
        "--config",
        "deployment/config",
        "--replica",
        "deployment/replica",
        "--log-dir",
        "deployment/log",
    ])
    .prepare(&Environment::default(), cwd)
    .unwrap();
    let PreparedCliIntent::Init(prepared) = prepared else {
        panic!()
    };
    assert_eq!(prepared.config_root, cwd.join("deployment/config"));
    assert_eq!(prepared.replica_root, cwd.join("deployment/replica"));
    assert_eq!(prepared.log_dir, cwd.join("deployment/log"));
}

#[test]
fn replica_import_requires_backup_and_replacement_confirmations() {
    let import_intent = intent(&["oll", "replica", "import", "file.ollsnap"]);
    assert_eq!(
        import_intent.confirmation_requirements(),
        [
            ConfirmationRequirement::ReplicaBackupCreated,
            ConfirmationRequirement::ReplicaReplacementApproved,
        ]
    );
    assert_eq!(
        import_intent.confirmation_requirements()[0].prompt(),
        "Have you exported the current replica to a backup snapshot?"
    );
    assert_eq!(
        import_intent.confirmation_requirements()[1].prompt(),
        "Import replaces the entire current replica. Continue?"
    );
    assert!(
        intent(&["oll", "replica", "snapshot", "verify", "file.ollsnap"])
            .confirmation_requirements()
            .is_empty()
    );
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
