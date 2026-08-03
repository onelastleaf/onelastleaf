use super::*;

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
        "oll://cli.example.com:17384",
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
        "oll://cli.example.com:17384"
    );
}

#[test]
fn run_overrides_apply_after_configuration_load() {
    let mut config = ResolvedNodeConfig {
        replica_root: PathBuf::from("/persisted/replica"),
        replica_store: ReplicaStoreConfig::Sqlite {
            path: PathBuf::from("/persisted/store.sqlite3"),
        },
        log_dir: PathBuf::from("/persisted/log"),
        listen: Some("127.0.0.1:7000".parse().unwrap()),
        connect: vec!["oll://persisted.example.com:17384".parse().unwrap()],
        network_key: None,
    };
    let overrides = RunOverrides {
        replica_root: Some(PathBuf::from("/override/replica")),
        log_dir: Some(PathBuf::from("/override/log")),
        listen: Some("127.0.0.1:8000".parse().unwrap()),
        connect: Some(vec!["oll://override.example.com:17384".parse().unwrap()]),
    };

    overrides.apply_to(&mut config);

    assert_eq!(config.replica_root, Path::new("/override/replica"));
    assert_eq!(config.log_dir, Path::new("/override/log"));
    assert_eq!(config.listen, Some("127.0.0.1:8000".parse().unwrap()));
    assert_eq!(
        config.connect[0].to_string(),
        "oll://override.example.com:17384"
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
    .prepare(
        &Environment {
            platform_data_dir: Some(cwd.join("platform/data/oll")),
            ..Environment::default()
        },
        cwd,
    )
    .unwrap();
    let PreparedCliIntent::Init(prepared) = prepared else {
        panic!()
    };
    assert_eq!(prepared.config_root, cwd.join("deployment/config"));
    assert_eq!(prepared.replica_root, cwd.join("deployment/replica"));
    assert_eq!(prepared.replica_store_base, cwd.join("platform/data/oll"));
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
