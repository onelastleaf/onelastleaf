use super::*;

#[test]
fn clap_schema_is_internally_consistent() {
    Cli::command().debug_assert();
}

#[test]
fn psk_is_a_dependency_free_local_intent() {
    let prepared = intent(&["oll", "psk"])
        .prepare(&Environment::default(), Path::new("/working"))
        .unwrap();
    let PreparedCliIntent::Client(prepared) = prepared else {
        panic!()
    };
    assert!(matches!(prepared.intent, CliIntent::Psk));
    assert_eq!(prepared.dependency, ClientDependency::None);
}

#[test]
fn resolves_path_precedence() {
    let environment = Environment {
        home: Some("/home/test".into()),
        state_home: Some("/state/test".into()),
        platform_config_root: None,
        platform_data_dir: None,
        platform_documents_dir: None,
        platform_downloads_dir: None,
        platform_state_dir: None,
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
    assert_eq!(run.color, ColorMode::Auto);
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

    let CliIntent::Run(run) = intent(&["oll", "run", "--color", "never"]) else {
        panic!()
    };
    assert_eq!(run.color, ColorMode::Never);
    assert!(parse_from(["oll", "run", "--color", "sometimes"]).is_err());
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
            "oll://oll.example.com:17384",
        ],
        vec!["oll", "run"],
        vec!["oll", "run", "--replica", "/path/to/replica/root"],
        vec!["oll", "run", "--config", "/path/to/config/root"],
        vec!["oll", "run", "--log-dir", "/path/to/log/dir"],
        vec!["oll", "run", "--listen", "127.0.0.1:7443"],
        vec!["oll", "run", "--connect", "oll://oll.example.com:17384"],
        vec![
            "oll",
            "run",
            "--listen",
            "127.0.0.1:7443",
            "--connect",
            "oll://oll.example.com:17384",
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
        "oll://oll.example.com:17384",
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
        "oll://node-a.example.com:17384",
        "--connect",
        "oll://node-b.example.com:17384",
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
