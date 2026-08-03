use super::*;

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
