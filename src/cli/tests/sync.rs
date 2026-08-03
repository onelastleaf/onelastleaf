use super::*;

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
