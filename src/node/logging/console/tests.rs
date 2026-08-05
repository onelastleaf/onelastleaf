use super::*;

fn record(event: &str, level: &str, fields: Value) -> Value {
    let mut record = serde_json::json!({
        "timestamp": "2026-08-05T08:12:43.169843252Z",
        "level": level,
        "target": "oll::sync",
        "event": event,
        "correlation_id": "foreground-test",
        "node_name": "home-node",
        "node_id": "a8e7865b-32f1-443f-840e-745d2413445d"
    });
    if let (Some(record), Value::Object(fields)) = (record.as_object_mut(), fields) {
        record.extend(fields);
    }
    record
}

fn output(choice: ColorChoice, record: &Value) -> String {
    let stream = AutoStream::new(Vec::new(), choice);
    let mut presenter = ConsolePresenter::new(stream);
    presenter.present(record, Instant::now()).unwrap();
    let bytes = presenter.output.into_inner();
    String::from_utf8(bytes).unwrap()
}

#[test]
fn renders_readable_plain_and_colored_session_failures() {
    let record = record(
        "sync_session_failed",
        "WARN",
        serde_json::json!({
            "connect_target": "oll://peer.example:17384",
            "error_code": "replica_mismatch",
            "message": "peer ReplicaId differs from the local replica"
        }),
    );
    let plain = output(ColorChoice::Never, &record);
    assert!(!plain.contains('\u{1b}'));
    assert!(plain.contains("08:12:43Z  WARN   sync"));
    assert!(plain.contains("sync handshake failed"));
    assert!(plain.contains("peer ReplicaId differs from the local replica"));
    assert!(plain.contains("oll://peer.example:17384"));

    let colored = output(ColorChoice::Always, &record);
    assert!(colored.contains("\u{1b}["));
    assert_eq!(anstream::adapter::strip_str(&colored).to_string(), plain);
}

#[test]
fn dynamic_fields_cannot_inject_lines_or_terminal_sequences() {
    let record = record(
        "sync_session_failed",
        "WARN",
        serde_json::json!({
            "connect_target": "oll://peer.example:17384\nforged",
            "message": "failure\u{1b}[31m\nforged"
        }),
    );
    let rendered = output(ColorChoice::Never, &record);
    assert_eq!(rendered.lines().count(), 1);
    assert!(!rendered.contains('\u{1b}'));
    assert!(rendered.contains(r"failure\u001b[31m\nforged"));
    assert!(rendered.contains(r"oll://peer.example:17384\nforged"));
}

#[test]
fn auto_mode_strips_styles_for_a_redirected_stream() {
    let record = record("sync_session_ready", "INFO", serde_json::json!({}));
    let rendered = output(ColorChoice::Auto, &record);
    if std::env::var_os("CLICOLOR_FORCE").is_none() {
        assert!(!rendered.contains('\u{1b}'));
    }
}

#[test]
fn waiting_for_a_replica_is_a_readable_info_transition() {
    let waiting = record(
        "sync_session_waiting_for_replica",
        "INFO",
        serde_json::json!({
            "remote_node_name": "peer-node",
            "connect_target": "oll://peer.example:17384"
        }),
    );
    let rendered = output(ColorChoice::Never, &waiting);
    assert!(rendered.contains("connected to peer-node; waiting for a replica"));
    assert!(rendered.contains("oll://peer.example:17384"));
    assert!(!rendered.contains("WARN"));
}

#[test]
fn liveness_and_progress_failures_have_actionable_foreground_text() {
    let heartbeat = record(
        "sync_session_liveness_failed",
        "WARN",
        serde_json::json!({
            "error_code": "heartbeat_timeout",
            "failure_stage": "heartbeat_response",
            "idle_ms": 40000
        }),
    );
    let heartbeat = output(ColorChoice::Never, &heartbeat);
    assert!(heartbeat.contains("sync connection became unresponsive"));
    assert!(heartbeat.contains("stage heartbeat_response"));
    assert!(heartbeat.contains("idle_ms 40000"));

    let round = record(
        "sync_round_progress_timeout",
        "WARN",
        serde_json::json!({
            "error_code": "round_progress_timeout",
            "failure_stage": "round_start_receive",
            "idle_ms": 120000
        }),
    );
    let round = output(ColorChoice::Never, &round);
    assert!(round.contains("sync round stopped making progress"));
    assert!(round.contains("stage round_start_receive"));
}

#[test]
fn repeated_failures_are_suppressed_then_summarized_and_success_resets_them() {
    let mut presenter = ConsolePresenter::new(Vec::new());
    let failure = record(
        "sync_session_failed",
        "WARN",
        serde_json::json!({
            "error_code": "replica_mismatch",
            "message": "peer ReplicaId differs from the local replica"
        }),
    );
    let started = Instant::now();
    presenter.present(&failure, started).unwrap();
    presenter
        .present(&failure, started + Duration::from_secs(1))
        .unwrap();
    presenter
        .present(&failure, started + REPEAT_WINDOW)
        .unwrap();
    let ready = record("sync_session_ready", "INFO", serde_json::json!({}));
    presenter
        .present(&ready, started + REPEAT_WINDOW + Duration::from_secs(1))
        .unwrap();
    presenter
        .present(&failure, started + REPEAT_WINDOW + Duration::from_secs(2))
        .unwrap();

    let rendered = String::from_utf8(presenter.output).unwrap();
    assert_eq!(rendered.matches("sync handshake failed").count(), 3);
    assert!(rendered.contains("1 equivalent events suppressed"));
}

#[test]
fn success_for_one_peer_does_not_reset_another_peers_suppression() {
    let mut presenter = ConsolePresenter::new(Vec::new());
    let failure = record(
        "sync_session_failed",
        "WARN",
        serde_json::json!({ "connect_target": "oll://peer-a.example:17384" }),
    );
    let other_ready = record(
        "sync_session_ready",
        "INFO",
        serde_json::json!({ "connect_target": "oll://peer-b.example:17384" }),
    );
    let started = Instant::now();
    presenter.present(&failure, started).unwrap();
    presenter
        .present(&other_ready, started + Duration::from_secs(1))
        .unwrap();
    presenter
        .present(&failure, started + Duration::from_secs(2))
        .unwrap();

    let rendered = String::from_utf8(presenter.output).unwrap();
    assert_eq!(rendered.matches("sync handshake failed").count(), 1);
}

#[test]
fn per_attempt_retry_scaffolding_is_not_presented() {
    let started = record("sync_session_started", "INFO", serde_json::json!({}));
    let retry = record("sync_reconnect_scheduled", "INFO", serde_json::json!({}));
    assert!(output(ColorChoice::Never, &started).is_empty());
    assert!(output(ColorChoice::Never, &retry).is_empty());
}
