use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{
    LOG_QUEUE_CAPACITY, LogLevel, MEBIBYTE, NodeLogger, OLL_LOG_FILENAME, PLUGIN_LOG_FILENAME,
    PLUGIN_ROTATION, SYNC_LOG_FILENAME,
};
use super::{
    files::{format_timestamp, rotated_log_files},
    sink::{CompressionWorker, RotatingLogSink, RotationPolicy},
};
use crate::node::{identity::NodeIdentity, runtime::NodeError};

#[test]
fn creates_valid_jsonl_logs_with_correlation_ids() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "node_started",
        "corr-1",
        json!({ "process_id": 42 }),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();

    let source = fs::read_to_string(logs.join(OLL_LOG_FILENAME)).unwrap();
    let record: Value = serde_json::from_str(source.trim()).unwrap();
    assert_eq!(record["event"], "node_started");
    assert_eq!(record["correlation_id"], "corr-1");
    assert_eq!(record["process_id"], 42);
    assert_eq!(
        fs::metadata(&logs).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for filename in [OLL_LOG_FILENAME, SYNC_LOG_FILENAME, PLUGIN_LOG_FILENAME] {
        assert_eq!(
            fs::metadata(logs.join(filename))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "unsafe permissions on {filename}"
        );
    }
}

#[test]
fn plugin_output_has_receive_time_and_stays_out_of_the_lifecycle_log() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    let plugin_timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let before_receive = OffsetDateTime::now_utc();

    logger.emit_plugin(
        LogLevel::Info,
        "plugin::oll_anki",
        "plugin_log_record",
        "corr-plugin",
        plugin_timestamp,
        json!({
            "plugin_id": "b4f2b080-6813-4ef0-ab48-e1f5c42e1d67",
            "plugin_name": "oll.anki",
            "message": "ready",
            "timestamp": "forged",
            "observed_at": "forged",
        }),
    );
    let after_receive = OffsetDateTime::now_utc();
    logger.emit(
        LogLevel::Info,
        "oll::plugin",
        "plugin_process_ready",
        "corr-lifecycle",
        json!({ "plugin_name": "oll.anki" }),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();

    let plugin_source = fs::read_to_string(logs.join(PLUGIN_LOG_FILENAME)).unwrap();
    let plugin_record: Value = serde_json::from_str(plugin_source.trim()).unwrap();
    assert_eq!(
        plugin_record["timestamp"],
        format_timestamp(plugin_timestamp)
    );
    let observed_at =
        OffsetDateTime::parse(plugin_record["observed_at"].as_str().unwrap(), &Rfc3339).unwrap();
    assert!(observed_at >= before_receive);
    assert!(observed_at <= after_receive);
    assert_eq!(plugin_record["correlation_id"], "corr-plugin");
    assert_eq!(plugin_record["plugin_name"], "oll.anki");
    assert_eq!(plugin_record["message"], "ready");

    let lifecycle_source = fs::read_to_string(logs.join(OLL_LOG_FILENAME)).unwrap();
    assert!(lifecycle_source.contains("plugin_process_ready"));
    assert!(lifecycle_source.contains("corr-lifecycle"));
    assert!(!lifecycle_source.contains("corr-plugin"));
    assert!(!plugin_source.contains("corr-lifecycle"));
    assert!(
        fs::read_to_string(logs.join(SYNC_LOG_FILENAME))
            .unwrap()
            .is_empty()
    );
    assert!(
        rotated_log_files(&logs, PLUGIN_LOG_FILENAME)
            .unwrap()
            .is_empty(),
        "a stale plugin timestamp must not rotate the host sink"
    );
}

#[test]
fn plugin_output_uses_the_shared_target_filter_and_documented_rotation_policy() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    let timestamp = OffsetDateTime::now_utc();

    logger.emit_plugin(
        LogLevel::Trace,
        "plugin::oll_pdf",
        "plugin_trace",
        "corr-filtered",
        timestamp,
        json!({}),
    );
    logger
        .set_filter("plugin::oll_pdf".to_owned(), LogLevel::Trace)
        .unwrap();
    logger.emit_plugin(
        LogLevel::Trace,
        "plugin::oll_pdf",
        "plugin_trace",
        "corr-retained",
        timestamp,
        json!({}),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();

    let source = fs::read_to_string(logs.join(PLUGIN_LOG_FILENAME)).unwrap();
    assert!(!source.contains("corr-filtered"));
    assert!(source.contains("corr-retained"));
    assert_eq!(PLUGIN_ROTATION.maximum_bytes, 25 * MEBIBYTE);
    assert_eq!(PLUGIN_ROTATION.retained_rotations, 10);
}

#[test]
fn target_filter_routes_sync_trace_only_when_enabled() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    logger.emit(
        LogLevel::Trace,
        "oll::sync",
        "frame_received",
        "corr-1",
        json!({}),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(
        fs::read_to_string(logs.join("sync.log"))
            .unwrap()
            .is_empty()
    );

    logger
        .set_filter("oll::sync".to_owned(), LogLevel::Trace)
        .unwrap();
    logger.emit(
        LogLevel::Trace,
        "oll::sync",
        "frame_received",
        "corr-2",
        json!({}),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(
        fs::read_to_string(logs.join("sync.log"))
            .unwrap()
            .contains("corr-2")
    );
}

#[test]
fn emit_does_not_block_when_the_bounded_queue_is_full() {
    let identity = NodeIdentity::generate("home".parse().unwrap());
    let (sender, receiver) = mpsc::sync_channel(1);
    let logger = NodeLogger {
        identity: Arc::new(RwLock::new(identity)),
        sender,
        filters: RwLock::new(BTreeMap::new()),
        dropped_events: Arc::new(AtomicU64::new(0)),
        emit_failure_reported: AtomicBool::new(false),
    };
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "first_event",
        "corr-1",
        json!({}),
    );

    let started = Instant::now();
    logger.emit(
        LogLevel::Error,
        "oll::node",
        "queue_is_full",
        "corr-2",
        json!({}),
    );

    assert!(started.elapsed() < Duration::from_millis(50));
    assert_eq!(logger.dropped_events.load(Ordering::Relaxed), 1);

    let flush_started = Instant::now();
    assert!(matches!(
        logger.flush_until(Instant::now() + Duration::from_millis(20)),
        Err(NodeError::Unavailable(_))
    ));
    assert!(flush_started.elapsed() < Duration::from_millis(200));

    drop(receiver);
    let _: () = logger.emit(
        LogLevel::Error,
        "oll::node",
        "writer_disconnected",
        "corr-3",
        json!({}),
    );
    assert_eq!(logger.dropped_events.load(Ordering::Relaxed), 2);

    let _: () = logger.emit(
        LogLevel::Error,
        "oll::node",
        "invalid_correlation",
        "",
        json!({}),
    );
    assert_eq!(logger.dropped_events.load(Ordering::Relaxed), 3);
}

#[test]
fn writer_reports_dropped_events_after_it_recovers() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    logger.dropped_events.store(7, Ordering::Relaxed);
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "retained_event",
        "corr-retained",
        json!({}),
    );
    logger
        .flush_until(Instant::now() + Duration::from_secs(2))
        .unwrap();

    let records = fs::read_to_string(logs.join("oll.log")).unwrap();
    let dropped = records
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|record| record["event"] == "log_events_dropped")
        .unwrap();
    assert_eq!(dropped["dropped_event_count"], 7);
    assert_eq!(dropped["queue_capacity"], LOG_QUEUE_CAPACITY);
}

#[test]
fn writer_flushes_a_partial_batch_on_the_periodic_interval() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let logger = NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
    logger.emit(
        LogLevel::Info,
        "oll::node",
        "partial_batch",
        "corr-periodic",
        json!({}),
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if fs::read_to_string(logs.join("oll.log"))
            .unwrap()
            .contains("corr-periodic")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "partial log batch was not flushed"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn rotation_is_atomic_and_compressed_off_the_logging_path() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join(OLL_LOG_FILENAME);
    let mut sink = RotatingLogSink::open(
        path.clone(),
        RotationPolicy {
            maximum_bytes: 1,
            retained_rotations: 2,
        },
    )
    .unwrap();
    let worker = CompressionWorker::new().unwrap();
    let now = OffsetDateTime::now_utc();

    sink.write(b"x", now).unwrap();
    let job = sink.write(b"y", now).unwrap().unwrap();
    assert!(path.exists());
    assert!(job.source.exists());
    worker.enqueue(job);

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let compressed = rotated_log_files(directory.path(), OLL_LOG_FILENAME)
            .unwrap()
            .into_iter()
            .any(|entry| entry.extension().is_some_and(|extension| extension == "gz"));
        if compressed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rotation did not compress in time"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
