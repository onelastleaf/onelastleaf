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
use time::OffsetDateTime;

use super::{LOG_QUEUE_CAPACITY, LogLevel, NodeLogger, OLL_LOG_FILENAME};
use super::{
    files::rotated_log_files,
    sink::{CompressionWorker, RotatingLogSink, RotationPolicy},
};
use crate::node::{identity::NodeIdentity, runtime::NodeError};

#[test]
fn creates_valid_jsonl_logs_with_correlation_ids() {
    let directory = TempDir::new().unwrap();
    let logger = NodeLogger::open(
        directory.path().join("logs").as_path(),
        NodeIdentity::generate("home".parse().unwrap()),
    )
    .unwrap();
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

    let source = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    let record: Value = serde_json::from_str(source.trim()).unwrap();
    assert_eq!(record["event"], "node_started");
    assert_eq!(record["correlation_id"], "corr-1");
    assert_eq!(record["process_id"], 42);
    assert_eq!(
        fs::metadata(directory.path().join("logs"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
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
