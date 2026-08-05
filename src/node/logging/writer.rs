use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Instant,
};

use serde_json::{Map, Value};
use time::OffsetDateTime;

use crate::node::{identity::NodeIdentity, runtime::NodeError};

use super::{
    LOG_BATCH_SIZE, LOG_FLUSH_INTERVAL, LOG_QUEUE_CAPACITY,
    files::{format_timestamp, new_correlation_id},
    logger::{LogCommand, LogRoute, QueuedLogEvent},
    sink::{CompressionWorker, LogLevel, LogSinks},
};

pub(super) struct StructuredLogEvent<'a> {
    pub(super) level: LogLevel,
    pub(super) target: &'a str,
    pub(super) event: &'a str,
    pub(super) correlation_id: &'a str,
    pub(super) fields: Value,
    pub(super) timestamp: OffsetDateTime,
    pub(super) observed_at: Option<OffsetDateTime>,
}

pub(super) fn encode_log_event(
    identity: &NodeIdentity,
    event: StructuredLogEvent<'_>,
) -> Result<Vec<u8>, NodeError> {
    let StructuredLogEvent {
        level,
        target,
        event,
        correlation_id,
        fields,
        timestamp,
        observed_at,
    } = event;
    let mut record = Map::new();
    record.insert(
        "timestamp".to_owned(),
        Value::String(format_timestamp(timestamp)),
    );
    record.insert("level".to_owned(), Value::String(level.as_str().to_owned()));
    record.insert("target".to_owned(), Value::String(target.to_owned()));
    record.insert("event".to_owned(), Value::String(event.to_owned()));
    record.insert(
        "correlation_id".to_owned(),
        Value::String(correlation_id.to_owned()),
    );
    if let Some(observed_at) = observed_at {
        record.insert(
            "observed_at".to_owned(),
            Value::String(format_timestamp(observed_at)),
        );
    }
    record.insert(
        "node_id".to_owned(),
        Value::String(identity.node_id().to_string()),
    );
    record.insert(
        "node_name".to_owned(),
        Value::String(identity.node_name().as_str().to_owned()),
    );
    if let Value::Object(fields) = fields {
        for (key, value) in fields {
            record.entry(key).or_insert(value);
        }
    }
    let mut encoded = serde_json::to_vec(&Value::Object(record))
        .map_err(|_| NodeError::Internal("cannot encode structured log event".to_owned()))?;
    encoded.push(b'\n');
    Ok(encoded)
}

pub(super) fn run_log_writer(
    mut sinks: LogSinks,
    compression: CompressionWorker,
    receiver: mpsc::Receiver<LogCommand>,
    dropped_events: Arc<AtomicU64>,
    identity: Arc<RwLock<NodeIdentity>>,
) {
    let mut commands_since_flush = 0_usize;
    let mut next_flush = Instant::now() + LOG_FLUSH_INTERVAL;
    let mut failure_reported = false;
    loop {
        let command = receiver.recv_timeout(next_flush.saturating_duration_since(Instant::now()));
        let result = match command {
            Ok(LogCommand::Event(event)) => {
                commands_since_flush += 1;
                write_queued_event(&mut sinks, &compression, &event)
                    .and_then(|_| {
                        write_dropped_summary(&mut sinks, &compression, &dropped_events, &identity)
                    })
                    .and_then(|_| {
                        if commands_since_flush >= LOG_BATCH_SIZE {
                            commands_since_flush = 0;
                            next_flush = Instant::now() + LOG_FLUSH_INTERVAL;
                            flush_log_sinks(&mut sinks, false)
                        } else {
                            Ok(())
                        }
                    })
            }
            Ok(LogCommand::Flush(result_sender)) => {
                commands_since_flush = 0;
                next_flush = Instant::now() + LOG_FLUSH_INTERVAL;
                let result =
                    write_dropped_summary(&mut sinks, &compression, &dropped_events, &identity)
                        .and_then(|_| flush_log_sinks(&mut sinks, true));
                let reply = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                let _ = result_sender.send(reply);
                result
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                commands_since_flush = 0;
                next_flush = Instant::now() + LOG_FLUSH_INTERVAL;
                write_dropped_summary(&mut sinks, &compression, &dropped_events, &identity)
                    .and_then(|_| flush_log_sinks(&mut sinks, false))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let result =
                    write_dropped_summary(&mut sinks, &compression, &dropped_events, &identity)
                        .and_then(|_| flush_log_sinks(&mut sinks, true));
                if let Err(error) = result {
                    eprintln!("oll structured log writer failed during close: {error}");
                }
                break;
            }
        };
        match result {
            Ok(()) => failure_reported = false,
            Err(error) if !failure_reported => {
                eprintln!("oll structured log writer failed: {error}");
                failure_reported = true;
            }
            Err(_) => {}
        }
    }
}

fn write_queued_event(
    sinks: &mut LogSinks,
    compression: &CompressionWorker,
    event: &QueuedLogEvent,
) -> Result<(), NodeError> {
    if matches!(event.route, LogRoute::Sync | LogRoute::SyncAndOll)
        && let Some(job) = sinks.sync.write(&event.encoded, event.rotation_at)?
    {
        compression.enqueue(job);
    }
    if matches!(event.route, LogRoute::Oll | LogRoute::SyncAndOll)
        && let Some(job) = sinks.oll.write(&event.encoded, event.rotation_at)?
    {
        compression.enqueue(job);
    }
    if matches!(event.route, LogRoute::Plugin)
        && let Some(job) = sinks.plugin.write(&event.encoded, event.rotation_at)?
    {
        compression.enqueue(job);
    }
    Ok(())
}

fn write_dropped_summary(
    sinks: &mut LogSinks,
    compression: &CompressionWorker,
    dropped_events: &AtomicU64,
    identity: &RwLock<NodeIdentity>,
) -> Result<(), NodeError> {
    let dropped = dropped_events.swap(0, Ordering::Relaxed);
    if dropped == 0 {
        return Ok(());
    }
    let identity = identity
        .read()
        .map_err(|_| NodeError::Internal("logger identity lock is poisoned".to_owned()))?
        .clone();
    let emitted_at = OffsetDateTime::now_utc();
    let correlation_id = new_correlation_id();
    let encoded = match encode_log_event(
        &identity,
        StructuredLogEvent {
            level: LogLevel::Warn,
            target: "oll::node",
            event: "log_events_dropped",
            correlation_id: &correlation_id,
            fields: serde_json::json!({
                "dropped_event_count": dropped,
                "queue_capacity": LOG_QUEUE_CAPACITY,
            }),
            timestamp: emitted_at,
            observed_at: None,
        },
    ) {
        Ok(encoded) => encoded,
        Err(error) => {
            dropped_events.fetch_add(dropped, Ordering::Relaxed);
            return Err(error);
        }
    };
    match sinks.oll.write(&encoded, emitted_at) {
        Ok(Some(job)) => compression.enqueue(job),
        Ok(None) => {}
        Err(error) => {
            dropped_events.fetch_add(dropped, Ordering::Relaxed);
            return Err(error);
        }
    }
    Ok(())
}

fn flush_log_sinks(sinks: &mut LogSinks, durable: bool) -> Result<(), NodeError> {
    let oll = sinks.oll.flush(durable);
    let sync = sinks.sync.flush(durable);
    let plugin = sinks.plugin.flush(durable);
    oll.and(sync).and(plugin)
}
