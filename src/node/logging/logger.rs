use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use time::OffsetDateTime;

use crate::node::{identity::NodeIdentity, runtime::NodeError};

use super::{
    LOG_QUEUE_CAPACITY, OLL_LOG_FILENAME, OLL_ROTATION, SYNC_LOG_FILENAME, SYNC_ROTATION,
    files::{ensure_log_directory, queue_pending_rotations},
    sink::{CompressionWorker, LogLevel, LogSinks, RotatingLogSink},
    writer::{encode_log_event, run_log_writer},
};

#[derive(Clone, Copy)]
pub(super) enum LogRoute {
    Oll,
    Sync,
    SyncAndOll,
}

pub(super) struct QueuedLogEvent {
    pub(super) encoded: Vec<u8>,
    pub(super) emitted_at: OffsetDateTime,
    pub(super) route: LogRoute,
}

pub(super) enum LogCommand {
    Event(QueuedLogEvent),
    Flush(mpsc::Sender<Result<(), String>>),
}

/// The user-owned JSONL logging sink for one node deployment.
pub struct NodeLogger {
    pub(super) identity: Arc<RwLock<NodeIdentity>>,
    pub(super) sender: SyncSender<LogCommand>,
    pub(super) filters: RwLock<BTreeMap<String, LogLevel>>,
    pub(super) dropped_events: Arc<AtomicU64>,
    pub(super) emit_failure_reported: AtomicBool,
}

impl NodeLogger {
    pub fn open(log_dir: &Path, identity: NodeIdentity) -> Result<Arc<Self>, NodeError> {
        ensure_log_directory(log_dir)?;
        let compression = CompressionWorker::new()?;
        queue_pending_rotations(log_dir, OLL_LOG_FILENAME, OLL_ROTATION, &compression)?;
        queue_pending_rotations(log_dir, SYNC_LOG_FILENAME, SYNC_ROTATION, &compression)?;
        let oll = RotatingLogSink::open(log_dir.join(OLL_LOG_FILENAME), OLL_ROTATION)?;
        let sync = RotatingLogSink::open(log_dir.join(SYNC_LOG_FILENAME), SYNC_ROTATION)?;
        let (sender, receiver) = mpsc::sync_channel(LOG_QUEUE_CAPACITY);
        let dropped_events = Arc::new(AtomicU64::new(0));
        let writer_dropped_events = Arc::clone(&dropped_events);
        let identity = Arc::new(RwLock::new(identity));
        let writer_identity = Arc::clone(&identity);
        thread::Builder::new()
            .name("oll-log-writer".to_owned())
            .spawn(move || {
                run_log_writer(
                    LogSinks { oll, sync },
                    compression,
                    receiver,
                    writer_dropped_events,
                    writer_identity,
                );
            })
            .map_err(|error| NodeError::io("start structured log writer", error))?;
        Ok(Arc::new(Self {
            identity,
            sender,
            filters: RwLock::new(BTreeMap::new()),
            dropped_events,
            emit_failure_reported: AtomicBool::new(false),
        }))
    }

    pub fn set_identity(&self, identity: NodeIdentity) -> Result<(), NodeError> {
        *self
            .identity
            .write()
            .map_err(|_| NodeError::Internal("logger identity lock is poisoned".to_owned()))? =
            identity;
        Ok(())
    }

    pub fn set_filter(&self, target: String, level: LogLevel) -> Result<(), NodeError> {
        self.filters
            .write()
            .map_err(|_| NodeError::Internal("log filter lock is poisoned".to_owned()))?
            .insert(target, level);
        Ok(())
    }

    pub fn emit(
        &self,
        level: LogLevel,
        target: &str,
        event: &str,
        correlation_id: &str,
        fields: Value,
    ) {
        if correlation_id.is_empty() {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            self.report_emit_failure(
                "oll structured logger rejected an event without a correlation ID",
            );
            return;
        }
        let enabled = match self.enabled(target, level) {
            Ok(enabled) => enabled,
            Err(error) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
                self.report_emit_failure(&format!(
                    "oll structured logger could not evaluate its filter: {error}"
                ));
                return;
            }
        };
        if !enabled {
            return;
        }

        let emitted_at = OffsetDateTime::now_utc();
        let identity = match self.identity.read() {
            Ok(identity) => identity.clone(),
            Err(_) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
                self.report_emit_failure("oll structured logger identity lock is poisoned");
                return;
            }
        };
        let encoded = match encode_log_event(
            &identity,
            level,
            target,
            event,
            correlation_id,
            fields,
            emitted_at,
        ) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
                self.report_emit_failure(&format!(
                    "oll structured logger could not encode an event: {error}"
                ));
                return;
            }
        };
        let route = if target.starts_with("oll::sync") {
            if level >= LogLevel::Info {
                LogRoute::SyncAndOll
            } else {
                LogRoute::Sync
            }
        } else {
            LogRoute::Oll
        };
        match self.sender.try_send(LogCommand::Event(QueuedLogEvent {
            encoded,
            emitted_at,
            route,
        })) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
                self.report_emit_failure(
                    "oll structured log writer stopped; subsequent events will be lost",
                );
            }
        }
    }

    pub fn flush_until(&self, deadline: Instant) -> Result<(), NodeError> {
        let (result_sender, result_receiver) = mpsc::channel();
        let mut command = LogCommand::Flush(result_sender);
        loop {
            match self.sender.try_send(command) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return Err(NodeError::Unavailable(
                            "structured log flush exceeded its shutdown deadline".to_owned(),
                        ));
                    }
                    command = returned;
                    thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(NodeError::Internal(
                        "structured log writer stopped before flush".to_owned(),
                    ));
                }
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        result_receiver
            .recv_timeout(remaining)
            .map_err(|_| {
                NodeError::Unavailable(
                    "structured log flush exceeded its shutdown deadline".to_owned(),
                )
            })?
            .map_err(NodeError::Internal)
    }

    fn enabled(&self, target: &str, level: LogLevel) -> Result<bool, NodeError> {
        let filters = self
            .filters
            .read()
            .map_err(|_| NodeError::Internal("log filter lock is poisoned".to_owned()))?;
        let filter = filters
            .iter()
            .filter(|(candidate, _)| {
                target == candidate.as_str() || target.starts_with(&format!("{candidate}::"))
            })
            .max_by_key(|(candidate, _)| candidate.len())
            .map(|(_, level)| *level)
            .unwrap_or(LogLevel::Info);
        Ok(level >= filter)
    }

    fn report_emit_failure(&self, message: &str) {
        if !self.emit_failure_reported.swap(true, Ordering::Relaxed) {
            eprintln!("{message}");
        }
    }
}
