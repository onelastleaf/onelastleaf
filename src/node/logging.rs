use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant},
};

use flate2::{Compression, write::GzEncoder};
use serde_json::{Map, Value};
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{cli::LogFilterLevel, protocol::oll::LogLevel as ProtoLogLevel};

use super::{identity::NodeIdentity, runtime::NodeError};

const OLL_LOG_FILENAME: &str = "oll.log";
const SYNC_LOG_FILENAME: &str = "sync.log";
const MEBIBYTE: u64 = 1024 * 1024;
const COMPRESSION_QUEUE_CAPACITY: usize = 4;
const LOG_QUEUE_CAPACITY: usize = 4096;
const LOG_BUFFER_CAPACITY: usize = 64 * 1024;
const LOG_BATCH_SIZE: usize = 256;
const LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

const OLL_ROTATION: RotationPolicy = RotationPolicy {
    maximum_bytes: 25 * MEBIBYTE,
    retained_rotations: 14,
};
const SYNC_ROTATION: RotationPolicy = RotationPolicy {
    maximum_bytes: 100 * MEBIBYTE,
    retained_rotations: 10,
};

#[derive(Clone, Copy)]
struct RotationPolicy {
    maximum_bytes: u64,
    retained_rotations: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_cli(level: LogFilterLevel) -> Self {
        match level {
            LogFilterLevel::Trace => Self::Trace,
            LogFilterLevel::Debug => Self::Debug,
            LogFilterLevel::Info => Self::Info,
            LogFilterLevel::Warn => Self::Warn,
            LogFilterLevel::Error => Self::Error,
        }
    }

    pub fn from_proto(level: ProtoLogLevel) -> Option<Self> {
        match level {
            ProtoLogLevel::Trace => Some(Self::Trace),
            ProtoLogLevel::Debug => Some(Self::Debug),
            ProtoLogLevel::Info => Some(Self::Info),
            ProtoLogLevel::Warn => Some(Self::Warn),
            ProtoLogLevel::Error => Some(Self::Error),
            ProtoLogLevel::Unspecified => None,
        }
    }

    pub fn to_proto(self) -> ProtoLogLevel {
        match self {
            Self::Trace => ProtoLogLevel::Trace,
            Self::Debug => ProtoLogLevel::Debug,
            Self::Info => ProtoLogLevel::Info,
            Self::Warn => ProtoLogLevel::Warn,
            Self::Error => ProtoLogLevel::Error,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

struct LogSinks {
    oll: RotatingLogSink,
    sync: RotatingLogSink,
}

struct RotatingLogSink {
    file: BufWriter<File>,
    path: PathBuf,
    filename: String,
    active_date: Date,
    active_size: u64,
    policy: RotationPolicy,
}

impl RotatingLogSink {
    fn open(path: PathBuf, policy: RotationPolicy) -> Result<Self, NodeError> {
        let file = open_log_file(&path)?;
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| NodeError::Internal("log file name is not valid UTF-8".to_owned()))?
            .to_owned();
        let now = OffsetDateTime::now_utc();
        let active_date = file_date(&file).unwrap_or_else(|| now.date());
        let active_size = file
            .metadata()
            .map_err(|error| NodeError::io("inspect active log file", error))?
            .len();
        Ok(Self {
            file: BufWriter::with_capacity(LOG_BUFFER_CAPACITY, file),
            path,
            filename,
            active_date,
            active_size,
            policy,
        })
    }

    fn write(
        &mut self,
        encoded: &[u8],
        now: OffsetDateTime,
    ) -> Result<Option<RotationJob>, NodeError> {
        let rotation = self.rotate_if_needed(encoded.len() as u64, now)?;
        write_event(&mut self.file, encoded)?;
        self.active_size = self.active_size.saturating_add(encoded.len() as u64);
        Ok(rotation)
    }

    fn rotate_if_needed(
        &mut self,
        incoming_bytes: u64,
        now: OffsetDateTime,
    ) -> Result<Option<RotationJob>, NodeError> {
        if self.active_date == now.date()
            && self.active_size.saturating_add(incoming_bytes) <= self.policy.maximum_bytes
        {
            return Ok(None);
        }

        self.flush(true)?;
        let rotated = rotation_path(&self.path, &self.filename, now)?;
        fs::rename(&self.path, &rotated)
            .map_err(|error| NodeError::io("rotate structured log", error))?;
        match open_log_file(&self.path) {
            Ok(file) => self.file = BufWriter::with_capacity(LOG_BUFFER_CAPACITY, file),
            Err(error) => {
                let _ = fs::rename(&rotated, &self.path);
                return Err(error);
            }
        }
        self.active_date = now.date();
        self.active_size = 0;
        Ok(Some(RotationJob {
            source: rotated,
            filename: self.filename.clone(),
            policy: self.policy,
        }))
    }

    fn flush(&mut self, durable: bool) -> Result<(), NodeError> {
        self.file
            .flush()
            .map_err(|error| NodeError::io("flush structured log", error))?;
        if durable {
            self.file
                .get_ref()
                .sync_data()
                .map_err(|error| NodeError::io("synchronize structured log", error))?;
        }
        Ok(())
    }
}

struct RotationJob {
    source: PathBuf,
    filename: String,
    policy: RotationPolicy,
}

/// Compression runs outside the daemon's logging path. If the bounded queue is
/// full, the intact rotated JSONL file is retained for a later startup instead
/// of making a daemon operation wait on disk compression.
struct CompressionWorker {
    sender: SyncSender<RotationJob>,
}

impl CompressionWorker {
    fn new() -> Result<Self, NodeError> {
        let (sender, receiver) = mpsc::sync_channel(COMPRESSION_QUEUE_CAPACITY);
        thread::Builder::new()
            .name("oll-log-compression".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let _ = compress_rotation(&job);
                    let _ = prune_rotations(&job.source, &job.filename, job.policy);
                }
            })
            .map_err(|error| NodeError::io("start log compression worker", error))?;
        Ok(Self { sender })
    }

    fn enqueue(&self, job: RotationJob) {
        match self.sender.try_send(job) {
            Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

#[derive(Clone, Copy)]
enum LogRoute {
    Oll,
    Sync,
    SyncAndOll,
}

struct QueuedLogEvent {
    encoded: Vec<u8>,
    emitted_at: OffsetDateTime,
    route: LogRoute,
}

enum LogCommand {
    Event(QueuedLogEvent),
    Flush(mpsc::Sender<Result<(), String>>),
}

/// The user-owned JSONL logging sink for one node deployment.
pub struct NodeLogger {
    identity: NodeIdentity,
    sender: SyncSender<LogCommand>,
    filters: RwLock<BTreeMap<String, LogLevel>>,
    dropped_events: Arc<AtomicU64>,
    emit_failure_reported: AtomicBool,
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
        let writer_identity = identity.clone();
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
        let encoded = match encode_log_event(
            &self.identity,
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

fn encode_log_event(
    identity: &NodeIdentity,
    level: LogLevel,
    target: &str,
    event: &str,
    correlation_id: &str,
    fields: Value,
    timestamp: OffsetDateTime,
) -> Result<Vec<u8>, NodeError> {
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

fn run_log_writer(
    mut sinks: LogSinks,
    compression: CompressionWorker,
    receiver: mpsc::Receiver<LogCommand>,
    dropped_events: Arc<AtomicU64>,
    identity: NodeIdentity,
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
        && let Some(job) = sinks.sync.write(&event.encoded, event.emitted_at)?
    {
        compression.enqueue(job);
    }
    if matches!(event.route, LogRoute::Oll | LogRoute::SyncAndOll)
        && let Some(job) = sinks.oll.write(&event.encoded, event.emitted_at)?
    {
        compression.enqueue(job);
    }
    Ok(())
}

fn write_dropped_summary(
    sinks: &mut LogSinks,
    compression: &CompressionWorker,
    dropped_events: &AtomicU64,
    identity: &NodeIdentity,
) -> Result<(), NodeError> {
    let dropped = dropped_events.swap(0, Ordering::Relaxed);
    if dropped == 0 {
        return Ok(());
    }
    let emitted_at = OffsetDateTime::now_utc();
    let encoded = match encode_log_event(
        identity,
        LogLevel::Warn,
        "oll::node",
        "log_events_dropped",
        &new_correlation_id(),
        serde_json::json!({
            "dropped_event_count": dropped,
            "queue_capacity": LOG_QUEUE_CAPACITY,
        }),
        emitted_at,
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
    sinks.oll.flush(durable)?;
    sinks.sync.flush(durable)
}

pub fn new_correlation_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn ensure_log_directory(log_dir: &Path) -> Result<(), NodeError> {
    let existed = match fs::symlink_metadata(log_dir) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(NodeError::io("inspect log directory", error)),
    };
    if existed.is_none() {
        fs::create_dir_all(log_dir)
            .map_err(|error| NodeError::io("create log directory", error))?;
        fs::set_permissions(log_dir, fs::Permissions::from_mode(0o700))
            .map_err(|error| NodeError::io("set log directory permissions", error))?;
    }

    let metadata = fs::symlink_metadata(log_dir)
        .map_err(|error| NodeError::io("inspect log directory", error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(NodeError::Config(format!(
            "log path {} is not a directory",
            log_dir.display()
        )));
    }
    if metadata.uid() != effective_uid() || metadata.mode() & 0o077 != 0 {
        return Err(NodeError::Config(format!(
            "log directory {} has unsafe ownership or permissions",
            log_dir.display()
        )));
    }
    Ok(())
}

fn open_log_file(path: &Path) -> Result<File, NodeError> {
    if let Ok(metadata) = fs::symlink_metadata(path)
        && (!metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != effective_uid()
            || metadata.mode() & 0o077 != 0)
    {
        return Err(NodeError::Config(format!(
            "log file {} has unsafe type, ownership, or permissions",
            path.display()
        )));
    }

    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| NodeError::io("open log file", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| NodeError::io("inspect log file", error))?;
    if !metadata.is_file() || metadata.uid() != effective_uid() || metadata.mode() & 0o077 != 0 {
        return Err(NodeError::Config(format!(
            "log file {} has unsafe ownership or permissions",
            path.display()
        )));
    }
    Ok(file)
}

fn write_event(file: &mut BufWriter<File>, encoded: &[u8]) -> Result<(), NodeError> {
    file.write_all(encoded)
        .map_err(|error| NodeError::io("write structured log event", error))
}

fn file_date(file: &File) -> Option<Date> {
    let modified = file.metadata().ok()?.modified().ok()?;
    Some(OffsetDateTime::from(modified).date())
}

fn rotation_path(path: &Path, filename: &str, now: OffsetDateTime) -> Result<PathBuf, NodeError> {
    let parent = path.parent().ok_or_else(|| {
        NodeError::Internal("active log file path has no parent directory".to_owned())
    })?;
    Ok(parent.join(format!(
        "{filename}.{}.{:09}.{}.jsonl",
        now.unix_timestamp(),
        now.nanosecond(),
        Uuid::new_v4()
    )))
}

fn queue_pending_rotations(
    directory: &Path,
    filename: &str,
    policy: RotationPolicy,
    worker: &CompressionWorker,
) -> Result<(), NodeError> {
    for source in rotated_log_files(directory, filename)? {
        if source
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            worker.enqueue(RotationJob {
                source,
                filename: filename.to_owned(),
                policy,
            });
        }
    }
    Ok(())
}

fn compress_rotation(job: &RotationJob) -> io::Result<()> {
    let input = File::open(&job.source)?;
    let compressed = append_suffix(&job.source, ".gz");
    let temporary = append_suffix(&compressed, &format!(".{}.tmp", Uuid::new_v4()));
    let result = (|| -> io::Result<()> {
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        let mut input = input;
        io::copy(&mut input, &mut encoder)?;
        let output = encoder.finish()?;
        output.sync_all()?;
        fs::rename(&temporary, &compressed)?;
        sync_parent(&compressed)?;
        fs::remove_file(&job.source)?;
        sync_parent(&compressed)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn prune_rotations(source: &Path, filename: &str, policy: RotationPolicy) -> io::Result<()> {
    let parent = source.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "rotated log file has no parent directory",
        )
    })?;
    let mut paths = rotated_log_files(parent, filename).map_err(|error| match error {
        NodeError::Io { source, .. } => source,
        _ => io::Error::other("cannot enumerate rotated log files"),
    })?;
    paths.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(std::time::UNIX_EPOCH)
    });
    paths.reverse();
    for path in paths.into_iter().skip(policy.retained_rotations) {
        fs::remove_file(path)?;
    }
    sync_parent(source)
}

fn rotated_log_files(directory: &Path, filename: &str) -> Result<Vec<PathBuf>, NodeError> {
    let prefix = format!("{filename}.");
    let entries = fs::read_dir(directory)
        .map_err(|error| NodeError::io("enumerate rotated log files", error))?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| NodeError::io("read rotated log entry", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| NodeError::io("inspect rotated log entry", error))?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(&prefix) && (name.ends_with(".jsonl") || name.ends_with(".jsonl.gz")) {
            paths.push(entry.path());
        }
    }
    Ok(paths)
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "log file has no parent directory",
        )
    })?;
    File::open(parent)?.sync_all()
}

fn format_timestamp(now: OffsetDateTime) -> String {
    now.format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::{Duration, Instant},
    };

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

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
        let logger =
            NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
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
            identity,
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
        let logger =
            NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
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
        let logger =
            NodeLogger::open(&logs, NodeIdentity::generate("home".parse().unwrap())).unwrap();
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
}
