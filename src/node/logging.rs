use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
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
    file: File,
    path: PathBuf,
    filename: String,
    active_date: Date,
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
        Ok(Self {
            file,
            path,
            filename,
            active_date,
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
        Ok(rotation)
    }

    fn rotate_if_needed(
        &mut self,
        incoming_bytes: u64,
        now: OffsetDateTime,
    ) -> Result<Option<RotationJob>, NodeError> {
        let size = self
            .file
            .metadata()
            .map_err(|error| NodeError::io("inspect active log file", error))?
            .len();
        if self.active_date == now.date()
            && size.saturating_add(incoming_bytes) <= self.policy.maximum_bytes
        {
            return Ok(None);
        }

        self.file
            .flush()
            .and_then(|_| self.file.sync_data())
            .map_err(|error| NodeError::io("flush active log before rotation", error))?;
        let rotated = rotation_path(&self.path, &self.filename, now)?;
        fs::rename(&self.path, &rotated)
            .map_err(|error| NodeError::io("rotate structured log", error))?;
        match open_log_file(&self.path) {
            Ok(file) => self.file = file,
            Err(error) => {
                let _ = fs::rename(&rotated, &self.path);
                return Err(error);
            }
        }
        self.active_date = now.date();
        Ok(Some(RotationJob {
            source: rotated,
            filename: self.filename.clone(),
            policy: self.policy,
        }))
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

/// The user-owned JSONL logging sink for one node deployment.
pub struct NodeLogger {
    identity: NodeIdentity,
    sinks: Mutex<LogSinks>,
    filters: RwLock<BTreeMap<String, LogLevel>>,
    compression: CompressionWorker,
}

impl NodeLogger {
    pub fn open(log_dir: &Path, identity: NodeIdentity) -> Result<Arc<Self>, NodeError> {
        ensure_log_directory(log_dir)?;
        let compression = CompressionWorker::new()?;
        queue_pending_rotations(log_dir, OLL_LOG_FILENAME, OLL_ROTATION, &compression)?;
        queue_pending_rotations(log_dir, SYNC_LOG_FILENAME, SYNC_ROTATION, &compression)?;
        let oll = RotatingLogSink::open(log_dir.join(OLL_LOG_FILENAME), OLL_ROTATION)?;
        let sync = RotatingLogSink::open(log_dir.join(SYNC_LOG_FILENAME), SYNC_ROTATION)?;
        Ok(Arc::new(Self {
            identity,
            sinks: Mutex::new(LogSinks { oll, sync }),
            filters: RwLock::new(BTreeMap::new()),
            compression,
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
    ) -> Result<(), NodeError> {
        if correlation_id.is_empty() {
            return Err(NodeError::Internal(
                "structured log event is missing a correlation ID".to_owned(),
            ));
        }
        if !self.enabled(target, level)? {
            return Ok(());
        }

        let now = OffsetDateTime::now_utc();
        let mut record = Map::new();
        record.insert("timestamp".to_owned(), Value::String(format_timestamp(now)));
        record.insert("level".to_owned(), Value::String(level.as_str().to_owned()));
        record.insert("target".to_owned(), Value::String(target.to_owned()));
        record.insert("event".to_owned(), Value::String(event.to_owned()));
        record.insert(
            "correlation_id".to_owned(),
            Value::String(correlation_id.to_owned()),
        );
        record.insert(
            "node_id".to_owned(),
            Value::String(self.identity.node_id().to_string()),
        );
        record.insert(
            "node_name".to_owned(),
            Value::String(self.identity.node_name().as_str().to_owned()),
        );
        if let Value::Object(fields) = fields {
            for (key, value) in fields {
                record.entry(key).or_insert(value);
            }
        }

        let mut encoded = serde_json::to_vec(&Value::Object(record))
            .map_err(|_| NodeError::Internal("cannot encode structured log event".to_owned()))?;
        encoded.push(b'\n');

        let mut sinks = self
            .sinks
            .lock()
            .map_err(|_| NodeError::Internal("log sink lock is poisoned".to_owned()))?;
        if target.starts_with("oll::sync") {
            if let Some(job) = sinks.sync.write(&encoded, now)? {
                self.compression.enqueue(job);
            }
            if level >= LogLevel::Info
                && let Some(job) = sinks.oll.write(&encoded, now)?
            {
                self.compression.enqueue(job);
            }
        } else if let Some(job) = sinks.oll.write(&encoded, now)? {
            self.compression.enqueue(job);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), NodeError> {
        let mut sinks = self
            .sinks
            .lock()
            .map_err(|_| NodeError::Internal("log sink lock is poisoned".to_owned()))?;
        sinks
            .oll
            .file
            .flush()
            .and_then(|_| sinks.oll.file.sync_data())
            .map_err(|error| NodeError::io("flush oll log", error))?;
        sinks
            .sync
            .file
            .flush()
            .and_then(|_| sinks.sync.file.sync_data())
            .map_err(|error| NodeError::io("flush sync log", error))
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

fn write_event(file: &mut File, encoded: &[u8]) -> Result<(), NodeError> {
    file.write_all(encoded)
        .and_then(|_| file.flush())
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
        logger
            .emit(
                LogLevel::Info,
                "oll::node",
                "node_started",
                "corr-1",
                json!({ "process_id": 42 }),
            )
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
        logger
            .emit(
                LogLevel::Trace,
                "oll::sync",
                "frame_received",
                "corr-1",
                json!({}),
            )
            .unwrap();
        assert!(
            fs::read_to_string(logs.join("sync.log"))
                .unwrap()
                .is_empty()
        );

        logger
            .set_filter("oll::sync".to_owned(), LogLevel::Trace)
            .unwrap();
        logger
            .emit(
                LogLevel::Trace,
                "oll::sync",
                "frame_received",
                "corr-2",
                json!({}),
            )
            .unwrap();
        assert!(
            fs::read_to_string(logs.join("sync.log"))
                .unwrap()
                .contains("corr-2")
        );
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
