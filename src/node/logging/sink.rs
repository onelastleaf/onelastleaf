use std::{
    fs,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::mpsc::{self, SyncSender, TrySendError},
    thread,
};

use time::{Date, OffsetDateTime};

use crate::{
    cli::LogFilterLevel, node::runtime::NodeError, protocol::oll::LogLevel as ProtoLogLevel,
};

use super::{
    COMPRESSION_QUEUE_CAPACITY, LOG_BUFFER_CAPACITY,
    files::{
        compress_rotation, file_date, open_log_file, prune_rotations, rotation_path, write_event,
    },
};

#[derive(Clone, Copy)]
pub(super) struct RotationPolicy {
    pub(super) maximum_bytes: u64,
    pub(super) retained_rotations: usize,
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

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

pub(super) struct LogSinks {
    pub(super) oll: RotatingLogSink,
    pub(super) sync: RotatingLogSink,
}

pub(super) struct RotatingLogSink {
    file: BufWriter<File>,
    path: PathBuf,
    filename: String,
    active_date: Date,
    active_size: u64,
    policy: RotationPolicy,
}

impl RotatingLogSink {
    pub(super) fn open(path: PathBuf, policy: RotationPolicy) -> Result<Self, NodeError> {
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

    pub(super) fn write(
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

    pub(super) fn flush(&mut self, durable: bool) -> Result<(), NodeError> {
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

pub(super) struct RotationJob {
    pub(super) source: PathBuf,
    pub(super) filename: String,
    pub(super) policy: RotationPolicy,
}

/// Compression runs outside the daemon's logging path. If the bounded queue is
/// full, the intact rotated JSONL file is retained for a later startup instead
/// of making a daemon operation wait on disk compression.
pub(super) struct CompressionWorker {
    sender: SyncSender<RotationJob>,
}

impl CompressionWorker {
    pub(super) fn new() -> Result<Self, NodeError> {
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

    pub(super) fn enqueue(&self, job: RotationJob) {
        match self.sender.try_send(job) {
            Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
        }
    }
}
