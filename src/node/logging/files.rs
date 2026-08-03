use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use flate2::{Compression, write::GzEncoder};
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::node::runtime::NodeError;

use super::sink::{CompressionWorker, RotationJob, RotationPolicy};

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

pub(super) fn open_log_file(path: &Path) -> Result<File, NodeError> {
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

pub(super) fn write_event(file: &mut BufWriter<File>, encoded: &[u8]) -> Result<(), NodeError> {
    file.write_all(encoded)
        .map_err(|error| NodeError::io("write structured log event", error))
}

pub(super) fn file_date(file: &File) -> Option<Date> {
    let modified = file.metadata().ok()?.modified().ok()?;
    Some(OffsetDateTime::from(modified).date())
}

pub(super) fn rotation_path(
    path: &Path,
    filename: &str,
    now: OffsetDateTime,
) -> Result<PathBuf, NodeError> {
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

pub(super) fn queue_pending_rotations(
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

pub(super) fn compress_rotation(job: &RotationJob) -> io::Result<()> {
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

pub(super) fn prune_rotations(
    source: &Path,
    filename: &str,
    policy: RotationPolicy,
) -> io::Result<()> {
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

pub(super) fn rotated_log_files(
    directory: &Path,
    filename: &str,
) -> Result<Vec<PathBuf>, NodeError> {
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

pub(super) fn format_timestamp(now: OffsetDateTime) -> String {
    now.format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}
