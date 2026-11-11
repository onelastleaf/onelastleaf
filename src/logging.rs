use std::fs::{OpenOptions, create_dir_all};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;

/// Absolute path to the default log directory.
pub const DEFAULT_LOG_DIR: &str = "/var/log/oll";

const LOG_FILE_NAME: &str = "onelastleaf.log";
static WRITE_LOCK: Mutex<()> = Mutex::new(());

/// Append an Info message with its source module (e.g. `module_path!()`).
pub fn info(module: impl AsRef<str>, message: impl AsRef<str>) -> io::Result<()> {
    write_log("INFO", module.as_ref(), message.as_ref())
}

/// Append a Debug message with its source module (e.g. `module_path!()`).
pub fn debug(module: impl AsRef<str>, message: impl AsRef<str>) -> io::Result<()> {
    write_log("DEBUG", module.as_ref(), message.as_ref())
}

/// Append a Warn message with its source module (e.g. `module_path!()`).
pub fn warn(module: impl AsRef<str>, message: impl AsRef<str>) -> io::Result<()> {
    write_log("WARN", module.as_ref(), message.as_ref())
}

/// Append an Error message with its source module (e.g. `module_path!()`).
pub fn error(module: impl AsRef<str>, message: impl AsRef<str>) -> io::Result<()> {
    write_log("ERROR", module.as_ref(), message.as_ref())
}

fn write_log(level: &str, module: &str, message: &str) -> io::Result<()> {
    let entry = format!("[{level}] [{module}] {message}\n");
    let _guard = WRITE_LOCK
        .lock()
        .map_err(|_| io::Error::other("log writer lock poisoned"))?;

    write_to_file(&entry).or_else(|_| {
        let mut stdout = io::stdout().lock();
        stdout.write_all(entry.as_bytes())?;
        stdout.flush()
    })
}

fn write_to_file(entry: &str) -> io::Result<()> {
    create_dir_all(DEFAULT_LOG_DIR)?;
    let path = Path::new(DEFAULT_LOG_DIR).join(LOG_FILE_NAME);
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(entry.as_bytes())
}
