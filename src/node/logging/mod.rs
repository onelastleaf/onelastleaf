mod files;
mod logger;
mod sink;
mod writer;

#[cfg(test)]
mod tests;

pub use files::{ensure_log_directory, new_correlation_id};
pub use logger::NodeLogger;
pub use sink::LogLevel;

use std::time::Duration;

use sink::RotationPolicy;

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
