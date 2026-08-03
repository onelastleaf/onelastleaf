mod archive_read;
mod archive_write;
mod export;
mod import;
mod inspect;
mod manifest;
mod support;
mod types;
mod verify;

#[cfg(test)]
mod tests;

#[cfg(test)]
use std::path::PathBuf;

pub(crate) use export::export_runtime;
pub(crate) use import::import_runtime;
pub use inspect::{inspect_snapshot, verify_snapshot};
pub use types::SnapshotInspection;

pub(super) const SNAPSHOT_FORMAT: &str = "onelastleaf-replica-snapshot";
pub(super) const SNAPSHOT_FORMAT_VERSION: u32 = 1;
pub(super) const MANIFEST_ENTRY: &str = "manifest.json";
pub(super) const CATALOG_ENTRY: &str = "catalog.loro";

#[cfg(test)]
pub(super) struct ExportArchiveTestHook {
    pub destination: PathBuf,
    pub started: std::sync::mpsc::Sender<()>,
    pub release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(super) static EXPORT_ARCHIVE_TEST_HOOK: std::sync::Mutex<Option<ExportArchiveTestHook>> =
    std::sync::Mutex::new(None);
