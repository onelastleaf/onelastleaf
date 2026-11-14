use std::path::PathBuf;

use super::FileSystem;

pub struct Session<FS: FileSystem> {
    filesystem: FS,
    mount_point: PathBuf,
}

pub struct Handle {}

impl<FS: FileSystem> Session<FS> {
    pub fn new(filesystem: FS, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            filesystem,
            mount_point: mount_point.into(),
        }
    }

    pub fn spawn() -> Handle {
        todo!()
    }
}
