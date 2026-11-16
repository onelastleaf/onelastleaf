use std::{
    io,
    path::PathBuf,
    thread::{self, JoinHandle},
};

use super::FileSystem;

pub struct Session<FS: FileSystem> {
    filesystem: FS,
    mount_point: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub enum Status {
    Initializing,
    Running,
    Stopped,
}

pub struct Handle {
    thread: JoinHandle<io::Result<()>>,
    status: Status,
    error_msg: Option<String>,
    negotiationed_config: Option<InitConfig>,
}

pub struct InitConfig {}

impl<FS: FileSystem> Session<FS> {
    pub fn new(filesystem: FS, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            filesystem,
            mount_point: mount_point.into(),
        }
    }

    pub fn spawn(self) -> io::Result<Handle> {
        let thread = thread::spawn(move || self.run());

        Ok(Handle {
            thread,
            status: Status::Initializing,
            error_msg: None,
            negotiationed_config: None,
        })
    }

    fn run(self) -> io::Result<()> {
        todo!()
    }
}

impl InitConfig {
    pub fn new() -> Self {
        todo!()
    }
}
