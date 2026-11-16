use super::requests;
use nix::{
    mount::{MsFlags, mount},
    unistd::{Gid, Uid},
};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read},
    os::fd::{AsRawFd, RawFd},
    path::PathBuf,
    sync::{Arc, nonpoison::Mutex},
    thread::{self, JoinHandle},
};

use super::FileSystem;

pub struct Session<FS: FileSystem> {
    filesystem: FS,
    mount_point: PathBuf,
}

pub struct Handle {
    thread: JoinHandle<io::Result<()>>,
    state: Arc<Mutex<HandleState>>,
}

pub struct HandleState {
    status: Status,
    error: Option<SessionError>,
    negotiated_config: Option<InitConfig>,
}

#[derive(Clone, Copy, Debug)]
pub enum Status {
    Initializing,
    Running,
    Stopped,
}

#[derive(Clone, Debug)]
pub enum SessionError {
    NoFuseFile,
    MountFailed,
    CannotReadFuse,
}

impl SessionError {
    pub fn message(&self) -> String {
        match self {
            Self::NoFuseFile =>
                "/dev/fuse cannot be opened. Please check possible permission settings.".into(),

            Self::MountFailed =>
                "Failed to mount the FUSE filesystem. Please check mount permissions and the target path.".into(),

            Self::CannotReadFuse =>
                "Failed to read data from /dev/fuse; the Linux kernel may have encountered a runtime issue.".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Request {
    Init(requests::InitRequest),
    Lookup,
    Forget,
    Getattr,
    Setattr,
}

pub struct InitConfig {}

impl<FS: FileSystem> Session<FS> {
    pub fn new(filesystem: FS, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            filesystem,
            mount_point: mount_point.into(),
        }
    }

    pub fn spawn(self) -> io::Result<Handle>
    where
        FS: Send + 'static,
    {
        let state = Arc::new(Mutex::new(HandleState {
            status: Status::Initializing,
            error: None,
            negotiated_config: None,
        }));

        let state_in_thread = Arc::clone(&state);

        let thread = thread::spawn(move || self.run(&state_in_thread));

        Ok(Handle { thread, state })
    }

    fn open_fuse_file() -> io::Result<File> {
        OpenOptions::new().read(true).write(true).open("/dev/fuse")
    }

    fn run(self, state: &Mutex<HandleState>) -> io::Result<()> {
        // 1. Open /dev/fuse, and get the fd
        let mut fuse_file = match Self::open_fuse_file() {
            Ok(device) => device,
            Err(error) => {
                state.lock().stop_with_error(SessionError::NoFuseFile);
                return Err(error);
            }
        };
        let fuse_fd: RawFd = fuse_file.as_raw_fd();

        // 2. Mount the filesystem
        let mount_options = format!(
            "fd={},rootmode=40000,user_id={},group_id={}",
            fuse_fd,
            Uid::current().as_raw(),
            Gid::current().as_raw(),
        );

        if let Err(error) = mount(
            Some("ollfs"),     // Filesystem name
            &self.mount_point, // Mount point
            Some("fuse"),      // Filesystem type
            MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
            Some(mount_options.as_str()), // FUSE mount options
        ) {
            state.lock().stop_with_error(SessionError::MountFailed);
            return Err(error.into());
        }

        // 3. Receive the INIT packet from the Linux kernel.
        let mut buffer = vec![0u8; 8192];

        let size = match fuse_file.read(&mut buffer) {
            Ok(size) => size,
            Err(error) => {
                state.lock().stop_with_error(SessionError::CannotReadFuse);
                return Err(error);
            }
        };

        buffer.truncate(size);

        todo!()
    }
}

fn parse_fuse_request(request: &[u8]) -> Request {
    todo!()
}

impl HandleState {
    fn stop_with_error(&mut self, error: SessionError) {
        self.status = Status::Stopped;
        self.error = Some(error);
    }
}

impl InitConfig {
    pub fn new() -> Self {
        todo!()
    }
}
