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

#[derive(Clone, Copy, Debug)]
pub enum SessionError {
    NoFuseFile,
    MountFailed,
    CannotReadFuse,
    CannotParseRequest,
    UnknownError,
}

#[derive(Clone, Copy, Debug)]
pub enum ParseError {
    InsufficientLength,
    UnknownError,
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

            Self::CannotParseRequest =>
                "Failed to parse the FUSE protocol request; the Linux kernel may have encountered a runtime issue.".into(),

            Self::UnknownError => "The FUSE protocol stack failed to run because it encountered an unexpected error.".into(),
        }
    }
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

        let request = match parse_fuse_request(&buffer) {
            Ok(request) => request,
            Err(error) => {
                state
                    .lock()
                    .stop_with_error(SessionError::CannotParseRequest);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{error:?}"),
                ));
            }
        };

        todo!()
    }
}

fn parse_fuse_request(request: &[u8]) -> Result<requests::Request, ParseError> {
    let Some(header_bytes) = request.get(..40) else {
        return Err(ParseError::InsufficientLength);
    };
    let Some(header) = parse_fuse_request_head(header_bytes) else {
        return Err(ParseError::UnknownError);
    };

    todo!()
}

fn parse_fuse_request_head(request_head: &[u8]) -> Option<requests::Header> {
    if request_head.len() != 40 {
        return None;
    };

    Some(requests::Header {
        len: u32::from_ne_bytes(request_head[0..4].try_into().unwrap()),
        opcode: u32::from_ne_bytes(request_head[4..8].try_into().unwrap()),
        unique: u64::from_ne_bytes(request_head[8..16].try_into().unwrap()),
        nodeid: u64::from_ne_bytes(request_head[16..24].try_into().unwrap()),
        uid: u32::from_ne_bytes(request_head[24..28].try_into().unwrap()),
        gid: u32::from_ne_bytes(request_head[28..32].try_into().unwrap()),
        total_extlen: u16::from_ne_bytes(request_head[36..38].try_into().unwrap()),
    })
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
