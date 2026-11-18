use super::payload;
use bitflags::bitflags;
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
    protocol_version: Option<u32>,
    capabilities: Option<Flags>,
}

pub struct Handle {
    thread: JoinHandle<io::Result<()>>,
    state: Arc<Mutex<HandleState>>,
}

pub struct HandleState {
    status: Status,
    error: Option<SessionError>,
    negotiated_config: Option<Flags>,
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
    ExcessiveLength,
    UnknownError,
    InvalidLength,
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

impl<FS: FileSystem> Session<FS> {
    pub fn new(filesystem: FS, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            filesystem,
            mount_point: mount_point.into(),
            protocol_version: None,
            capabilities: None,
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

        let request = match Self::parse_fuse_request(&buffer) {
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

    fn parse_fuse_request<'a>(request: &'a [u8]) -> Result<payload::Payload<'a>, ParseError> {
        let Some((request_header, request_remaining)) = request.split_at_checked(40) else {
            return Err(ParseError::InsufficientLength);
        };
        let Some(header) = Self::parse_fuse_request_head(request_header) else {
            return Err(ParseError::UnknownError);
        };

        if header.len as usize != request.len() {
            return Err(ParseError::InvalidLength);
        }

        let ext_index = usize::from(header.total_extlen) * 8;

        let payload_len = request_remaining
            .len()
            .checked_sub(ext_index)
            .ok_or(ParseError::InsufficientLength)?;

        let (request_payload, request_extensions) = request_remaining.split_at(payload_len);

        todo!()
    }

    fn parse_fuse_request_head(request_head: &[u8]) -> Option<payload::Header> {
        if request_head.len() != 40 {
            return None;
        };

        Some(payload::Header {
            len: u32::from_ne_bytes(request_head[0..4].try_into().unwrap()),
            opcode: u32::from_ne_bytes(request_head[4..8].try_into().unwrap()),
            unique: u64::from_ne_bytes(request_head[8..16].try_into().unwrap()),
            nodeid: u64::from_ne_bytes(request_head[16..24].try_into().unwrap()),
            uid: u32::from_ne_bytes(request_head[24..28].try_into().unwrap()),
            gid: u32::from_ne_bytes(request_head[28..32].try_into().unwrap()),
            total_extlen: u16::from_ne_bytes(request_head[36..38].try_into().unwrap()),
        })
    }

    #[allow(dead_code, unused_variables)]
    fn parse_fuse_request_payload<'a>(
        &self,
        opcode: u32,
        request_payload: &'a [u8],
    ) -> Result<payload::Payload<'a>, ParseError> {
        todo!()
    }
}

impl HandleState {
    fn stop_with_error(&mut self, error: SessionError) {
        self.status = Status::Stopped;
        self.error = Some(error);
    }
}

bitflags::bitflags! {
    /// FUSE capabilities negotiated with the Linux kernel during `INIT`.
    ///
    /// Use `HandleState::negotiated_config` to choose which protocol features
    /// to use for this session. `None` means no negotiation result is available.
    /// With `Some(flags)`, check the capability before using its feature.
    ///
    /// For example, use `splice` to send replies to `/dev/fuse` when
    /// `SPLICE_WRITE` is set. Otherwise, send replies with a regular write.
    ///
    /// ```
    /// use onelastleaf::storage::fuse::session::Flags;
    ///
    /// fn can_splice_replies(negotiated_config: Option<Flags>) -> bool {
    ///     negotiated_config.is_some_and(|flags| flags.contains(Flags::SPLICE_WRITE))
    /// }
    /// ```
    ///
    /// Combine capabilities with `|`. Use [`contains`](Self::contains) to require
    /// all of them or [`intersects`](Self::intersects) to require any of them.
    /// For logging, use [`iter_names`](Self::iter_names) or [`bits`](Self::bits).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Flags: u64 {
        const ASYNC_READ   = 1 << 0;
        const POSIX_LOCKS = 1 << 1;
        const FILE_OPS = 1 << 2;
        const ATOMIC_O_TRUNC = 1 << 3;
        const EXPORT_SUPPORT = 1 << 4;
        const BIG_WRITES = 1 << 5;
        const DONT_MASK = 1 << 6;
        const SPLICE_WRITE = 1 << 7;
        const SPLICE_MOVE = 1 << 8;
        const SPLICE_READ = 1 << 9;
        const FLOCK_LOCKS = 1 << 10;
        const HAS_IOCTL_DIR = 1 << 11;
        const AUTO_INVAL_DATA = 1 << 12;
        const DO_READDIRPLUS = 1 << 13;
        const READDIRPLUS_AUTO = 1 << 14;
        const ASYNC_DIO = 1 << 15;
        const WRITEBACK_CACHE = 1 << 16;
        const NO_OPEN_SUPPORT = 1 << 17;
        const PARALLEL_DIROPS = 1 << 18;
        const HANDLE_KILLPRIV = 1 << 19;
        const POSIX_ACL = 1 << 20;
        const ABORT_ERROR = 1 << 21;
        const MAX_PAGES = 1 << 22;
        const CACHE_SYMLINKS = 1 << 23;
        const NO_OPENDIR_SUPPORT = 1 << 24;
        const EXPLICIT_INVAL_DATA = 1 << 25;
        const MAP_ALIGNMENT = 1 << 26;
        const SUBMOUNTS = 1 << 27;
        const HANDLE_KILLPRIV_V2 = 1 << 28;
        const SETXATTR_EXT = 1 << 29;
        const INIT_EXT     = 1 << 30;
        const INIT_RESERVED = 1 << 31;
        const SECURITY_CTX = 1 << 32;
        const HAS_INODE_DAX = 1 << 33;
        const CREATE_SUPP_GROUP = 1 << 34;
        const HAS_EXPIRE_ONLY = 1 << 35;
        const DIRECT_IO_ALLOW_MMAP = 1 << 36;
        const PASSTHROUGH = 1 << 37;
        const NO_EXPORT_SUPPORT = 1 << 38;
        const HAS_RESEND = 1 << 39;
        const DIRECT_IO_RELAX = Self::DIRECT_IO_ALLOW_MMAP.bits();
        const ALLOW_IDMAP = 1 << 40;
        const OVER_IO_URING = 1 << 41;
        const REQUEST_TIMEOUT = 1 << 42;
        const HAS_IO_URING_BUFPOOL = 1 << 43;
        const HAS_SYNCFS = 1 << 44;
        const PASSTHROUGH_V2 = 1 << 45;
    }
}
