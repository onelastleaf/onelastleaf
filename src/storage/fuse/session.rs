use super::{Flags, payload};
use nix::{
    mount::{MntFlags, MsFlags, mount, umount2},
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
    RequestBeforeInit,
    InconsistentInitState,
    UnsupportedOpcode(u32),
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

        let thread = thread::Builder::new().spawn(move || self.run(&state_in_thread))?;

        Ok(Handle { thread, state })
    }

    fn open_fuse_file() -> io::Result<File> {
        OpenOptions::new().read(true).write(true).open("/dev/fuse")
    }

    fn rollback_mount(&self, fuse_file: File, init_error: io::Error) -> io::Error {
        drop(fuse_file);

        match umount2(&self.mount_point, MntFlags::MNT_DETACH) {
            Ok(()) => init_error,
            Err(unmount_error) => io::Error::new(
                init_error.kind(),
                format!(
                    "{init_error}. Failed to unmount {}: {unmount_error}",
                    self.mount_point.display(),
                ),
            ),
        }
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

        let size = loop {
            match fuse_file.read(&mut buffer) {
                Ok(size) => break size,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    state.lock().stop_with_error(SessionError::CannotReadFuse);
                    return Err(self.rollback_mount(fuse_file, error));
                }
            }
        };

        buffer.truncate(size);

        let request = match self.parse_fuse_request(&buffer) {
            Ok(request) => request,
            Err(error) => {
                state
                    .lock()
                    .stop_with_error(SessionError::CannotParseRequest);
                let error = io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}"));
                return Err(self.rollback_mount(fuse_file, error));
            }
        };

        todo!()
    }

    fn parse_fuse_request<'a>(
        &self,
        request: &'a [u8],
    ) -> Result<payload::Payload<'a>, ParseError> {
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

        let payload = self.parse_fuse_request_payload(header.opcode, request_payload)?;

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
            pid: u32::from_ne_bytes(request_head[32..36].try_into().unwrap()),
            total_extlen: u16::from_ne_bytes(request_head[36..38].try_into().unwrap()),
        })
    }

    fn parse_fuse_request_payload<'a>(
        &self,
        opcode: u32,
        request_payload: &'a [u8],
    ) -> Result<payload::Payload<'a>, ParseError> {
        let (protocol_version, capabilities) =
            match (self.protocol_version, self.capabilities.as_ref()) {
                // If protocol_version and capabilities all None, so it must before initialized
                (None, None) => {
                    // FUSE_INIT is 26
                    if opcode == 26 {
                        return payload::Payload::parse_init(request_payload);
                    } else {
                        return Err(ParseError::RequestBeforeInit);
                    }
                }

                (Some(version), Some(flags)) => (version, flags),

                _ => return Err(ParseError::InconsistentInitState),
            };

        match opcode {
            5 | 17 | 38 if !request_payload.is_empty() => Err(ParseError::ExcessiveLength),
            1 => payload::Payload::parse_lookup(request_payload),
            2 => payload::Payload::parse_forget(request_payload),
            3 => payload::Payload::parse_getattr(request_payload, protocol_version),
            4 => payload::Payload::parse_setattr(request_payload, protocol_version),
            5 => payload::Payload::parse_readlink(),
            6 => payload::Payload::parse_symlink(request_payload),
            8 => payload::Payload::parse_mknod(request_payload, protocol_version),
            9 => payload::Payload::parse_mkdir(request_payload, protocol_version),
            10 => payload::Payload::parse_unlink(request_payload),
            11 => payload::Payload::parse_rmdir(request_payload),
            12 => payload::Payload::parse_rename(request_payload),
            13 => payload::Payload::parse_link(request_payload),
            14 => payload::Payload::parse_open(request_payload),
            15 => payload::Payload::parse_read(request_payload, protocol_version),
            16 => payload::Payload::parse_write(request_payload, protocol_version),
            17 => payload::Payload::parse_statfs(),
            18 => payload::Payload::parse_release(request_payload, protocol_version),
            20 => payload::Payload::parse_fsync(request_payload),
            21 => payload::Payload::parse_setxattr(request_payload, *capabilities),
            22 => payload::Payload::parse_getxattr(request_payload),
            23 => payload::Payload::parse_listxattr(request_payload),
            24 => payload::Payload::parse_removexattr(request_payload),
            25 => payload::Payload::parse_flush(request_payload, protocol_version),
            27 => payload::Payload::parse_opendir(request_payload),
            28 => payload::Payload::parse_readdir(request_payload, protocol_version),
            29 => payload::Payload::parse_releasedir(request_payload, protocol_version),
            30 => payload::Payload::parse_fsyncdir(request_payload),
            31 => payload::Payload::parse_getlk(request_payload, protocol_version),
            32 => payload::Payload::parse_setlk(request_payload, protocol_version),
            33 => payload::Payload::parse_setlkw(request_payload, protocol_version),
            34 => payload::Payload::parse_access(request_payload),
            35 => payload::Payload::parse_create(request_payload, protocol_version),
            36 => payload::Payload::parse_interrupt(request_payload),
            37 => payload::Payload::parse_bmap(request_payload),
            38 => payload::Payload::parse_destroy(),
            39 => payload::Payload::parse_ioctl(request_payload),
            40 => payload::Payload::parse_poll(request_payload, protocol_version),
            41 => payload::Payload::parse_notify_reply(request_payload),
            42 => payload::Payload::parse_batch_forget(request_payload),
            43 => payload::Payload::parse_fallocate(request_payload),
            44 => payload::Payload::parse_readdir_plus(request_payload),
            45 => payload::Payload::parse_rename2(request_payload),
            46 => payload::Payload::parse_lseek(request_payload),
            47 => payload::Payload::parse_copy_file_range(request_payload),
            48 => payload::Payload::parse_setupmapping(request_payload),
            49 => payload::Payload::parse_removemapping(request_payload),
            50 => payload::Payload::parse_syncfs(request_payload),
            51 => payload::Payload::parse_tmpfile(request_payload),
            52 => payload::Payload::parse_statx(request_payload),
            53 => payload::Payload::parse_copy_file_range64(request_payload),
            _ => Err(ParseError::UnsupportedOpcode(opcode)),
        }
    }
}

impl HandleState {
    fn stop_with_error(&mut self, error: SessionError) {
        self.status = Status::Stopped;
        self.error = Some(error);
    }
}
