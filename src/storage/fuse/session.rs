use super::{Flags, helper, reply, request};
use nix::{
    errno::Errno,
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

use super::{FileSystem, InitConfig};


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
    // The request length is too short
    InsufficientLength,

    // The request length is too long
    ExcessiveLength,

    // The request length can't be this length, no matter long or short
    InvalidLength,

    // It's pretty self-explanatory. Its name bro
    RequestBeforeInit,

    // Some internal invariants have been violated
    InvalidState,

    // Maybe this opcode is used in newer kernel, but we haven't support yet
    UnsupportedOpcode(u32),

    // This opcode is totally unexpected, it shouldn't appear in such place
    UnexpectedOpcode(u32),

    // There is never this LockType: not F_RDLCK, F_WRLCK or F_UNLCK
    InvalidLockType(u32),

    // We just can't decode the extensions bytes in request
    InvalidExtensions,

    // We can decode the extensions bytes, but its parse result doesn't make sense
    UnexpectedExtensions,

    // We don't know what happen
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
            Some(self.filesystem.get_name().as_str()), // Filesystem name
            &self.mount_point,                         // Mount point
            Some("fuse"),                              // Filesystem type
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
    ) -> Result<request::Request<'a>, ParseError> {
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

        let has_security_ctx: bool = self
            .capabilities
            .is_some_and(|flags| flags.contains(Flags::SECURITY_CTX));

        if matches!(
            (self.protocol_version, header.opcode),
            (Some(36 | 37), 6 | 8 | 9 | 35) | (Some(37), 51)
        ) && has_security_ctx
            && header.total_extlen == 0
        {
            // The legacy layout places the security context after the fixed fields
            // and NUL-terminated strings of the request body.
            let (jumped_len, string_count) = match header.opcode {
                6 => (0, 2),            // SYMLINK has a link name and target.
                8 | 35 | 51 => (16, 1), // MKNOD, CREATE, and TMPFILE each have one name.
                9 => (8, 1),            // MKDIR has one name.
                opcode => return Err(ParseError::UnexpectedOpcode(opcode)),
            };

            // Find the end of the request payload.
            let mut cursor = helper::Cursor::new(request_payload);
            cursor.skip(jumped_len)?;
            for _ in 0..string_count {
                cursor.read_until_nul()?;
            }

            // Remaining bytes have security contexts.
            let payload_end = request_payload.len() - cursor.bytes.len();
            let (body, extension_bytes) = request_payload.split_at(payload_end);

            let payload = self.parse_fuse_request_payload(header.opcode, body)?;
            let extensions = Self::parse_fuse_request_extensions(extension_bytes)
                .ok_or(ParseError::InvalidExtensions)?;

            // This layout requires a security context block and permits zero entries.
            if extensions.security_contexts.is_none() || extensions.supplementary_groups.is_some() {
                return Err(ParseError::UnexpectedExtensions);
            }

            Ok(request::Request {
                header,
                payload,
                extensions: Some(extensions),
            })
        } else {
            let payload: request::Payload =
                self.parse_fuse_request_payload(header.opcode, request_payload)?;

            let extensions = if request_extensions.is_empty() {
                None
            } else {
                Some(
                    Self::parse_fuse_request_extensions(request_extensions)
                        .ok_or(ParseError::InvalidExtensions)?,
                )
            };

            Ok(request::Request {
                header,
                payload,
                extensions,
            })
        }
    }

    fn parse_fuse_request_head(request_head: &[u8]) -> Option<request::Header> {
        if request_head.len() != 40 {
            return None;
        };

        Some(request::Header {
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
    ) -> Result<request::Payload<'a>, ParseError> {
        let (protocol_version, capabilities) =
            match (self.protocol_version, self.capabilities.as_ref()) {
                // If protocol_version and capabilities all None, so it must before initialized
                (None, None) => {
                    // FUSE_INIT is 26
                    if opcode == 26 {
                        return request::Payload::parse_init(request_payload);
                    } else {
                        return Err(ParseError::RequestBeforeInit);
                    }
                }

                (Some(version), Some(flags)) => (version, flags),

                _ => return Err(ParseError::InvalidState),
            };

        match opcode {
            5 | 17 | 38 if !request_payload.is_empty() => Err(ParseError::ExcessiveLength),
            1 => request::Payload::parse_lookup(request_payload),
            2 => request::Payload::parse_forget(request_payload),
            3 => request::Payload::parse_getattr(request_payload, protocol_version),
            4 => request::Payload::parse_setattr(request_payload, protocol_version),
            5 => request::Payload::parse_readlink(),
            6 => request::Payload::parse_symlink(request_payload),
            8 => request::Payload::parse_mknod(request_payload, protocol_version),
            9 => request::Payload::parse_mkdir(request_payload, protocol_version),
            10 => request::Payload::parse_unlink(request_payload),
            11 => request::Payload::parse_rmdir(request_payload),
            12 => request::Payload::parse_rename(request_payload),
            13 => request::Payload::parse_link(request_payload),
            14 => request::Payload::parse_open(request_payload),
            15 => request::Payload::parse_read(request_payload, protocol_version),
            16 => request::Payload::parse_write(request_payload, protocol_version),
            17 => request::Payload::parse_statfs(),
            18 => request::Payload::parse_release(request_payload, protocol_version),
            20 => request::Payload::parse_fsync(request_payload),
            21 => request::Payload::parse_setxattr(request_payload, *capabilities),
            22 => request::Payload::parse_getxattr(request_payload),
            23 => request::Payload::parse_listxattr(request_payload),
            24 => request::Payload::parse_removexattr(request_payload),
            25 => request::Payload::parse_flush(request_payload, protocol_version),
            27 => request::Payload::parse_opendir(request_payload),
            28 => request::Payload::parse_readdir(request_payload, protocol_version),
            29 => request::Payload::parse_releasedir(request_payload, protocol_version),
            30 => request::Payload::parse_fsyncdir(request_payload),
            31 => request::Payload::parse_getlk(request_payload, protocol_version),
            32 => request::Payload::parse_setlk(request_payload, protocol_version),
            33 => request::Payload::parse_setlkw(request_payload, protocol_version),
            34 => request::Payload::parse_access(request_payload),
            35 => request::Payload::parse_create(request_payload, protocol_version),
            36 => request::Payload::parse_interrupt(request_payload),
            37 => request::Payload::parse_bmap(request_payload),
            38 => request::Payload::parse_destroy(),
            39 => request::Payload::parse_ioctl(request_payload),
            40 => request::Payload::parse_poll(request_payload, protocol_version),
            41 => request::Payload::parse_notify_reply(request_payload),
            42 => request::Payload::parse_batch_forget(request_payload),
            43 => request::Payload::parse_fallocate(request_payload),
            44 => request::Payload::parse_readdir_plus(request_payload),
            45 => request::Payload::parse_rename2(request_payload),
            46 => request::Payload::parse_lseek(request_payload),
            47 => request::Payload::parse_copy_file_range(request_payload),
            48 => request::Payload::parse_setupmapping(request_payload),
            49 => request::Payload::parse_removemapping(request_payload),
            50 => request::Payload::parse_syncfs(request_payload),
            51 => request::Payload::parse_tmpfile(request_payload),
            52 => request::Payload::parse_statx(request_payload),
            53 => request::Payload::parse_copy_file_range64(request_payload),
            _ => Err(ParseError::UnsupportedOpcode(opcode)),
        }
    }

    fn parse_fuse_request_extensions<'a>(
        request_extensions: &'a [u8],
    ) -> Option<request::Extensions<'a>> {
        const FUSE_MAX_NR_SECCTX: u32 = 31;
        const FUSE_EXT_GROUPS: u32 = 32;

        let mut cursor = helper::Cursor::new(request_extensions);
        let mut extensions = request::Extensions {
            security_contexts: None,
            supplementary_groups: None,
        };

        while !cursor.finish() {
            let size = cursor.u32().ok()? as usize;
            let kind = cursor.u32().ok()?;
            if !size.is_multiple_of(8) {
                return None;
            }
            let mut extension = helper::Cursor::new(cursor.take(size.checked_sub(8)?).ok()?);

            match kind {
                0..=FUSE_MAX_NR_SECCTX if extensions.security_contexts.is_none() => {
                    let mut contexts = Vec::with_capacity(kind as usize);
                    for _ in 0..kind {
                        let label_len = extension.u32().ok()? as usize;
                        extension.skip(4).ok()?;
                        let name = extension.read_until_nul().ok()?;
                        let label = extension.take(label_len).ok()?;

                        let entry_len = 8 + name.len() + 1 + label_len;
                        extension
                            .skip(entry_len.next_multiple_of(8) - entry_len)
                            .ok()?;
                        contexts.push(request::SecurityContext { name, label });
                    }
                    extensions.security_contexts = Some(contexts);
                }
                FUSE_EXT_GROUPS if extensions.supplementary_groups.is_none() => {
                    let group_count = extension.u32().ok()? as usize;
                    let groups_len = group_count.checked_mul(4)?;
                    let groups = extension.take(groups_len).ok()?;
                    let data_len = 4 + groups_len;
                    extension
                        .skip(data_len.next_multiple_of(8) - data_len)
                        .ok()?;

                    let (groups, _) = groups.as_chunks::<4>();
                    extensions.supplementary_groups =
                        Some(groups.iter().copied().map(u32::from_ne_bytes).collect());
                }
                _ => return None,
            }

            if !extension.finish() {
                return None;
            }
        }

        Some(extensions)
    }

    fn serialize_reply(&self, payload: reply::Payload) -> Result<Vec<u8>, Errno> {
        todo!()
    }

    // Encode the agreed INIT values before publishing them on the session.
    #[inline]
    fn serialize_init(config: &InitConfig) -> Vec<u8> {
        let minor = config.protocol_version;
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&7u32.to_ne_bytes());
        bytes.extend_from_slice(&minor.to_ne_bytes());
        if minor < 5 {
            return bytes;
        }

        let flags = config.flags.bits();
        let max_readahead = if minor >= 6 { config.max_readahead } else { 0 };
        bytes.extend_from_slice(&max_readahead.to_ne_bytes());
        // This word is reserved in FUSE 7.5 and should be zero; the 7.5 kernel ignores any flag
        // bits written here. Technically there is an issue here because the FUSE 7.5 kernel marks
        // bits 8–20 as unused, but valid values may be filled in there; since the FUSE kernel
        // ignores this value, it's not a big deal, so I won't change it.
        bytes.extend_from_slice(&(flags as u32).to_ne_bytes());
        if minor >= 13 {
            bytes.extend_from_slice(&config.max_background.to_ne_bytes());
            bytes.extend_from_slice(&config.congestion_threshold.to_ne_bytes());
        } else {
            bytes.extend_from_slice(&[0; 4]);
        }
        bytes.extend_from_slice(&config.max_write.to_ne_bytes());
        if minor < 23 {
            return bytes;
        }

        let max_pages = if minor >= 28 && config.flags.contains(Flags::MAX_PAGES) {
            config.max_pages
        } else {
            0
        };
        let map_alignment = if minor >= 31 && config.flags.contains(Flags::MAP_ALIGNMENT) {
            config.map_alignment
        } else {
            0
        };
        let flags2 = if minor >= 36 && config.flags.contains(Flags::INIT_EXT) {
            (flags >> 32) as u32
        } else {
            0
        };
        let max_stack_depth = if minor >= 40 && config.flags.contains(Flags::PASSTHROUGH) {
            config.max_stack_depth
        } else {
            0
        };
        let request_timeout = if minor >= 43 && config.flags.contains(Flags::REQUEST_TIMEOUT) {
            config.request_timeout
        } else {
            0
        };
        bytes.extend_from_slice(&config.time_gran.to_ne_bytes());
        bytes.extend_from_slice(&max_pages.to_ne_bytes());
        bytes.extend_from_slice(&map_alignment.to_ne_bytes());
        bytes.extend_from_slice(&flags2.to_ne_bytes());
        bytes.extend_from_slice(&max_stack_depth.to_ne_bytes());
        bytes.extend_from_slice(&request_timeout.to_ne_bytes());
        bytes.extend_from_slice(&[0; 22]);
        bytes
    }

    fn write_reply(
        fuse_file: &mut File,
        unique: u64,
        result: Result<&[u8], Errno>,
    ) -> io::Result<()> {
        todo!()
    }
}

impl HandleState {
    fn stop_with_error(&mut self, error: SessionError) {
        self.status = Status::Stopped;
        self.error = Some(error);
    }
}
