use super::{Flags, helper, reply, request};
use nix::{
    errno::Errno,
    libc::{PATH_MAX, S_IFDIR, S_IFMT},
    mount::{MntFlags, MsFlags, mount, umount2},
    sys::utsname::uname,
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

use super::{FileSystem, InitConfig, LockType, SUPPORTED_PROTOCOL_MINOR};

const REPLY_HEADER_LEN: usize = 16;
// fuse_out_header.len includes both the header and the payload.
const MAX_REPLY_PAYLOAD_LEN: usize = u32::MAX as usize - REPLY_HEADER_LEN;
const LEGACY_DIRENT_NAME_MAX: usize = 1024;

/// Options used when mounting a FUSE session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MountOptions {
    /// Pass `default_permissions` to Linux to enable VFS permission checks.
    /// Linux also enables these checks when `FUSE_POSIX_ACL` is negotiated.
    pub default_permissions: bool,
}

pub struct Session<FS: FileSystem> {
    filesystem: Arc<FS>,
    mount_point: PathBuf,
    mount_options: MountOptions,
    protocol_version: Option<u32>,
    capabilities: Option<Flags>,
    dirent_name_max: usize,
}

pub struct Handle {
    pub thread: JoinHandle<io::Result<()>>,
    pub state: Arc<Mutex<HandleState>>,
}

pub struct HandleState {
    pub status: Status,
    pub error: Option<SessionError>,
    pub negotiated_config: Option<Flags>,
}

impl HandleState {
    fn stop_with_error(&mut self, error: SessionError) {
        self.status = Status::Stopped;
        self.error = Some(error);
    }
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
    InitFailed,
    UnknownError,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NoFuseFile => {
                "/dev/fuse cannot be opened. Please check possible permission settings."
            }
            Self::MountFailed => {
                "Failed to mount the FUSE filesystem. Please check mount permissions and the target path."
            }
            Self::CannotReadFuse => {
                "Failed to read data from /dev/fuse; the Linux kernel may have encountered a runtime issue."
            }
            Self::CannotParseRequest => {
                "Failed to parse the FUSE protocol request; the Linux kernel may have encountered a runtime issue."
            }
            Self::InitFailed => "Failed to initialize the FUSE session.",
            Self::UnknownError => {
                "The FUSE protocol stack failed to run because it encountered an unexpected error."
            }
        })
    }
}

impl std::error::Error for SessionError {}

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

impl std::fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientLength => formatter.write_str("FUSE request is too short"),
            Self::ExcessiveLength => formatter.write_str("FUSE request is too long"),
            Self::InvalidLength => {
                formatter.write_str("FUSE request length does not match its header")
            }
            Self::RequestBeforeInit => {
                formatter.write_str("received a non-INIT request before FUSE initialization")
            }
            Self::InvalidState => {
                formatter.write_str("FUSE session has inconsistent initialization state")
            }
            Self::UnsupportedOpcode(opcode) => {
                write!(formatter, "unsupported FUSE opcode {opcode}")
            }
            Self::UnexpectedOpcode(opcode) => write!(formatter, "unexpected FUSE opcode {opcode}"),
            Self::InvalidLockType(lock_type) => {
                write!(formatter, "invalid FUSE lock type {lock_type}")
            }
            Self::InvalidExtensions => formatter.write_str("malformed FUSE request extensions"),
            Self::UnexpectedExtensions => {
                formatter.write_str("FUSE request extensions do not match the expected layout")
            }
            Self::UnknownError => formatter.write_str("unknown error while parsing a FUSE request"),
        }
    }
}

impl std::error::Error for ParseError {}

impl<FS: FileSystem> Session<FS> {
    pub fn new(
        filesystem: Arc<FS>,
        mount_point: impl Into<PathBuf>,
        mount_options: MountOptions,
    ) -> Self {
        let dirent_name_max = uname()
            .ok()
            .and_then(|kernel| kernel.release().to_str().map(Self::dirent_name_limit))
            .unwrap_or(LEGACY_DIRENT_NAME_MAX);
        Self {
            filesystem,
            mount_point: mount_point.into(),
            mount_options,
            protocol_version: None,
            capabilities: None,
            dirent_name_max,
        }
    }

    fn dirent_name_limit(kernel_release: &str) -> usize {
        // Split the release returned by uname at version dots and suffix separators.
        // For example, 6.18.2-gentoo-dist starts with the components "6" and "18".
        let mut version = kernel_release.split(['.', '-', '+']);

        // Parse the first component as the major version. A missing or invalid part yields None.
        let major = version.next().and_then(|part| part.parse::<u32>().ok());

        // Parse the second component as the minor version. Later components are unused.
        let minor = version.next().and_then(|part| part.parse::<u32>().ok());

        // Select the directory entry name limit by comparing the numeric version pair.
        match (major, minor) {
            // Linux 6.15 raised FUSE_NAME_MAX for directory records to PATH_MAX - 1.
            (Some(major), Some(minor)) if (major, minor) >= (6, 15) => PATH_MAX as usize - 1,
            // Use 1024 bytes for older kernels or releases that could not be parsed.
            _ => LEGACY_DIRENT_NAME_MAX,
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
        let mut mount_options = format!(
            "fd={},rootmode=40000,user_id={},group_id={}",
            fuse_fd,
            Uid::current().as_raw(),
            Gid::current().as_raw(),
        );
        if self.mount_options.default_permissions {
            mount_options.push_str(",default_permissions");
        }

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

        let request::Payload::Init(init_payload) = &request.payload else {
            state
                .lock()
                .stop_with_error(SessionError::CannotParseRequest);
            let error = ParseError::UnexpectedOpcode(request.header.opcode);
            let error = io::Error::new(io::ErrorKind::InvalidData, error);
            return Err(self.rollback_mount(fuse_file, error));
        };

        if init_payload.major != 7 {
            state.lock().stop_with_error(SessionError::InitFailed);
            let error = io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "unsupported FUSE protocol major version {}",
                    init_payload.major
                ),
            );
            return Err(self.rollback_mount(fuse_file, error));
        }

        let mut init_config = match self.filesystem.init() {
            Ok(config) => config,
            Err(errno) => {
                state.lock().stop_with_error(SessionError::InitFailed);
                return Err(self.rollback_mount(fuse_file, errno.into()));
            }
        };

        let writeback_cache =
            (init_config.flags & init_payload.flags).contains(Flags::WRITEBACK_CACHE);
        if !(1..=2).contains(&init_config.max_stack_depth) || writeback_cache {
            init_config
                .flags
                .remove(Flags::PASSTHROUGH | Flags::PASSTHROUGH_V2);
            init_config.max_stack_depth = 0;
        }

        let (selected_config, init_reply) = Self::serialize_init(init_payload, init_config);

        // Linux rejects INIT if FUSE_ALLOW_IDMAP is set without VFS permission checks. Either
        // default_permissions at mount time or FUSE_POSIX_ACL in the INIT reply enables them.
        let default_permissions = self.mount_options.default_permissions
            || selected_config.flags.contains(Flags::POSIX_ACL);
        if selected_config.flags.contains(Flags::ALLOW_IDMAP) && !default_permissions {
            state.lock().stop_with_error(SessionError::InitFailed);
            let error = io::Error::new(
                io::ErrorKind::InvalidInput,
                "FUSE_ALLOW_IDMAP requires default_permissions or FUSE_POSIX_ACL",
            );
            return Err(self.rollback_mount(fuse_file, error));
        }

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
        use reply::Payload;

        let protocol_version = self.protocol_version.ok_or(Errno::EPROTO)?;
        let capabilities = self.capabilities.ok_or(Errno::EPROTO)?;
        let mut bytes = Vec::new();
        match payload {
            Payload::Empty => bytes = Self::serialize_empty(),
            Payload::Entry(entry) => {
                Self::serialize_entry(&mut bytes, &entry, protocol_version, capabilities)?
            }
            Payload::Attr(attr) => {
                Self::serialize_attr(&mut bytes, &attr, protocol_version, capabilities)?
            }
            Payload::Readlink(readlink) => bytes = Self::serialize_readlink(readlink),
            Payload::Open(open) => {
                Self::serialize_open(&mut bytes, &open, protocol_version, capabilities)?
            }
            Payload::Read(read) => bytes = Self::serialize_read(read),
            Payload::Write(write) => Self::serialize_write(&mut bytes, write),
            Payload::Statfs(statfs) => {
                Self::serialize_statfs(&mut bytes, &statfs, protocol_version)
            }
            Payload::Xattr(xattr) => bytes = Self::serialize_xattr(xattr),
            Payload::Readdir(readdir) => {
                Self::serialize_readdir(&mut bytes, &readdir, self.dirent_name_max)?
            }
            Payload::Lock(lock) => Self::serialize_lock(&mut bytes, &lock)?,
            Payload::Create(create) => {
                Self::serialize_create(&mut bytes, &create, protocol_version, capabilities)?
            }
            Payload::Bmap(bmap) => Self::serialize_bmap(&mut bytes, bmap),
            Payload::Ioctl(ioctl) => Self::serialize_ioctl(&mut bytes, ioctl)?,
            Payload::Poll(poll) => Self::serialize_poll(&mut bytes, poll),
            Payload::ReaddirPlus(readdir) => Self::serialize_readdir_plus(
                &mut bytes,
                &readdir,
                protocol_version,
                capabilities,
                self.dirent_name_max,
            )?,
            Payload::Lseek(lseek) => Self::serialize_lseek(&mut bytes, lseek),
            Payload::Statx(statx) => Self::serialize_statx(&mut bytes, statx),
            Payload::CopyFileRange64(copy) => Self::serialize_copy_file_range64(&mut bytes, copy),
        }

        if bytes.len() > MAX_REPLY_PAYLOAD_LEN {
            return Err(Errno::EOVERFLOW);
        }
        Ok(bytes)
    }

    // After confirming major version 7, encode the INIT reply and return the selected config.
    #[inline]
    fn serialize_init(
        kernel: &request::InitPayload,
        mut config: InitConfig,
    ) -> (InitConfig, Vec<u8>) {
        const MIN_MAX_WRITE: u32 = 4096;

        let minor = kernel
            .minor
            .min(config.protocol_version)
            .min(SUPPORTED_PROTOCOL_MINOR);
        config.protocol_version = minor;
        config.flags &= kernel.flags & Flags::at_minor(minor);
        if !config.flags.contains(Flags::INIT_EXT) {
            config.flags &= Flags::from_bits_truncate(u64::from(u32::MAX));
        }

        config.max_write = if minor < 5 {
            MIN_MAX_WRITE
        } else {
            config.max_write.max(MIN_MAX_WRITE)
        };
        if minor < 6 {
            config.max_readahead = 0;
        }
        if minor < 13 {
            config.max_background = 0;
            config.congestion_threshold = 0;
        }
        if minor < 23 {
            config.time_gran = 0;
        }

        if !config.flags.contains(Flags::MAX_PAGES) {
            config.max_pages = 0;
        }
        if !config.flags.contains(Flags::MAP_ALIGNMENT) {
            config.map_alignment = 0;
        }
        if !config
            .flags
            .intersects(Flags::PASSTHROUGH | Flags::PASSTHROUGH_V2)
        {
            config.max_stack_depth = 0;
        }
        if !config.flags.contains(Flags::REQUEST_TIMEOUT) {
            config.request_timeout = 0;
        }

        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&7u32.to_ne_bytes());
        bytes.extend_from_slice(&minor.to_ne_bytes());
        if minor < 5 {
            return (config, bytes);
        }

        let flags = config.flags.bits();
        bytes.extend_from_slice(&config.max_readahead.to_ne_bytes());
        // This word is reserved in FUSE 7.5 and should be zero; the 7.5 kernel ignores any flag
        // bits written here. Technically there is an issue here because the FUSE 7.5 kernel marks
        // bits 8–20 as unused, but valid values may be filled in there; since the FUSE kernel
        // ignores this value, it's not a big deal, so I won't change it.
        bytes.extend_from_slice(&(flags as u32).to_ne_bytes());
        bytes.extend_from_slice(&config.max_background.to_ne_bytes());
        bytes.extend_from_slice(&config.congestion_threshold.to_ne_bytes());
        bytes.extend_from_slice(&config.max_write.to_ne_bytes());
        if minor < 23 {
            return (config, bytes);
        }

        let flags2 = (flags >> 32) as u32;
        bytes.extend_from_slice(&config.time_gran.to_ne_bytes());
        bytes.extend_from_slice(&config.max_pages.to_ne_bytes());
        bytes.extend_from_slice(&config.map_alignment.to_ne_bytes());
        bytes.extend_from_slice(&flags2.to_ne_bytes());
        bytes.extend_from_slice(&config.max_stack_depth.to_ne_bytes());
        bytes.extend_from_slice(&config.request_timeout.to_ne_bytes());
        bytes.extend_from_slice(&[0; 22]);
        (config, bytes)
    }

    #[inline]
    fn serialize_empty() -> Vec<u8> {
        Vec::new()
    }

    #[inline]
    fn serialize_entry(
        bytes: &mut Vec<u8>,
        entry: &reply::EntryResult,
        protocol_version: u32,
        capabilities: Flags,
    ) -> Result<(), Errno> {
        bytes.extend_from_slice(&entry.nodeid.to_ne_bytes());
        bytes.extend_from_slice(&entry.generation.to_ne_bytes());
        bytes.extend_from_slice(&entry.entry_valid.to_ne_bytes());
        bytes.extend_from_slice(&entry.attr_valid.to_ne_bytes());
        bytes.extend_from_slice(&entry.entry_valid_nsec.to_ne_bytes());
        bytes.extend_from_slice(&entry.attr_valid_nsec.to_ne_bytes());
        Self::serialize_inode_attr(bytes, &entry.attr, protocol_version, capabilities)
    }

    #[inline]
    fn serialize_attr(
        bytes: &mut Vec<u8>,
        result: &reply::AttrResult,
        protocol_version: u32,
        capabilities: Flags,
    ) -> Result<(), Errno> {
        bytes.extend_from_slice(&result.attr_valid.to_ne_bytes());
        bytes.extend_from_slice(&result.attr_valid_nsec.to_ne_bytes());
        bytes.extend_from_slice(&[0; 4]);
        Self::serialize_inode_attr(bytes, &result.attr, protocol_version, capabilities)
    }

    #[inline]
    fn serialize_inode_attr(
        bytes: &mut Vec<u8>,
        attr: &reply::Attr,
        protocol_version: u32,
        capabilities: Flags,
    ) -> Result<(), Errno> {
        const FUSE_ATTR_SUBMOUNT: u32 = 1 << 0;

        if attr.flags & FUSE_ATTR_SUBMOUNT != 0
            && (protocol_version < 32
                || !capabilities.contains(Flags::SUBMOUNTS)
                || attr.mode & S_IFMT as u32 != S_IFDIR as u32)
        {
            return Err(Errno::EIO);
        }

        bytes.extend_from_slice(&attr.ino.to_ne_bytes());
        bytes.extend_from_slice(&attr.size.to_ne_bytes());
        bytes.extend_from_slice(&attr.blocks.to_ne_bytes());
        bytes.extend_from_slice(&attr.atime.to_ne_bytes());
        bytes.extend_from_slice(&attr.mtime.to_ne_bytes());
        bytes.extend_from_slice(&attr.ctime.to_ne_bytes());
        bytes.extend_from_slice(&attr.atimensec.to_ne_bytes());
        bytes.extend_from_slice(&attr.mtimensec.to_ne_bytes());
        bytes.extend_from_slice(&attr.ctimensec.to_ne_bytes());
        bytes.extend_from_slice(&attr.mode.to_ne_bytes());
        bytes.extend_from_slice(&attr.nlink.to_ne_bytes());
        bytes.extend_from_slice(&attr.uid.to_ne_bytes());
        bytes.extend_from_slice(&attr.gid.to_ne_bytes());
        bytes.extend_from_slice(&attr.rdev.to_ne_bytes());
        if protocol_version >= 9 {
            let flags = if protocol_version >= 32 {
                attr.flags
            } else {
                0
            };
            bytes.extend_from_slice(&attr.blksize.to_ne_bytes());
            bytes.extend_from_slice(&flags.to_ne_bytes());
        }
        Ok(())
    }

    #[inline]
    fn serialize_readlink(readlink: reply::ReadlinkResult) -> Vec<u8> {
        readlink.target
    }

    #[inline]
    fn serialize_open(
        bytes: &mut Vec<u8>,
        open: &reply::OpenResult,
        protocol_version: u32,
        capabilities: Flags,
    ) -> Result<(), Errno> {
        const FOPEN_DIRECT_IO: u32 = 1 << 0;
        const FOPEN_NOFLUSH: u32 = 1 << 5;
        const FOPEN_PARALLEL_DIRECT_WRITES: u32 = 1 << 6;
        const FOPEN_PASSTHROUGH: u32 = 1 << 7;
        const FOPEN_PASSTHROUGH_MASK: u32 =
            FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO | FOPEN_PARALLEL_DIRECT_WRITES | FOPEN_NOFLUSH;

        if open.backing_id_64 != 0 {
            return Err(Errno::EIO);
        }

        if open.open_flags & FOPEN_PASSTHROUGH != 0 {
            if protocol_version < 40
                || !capabilities.contains(Flags::PASSTHROUGH)
                || open.backing_id <= 0
                || open.open_flags & !FOPEN_PASSTHROUGH_MASK != 0
            {
                return Err(Errno::EIO);
            }
        } else if open.backing_id != 0 {
            return Err(Errno::EIO);
        }

        bytes.extend_from_slice(&open.fh.to_ne_bytes());
        bytes.extend_from_slice(&open.open_flags.to_ne_bytes());
        bytes.extend_from_slice(&open.backing_id.to_ne_bytes());
        Ok(())
    }

    #[inline]
    fn serialize_read(read: reply::ReadResult) -> Vec<u8> {
        read.data
    }

    #[inline]
    fn serialize_write(bytes: &mut Vec<u8>, write: reply::WriteResult) {
        bytes.extend_from_slice(&write.size.to_ne_bytes());
        bytes.extend_from_slice(&[0; 4]);
    }

    #[inline]
    fn serialize_statfs(bytes: &mut Vec<u8>, statfs: &reply::StatfsResult, protocol_version: u32) {
        let stats = &statfs.stats;
        bytes.extend_from_slice(&stats.blocks.to_ne_bytes());
        bytes.extend_from_slice(&stats.free_blocks.to_ne_bytes());
        bytes.extend_from_slice(&stats.available_blocks.to_ne_bytes());
        bytes.extend_from_slice(&stats.files.to_ne_bytes());
        bytes.extend_from_slice(&stats.free_inodes.to_ne_bytes());
        bytes.extend_from_slice(&stats.block_size.to_ne_bytes());
        bytes.extend_from_slice(&stats.namelen.to_ne_bytes());
        if protocol_version >= 4 {
            bytes.extend_from_slice(&stats.fragment_size.to_ne_bytes());
            bytes.extend_from_slice(&[0; 28]);
        }
    }

    #[inline]
    fn serialize_xattr(xattr: reply::XattrResult) -> Vec<u8> {
        match xattr {
            reply::XattrResult::Size(size) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.extend_from_slice(&size.to_ne_bytes());
                bytes.extend_from_slice(&[0; 4]);
                bytes
            }
            reply::XattrResult::Data(data) => data,
        }
    }

    #[inline]
    fn serialize_readdir(
        bytes: &mut Vec<u8>,
        readdir: &reply::ReaddirResult,
        max_name_len: usize,
    ) -> Result<(), Errno> {
        for entry in &readdir.entries {
            Self::serialize_dirent(bytes, entry, max_name_len)?;
        }
        Ok(())
    }

    #[inline]
    fn serialize_lock(bytes: &mut Vec<u8>, result: &reply::LockResult) -> Result<(), Errno> {
        let lock = &result.lock;
        if lock.lock_type != LockType::Unlock
            && (lock.start > lock.end || lock.end > i64::MAX as u64)
        {
            return Err(Errno::EIO);
        }
        bytes.extend_from_slice(&lock.start.to_ne_bytes());
        bytes.extend_from_slice(&lock.end.to_ne_bytes());
        bytes.extend_from_slice(&(lock.lock_type as u32).to_ne_bytes());
        bytes.extend_from_slice(&lock.pid.to_ne_bytes());
        Ok(())
    }

    #[inline]
    fn serialize_create(
        bytes: &mut Vec<u8>,
        create: &reply::CreateResult,
        protocol_version: u32,
        capabilities: Flags,
    ) -> Result<(), Errno> {
        Self::serialize_entry(bytes, &create.entry, protocol_version, capabilities)?;
        Self::serialize_open(bytes, &create.open, protocol_version, capabilities)
    }

    #[inline]
    fn serialize_bmap(bytes: &mut Vec<u8>, bmap: reply::BmapResult) {
        bytes.extend_from_slice(&bmap.block.to_ne_bytes());
    }

    #[inline]
    fn serialize_ioctl(bytes: &mut Vec<u8>, ioctl: reply::IoctlResult) -> Result<(), Errno> {
        const IOCTL_OUT_LEN: usize = 16;

        // Regular FUSE file and directory ioctls do not allow FUSE_IOCTL_RETRY.
        let reply::IoctlResult::Complete { result, data } = ioctl else {
            return Err(Errno::EIO);
        };
        if data.len() > MAX_REPLY_PAYLOAD_LEN - IOCTL_OUT_LEN {
            return Err(Errno::EOVERFLOW);
        }
        bytes
            .try_reserve_exact(IOCTL_OUT_LEN + data.len())
            .map_err(|_| Errno::ENOMEM)?;
        bytes.extend_from_slice(&result.to_ne_bytes());
        bytes.extend_from_slice(&[0; 12]);
        bytes.extend_from_slice(&data);
        Ok(())
    }

    #[inline]
    fn serialize_poll(bytes: &mut Vec<u8>, poll: reply::PollResult) {
        bytes.extend_from_slice(&poll.revents.bits().to_ne_bytes());
        bytes.extend_from_slice(&[0; 4]);
    }

    #[inline]
    fn serialize_readdir_plus(
        bytes: &mut Vec<u8>,
        readdir: &reply::ReaddirPlusResult,
        protocol_version: u32,
        capabilities: Flags,
        max_name_len: usize,
    ) -> Result<(), Errno> {
        if protocol_version < 21 || !capabilities.contains(Flags::DO_READDIRPLUS) {
            return Err(Errno::EIO);
        }
        for entry in &readdir.entries {
            match &entry.entry {
                Some(inode) if inode.nodeid != 0 => {
                    Self::serialize_entry(bytes, inode, protocol_version, capabilities)?
                }
                Some(_) => return Err(Errno::EIO),
                None => bytes.extend_from_slice(&[0; 128]),
            }
            Self::serialize_dirent(bytes, &entry.dirent, max_name_len)?;
        }
        Ok(())
    }

    #[inline]
    fn serialize_lseek(bytes: &mut Vec<u8>, lseek: reply::LseekResult) {
        bytes.extend_from_slice(&lseek.offset.to_ne_bytes());
    }

    #[inline]
    fn serialize_statx(bytes: &mut Vec<u8>, statx: reply::StatxResult) {
        bytes.extend_from_slice(&statx.attr_valid.to_ne_bytes());
        bytes.extend_from_slice(&statx.attr_valid_nsec.to_ne_bytes());
        bytes.extend_from_slice(&[0; 20]);

        let stat = statx.stat;
        bytes.extend_from_slice(&stat.mask.to_ne_bytes());
        bytes.extend_from_slice(&stat.blksize.to_ne_bytes());
        bytes.extend_from_slice(&stat.attributes.to_ne_bytes());
        bytes.extend_from_slice(&stat.nlink.to_ne_bytes());
        bytes.extend_from_slice(&stat.uid.to_ne_bytes());
        bytes.extend_from_slice(&stat.gid.to_ne_bytes());
        bytes.extend_from_slice(&stat.mode.to_ne_bytes());
        bytes.extend_from_slice(&[0; 2]);
        bytes.extend_from_slice(&stat.ino.to_ne_bytes());
        bytes.extend_from_slice(&stat.size.to_ne_bytes());
        bytes.extend_from_slice(&stat.blocks.to_ne_bytes());
        bytes.extend_from_slice(&stat.attributes_mask.to_ne_bytes());
        for time in [stat.atime, stat.btime, stat.ctime, stat.mtime] {
            bytes.extend_from_slice(&time.tv_sec.to_ne_bytes());
            bytes.extend_from_slice(&time.tv_nsec.to_ne_bytes());
            bytes.extend_from_slice(&[0; 4]);
        }
        bytes.extend_from_slice(&stat.rdev_major.to_ne_bytes());
        bytes.extend_from_slice(&stat.rdev_minor.to_ne_bytes());
        bytes.extend_from_slice(&stat.dev_major.to_ne_bytes());
        bytes.extend_from_slice(&stat.dev_minor.to_ne_bytes());
        bytes.extend_from_slice(&[0; 112]);
    }

    #[inline]
    fn serialize_copy_file_range64(bytes: &mut Vec<u8>, copy: reply::CopyFileRangeResult) {
        bytes.extend_from_slice(&copy.bytes_copied.to_ne_bytes());
    }

    #[inline]
    fn serialize_dirent(
        bytes: &mut Vec<u8>,
        entry: &reply::DirEntry,
        max_name_len: usize,
    ) -> Result<(), Errno> {
        const DIRENT_HEADER_LEN: usize = 24;

        if entry.name.is_empty()
            || entry.name.len() > max_name_len
            || entry.name.contains(&0)
            || entry.name.contains(&b'/')
        {
            return Err(Errno::EIO);
        }
        let name_len = u32::try_from(entry.name.len()).map_err(|_| Errno::EOVERFLOW)?;
        let record_end = bytes
            .len()
            .checked_add(DIRENT_HEADER_LEN)
            .and_then(|end| end.checked_add(entry.name.len()))
            .and_then(|end| end.checked_next_multiple_of(8))
            .filter(|&end| end <= MAX_REPLY_PAYLOAD_LEN)
            .ok_or(Errno::EOVERFLOW)?;

        bytes
            .try_reserve(record_end - bytes.len())
            .map_err(|_| Errno::ENOMEM)?;
        bytes.extend_from_slice(&entry.ino.to_ne_bytes());
        bytes.extend_from_slice(&entry.offset.to_ne_bytes());
        bytes.extend_from_slice(&name_len.to_ne_bytes());
        bytes.extend_from_slice(&entry.file_type.to_ne_bytes());
        bytes.extend_from_slice(&entry.name);
        bytes.resize(record_end, 0);
        Ok(())
    }

    fn write_reply(
        fuse_file: &mut File,
        unique: u64,
        result: Result<&[u8], Errno>,
    ) -> io::Result<()> {
        todo!()
    }
}
