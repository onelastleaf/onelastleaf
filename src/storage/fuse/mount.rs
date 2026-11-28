use super::session::SessionError;
use nix::{
    errno::Errno,
    fcntl::{FcntlArg, OFlag, fcntl},
    libc::{self, O_NONBLOCK},
    mount::{MntFlags, MsFlags, mount, umount2},
    unistd::{Gid, Uid},
};
use std::{
    fs::{self, File, OpenOptions},
    io,
    mem::{offset_of, size_of, zeroed},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStringExt, fs::OpenOptionsExt, net::UnixStream},
    },
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

/// Options used when mounting a FUSE session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MountOptions {
    /// Pass `default_permissions` to Linux to enable VFS permission checks.
    /// Linux also enables these checks when `FUSE_POSIX_ACL` is negotiated.
    pub default_permissions: bool,
}

pub(super) struct Mount {
    fsname: String,
    unmount_helper: Option<&'static str>,
}

impl Mount {
    pub(super) fn new(
        mount_point: &Path,
        filesystem_name: &str,
        options: MountOptions,
    ) -> Result<(Self, File), (SessionError, io::Error)> {
        let mount_point = fs::canonicalize(mount_point)
            .map_err(|error| (SessionError::ResolveMountPointFailed, error))?;
        let suffix: u32 = std::random::random(..);
        let fsname = format!("{filesystem_name}-{suffix:08x}");

        match Self::mount_direct(&mount_point, &fsname, options) {
            Ok(fuse_file) => Ok((
                Self {
                    fsname,
                    unmount_helper: None,
                },
                fuse_file,
            )),
            Err((reason, error)) if error.kind() == io::ErrorKind::PermissionDenied => {
                Self::mount_with_helper(&mount_point, fsname, options).map_err(|helper_error| {
                    let error = io::Error::new(
                        error.kind(),
                        format!(
                            "Direct FUSE mount failed: {error}. Helper mount failed: {helper_error}"
                        ),
                    );
                    (reason, error)
                })
            }
            Err(error) => Err(error),
        }
    }

    fn mount_direct(
        mount_point: &Path,
        fsname: &str,
        options: MountOptions,
    ) -> Result<File, (SessionError, io::Error)> {
        let fuse_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_NONBLOCK)
            .open("/dev/fuse")
            .map_err(|error| (SessionError::NoFuseFile, error))?;

        let mut mount_options = format!(
            "fd={},rootmode=40000,user_id={},group_id={}",
            fuse_file.as_raw_fd(),
            Uid::current().as_raw(),
            Gid::current().as_raw(),
        );
        if options.default_permissions {
            mount_options.push_str(",default_permissions");
        }

        mount(
            Some(fsname),
            mount_point,
            Some("fuse"),
            MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
            Some(mount_options.as_str()),
        )
        .map_err(|errno| {
            let reason = match errno {
                Errno::EACCES | Errno::EPERM => SessionError::MountPermissionDenied,
                Errno::EBUSY => SessionError::MountPointBusy,
                _ => SessionError::MountSyscallFailed,
            };
            (reason, errno.into())
        })?;

        Ok(fuse_file)
    }

    fn mount_with_helper(
        mount_point: &Path,
        fsname: String,
        options: MountOptions,
    ) -> io::Result<(Self, File)> {
        let mut mount_options = String::from("fsname=");
        for (index, character) in fsname.char_indices() {
            if matches!(character, ',' | '\\') {
                mount_options.push('\\');
            }
            mount_options.push(character);
            if character == ',' && fsname[index + 1..].starts_with("x-") {
                // Recent fusermount3 versions extract x-* options before unescaping fsname.
                mount_options.push('\\');
            }
        }
        mount_options.push_str(",nosuid,nodev");
        if options.default_permissions {
            mount_options.push_str(",default_permissions");
        }

        let mut helper = "fusermount3";
        loop {
            let (socket, helper_socket) = UnixStream::pair()?;
            // Pass the socket as fd 0 only in the child.
            let child = match Command::new(helper)
                .args(["-o", &mount_options, "--"])
                .arg(mount_point)
                .env("_FUSE_COMMFD", "0")
                .stdin(Stdio::from(OwnedFd::from(helper_socket)))
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(error)
                    if helper == "fusermount3" && error.kind() == io::ErrorKind::NotFound =>
                {
                    helper = "fusermount";
                    // libfuse 2 otherwise refuses to mount over a nonempty directory.
                    mount_options.push_str(",nonempty");
                    continue;
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("could not start {helper} ({error})"),
                    ));
                }
            };

            let mounted = Self {
                fsname,
                unmount_helper: Some(helper),
            };
            // The helper sends one byte and one fd. The socket keeps the fd until we receive it.
            let result = child.wait_with_output().and_then(|output| {
                check_helper_status(helper, output)?;
                let fuse_file = receive_fuse_file(&socket)?;
                let flags = OFlag::from_bits_retain(fcntl(&fuse_file, FcntlArg::F_GETFL)?);
                fcntl(&fuse_file, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
                Ok(fuse_file)
            });
            drop(socket);
            let fuse_file = result.map_err(|error| mounted.cleanup_after_error(error))?;
            return Ok((mounted, fuse_file));
        }
    }

    pub(super) fn current_mount_point(&self) -> Result<Option<PathBuf>, (SessionError, io::Error)> {
        let contents = fs::read("/proc/self/mountinfo")
            .map_err(|error| (SessionError::ReadMountInfoFailed, error))?;
        find_mount_point(&contents, &self.fsname)
    }

    pub(super) fn unmount(&self) -> Result<(), (SessionError, io::Error)> {
        let Some(mount_point) = self.current_mount_point()? else {
            return Ok(());
        };
        let result = if let Some(helper) = self.unmount_helper {
            Command::new(helper)
                .args(["-u", "-z", "--"])
                .arg(&mount_point)
                .output()
                .and_then(|output| check_helper_status(helper, output))
        } else {
            umount2(&mount_point, MntFlags::MNT_DETACH).map_err(io::Error::from)
        };
        result.map_err(|error| (SessionError::UnmountFailed, error))
    }

    pub(super) fn rollback(&self, fuse_file: File, error: io::Error) -> io::Error {
        drop(fuse_file);
        self.cleanup_after_error(error)
    }

    fn cleanup_after_error(&self, error: io::Error) -> io::Error {
        match self.unmount() {
            Ok(()) => error,
            Err((reason, unmount_error)) => io::Error::new(
                error.kind(),
                format!(
                    "{error}. Cleanup for {} failed. {reason} {unmount_error}",
                    self.fsname,
                ),
            ),
        }
    }
}

fn check_helper_status(helper: &str, output: Output) -> io::Result<()> {
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(io::Error::other(format!(
        "{helper} failed ({}). {}",
        output.status,
        stderr.trim(),
    )))
}

fn receive_fuse_file(socket: &UnixStream) -> io::Result<File> {
    #[repr(C)]
    struct ControlMessage {
        header: libc::cmsghdr,
        fd: RawFd,
    }

    // Exclude the tail padding to give the kernel room for exactly one descriptor.
    const CONTROL_LEN: usize = offset_of!(ControlMessage, fd) + size_of::<RawFd>();
    let mut byte = [0u8];
    let mut iovec = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: byte.len(),
    };
    // Both types contain only integers and pointers, which can be zero initialized.
    let mut control: ControlMessage = unsafe { zeroed() };
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut iovec;
    message.msg_iovlen = 1;
    message.msg_control = (&mut control as *mut ControlMessage).cast();
    let received = loop {
        message.msg_controllen = CONTROL_LEN as _;
        // The pointers and lengths refer to live, writable buffers for the duration of recvmsg.
        let received =
            unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_CMSG_CLOEXEC) };
        if received >= 0 {
            break received;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };

    // Linux can install a descriptor and set MSG_CTRUNC in the same receive. Take ownership of
    // the installed descriptor before checking for truncation so the error path also closes it.
    let fd = (message.msg_controllen as usize == CONTROL_LEN
        && control.header.cmsg_len as usize == CONTROL_LEN
        && control.header.cmsg_level == libc::SOL_SOCKET
        && control.header.cmsg_type == libc::SCM_RIGHTS
        && control.fd >= 0)
        .then(|| unsafe { OwnedFd::from_raw_fd(control.fd) });
    if message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mount helper's FUSE fd message was truncated",
        ));
    }
    if received == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "mount helper closed the socket without sending a FUSE fd",
        ));
    }
    fd.map(File::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "mount helper must send exactly one FUSE fd",
        )
    })
}

struct MountInfo<'a> {
    id: u64,
    parent_id: u64,
    root: &'a [u8],
    mount_point: PathBuf,
    filesystem_type: &'a [u8],
    source: &'a [u8],
}

impl<'a> MountInfo<'a> {
    fn parse(line: &'a [u8]) -> Option<Self> {
        let mut fields = line.split(|&byte| byte == b' ');
        let id = std::str::from_utf8(fields.next()?).ok()?.parse().ok()?;
        let parent_id = std::str::from_utf8(fields.next()?).ok()?.parse().ok()?;
        let root = fields.nth(1)?;
        let decoded_path = unescape_mountinfo_field(fields.next()?)?;
        let mount_point = PathBuf::from(std::ffi::OsString::from_vec(decoded_path));
        if !mount_point.is_absolute() {
            return None;
        }
        fields.next()?;
        fields.find(|field| *field == b"-")?;
        let filesystem_type = fields.next()?;
        let source = fields.next()?;
        fields.next()?;
        Some(Self {
            id,
            parent_id,
            root,
            mount_point,
            filesystem_type,
            source,
        })
    }
}

fn unescape_mountinfo_field(mut field: &[u8]) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(field.len());
    while let Some((&byte, remaining)) = field.split_first() {
        field = remaining;
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        let (escape, remaining) = field.split_at_checked(3)?;
        decoded.push(match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            b"043" => b'#',
            _ => return None,
        });
        field = remaining;
    }
    Some(decoded)
}

fn parse_mountinfo(contents: &[u8]) -> io::Result<Vec<MountInfo<'_>>> {
    let mut mounts = Vec::new();
    let lines = contents.strip_suffix(b"\n").unwrap_or(contents);
    if lines.is_empty() {
        return Ok(mounts);
    }
    for (index, line) in lines.split(|&byte| byte == b'\n').enumerate() {
        let entry = MountInfo::parse(line).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid mountinfo entry on line {}", index + 1),
            )
        })?;
        mounts.push(entry);
    }
    mounts.sort_unstable_by_key(|entry| entry.id);
    if mounts.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "duplicate mount ID",
        ));
    }
    Ok(mounts)
}

fn find_mount_point(
    contents: &[u8],
    fsname: &str,
) -> Result<Option<PathBuf>, (SessionError, io::Error)> {
    let parse_error = |error| (SessionError::ParseMountInfoFailed, error);
    let mounts = parse_mountinfo(contents).map_err(parse_error)?;
    let mut matched = None;
    for entry in &mounts {
        if entry.filesystem_type != b"fuse" || entry.root != b"/" {
            continue;
        }
        let source = unescape_mountinfo_field(entry.source).ok_or_else(|| {
            parse_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid FUSE mount source",
            ))
        })?;
        if source != fsname.as_bytes() {
            continue;
        }
        if matched.replace(entry).is_some() {
            return Err((
                SessionError::AmbiguousMountSource,
                io::Error::other(format!(
                    "multiple FUSE root mounts use the source {fsname:?}"
                )),
            ));
        }
    }
    let Some(target) = matched else {
        return Ok(None);
    };
    match topmost_mount(&mounts, &target.mount_point).map_err(parse_error)? {
        Some(visible) if visible.id == target.id => Ok(Some(target.mount_point.clone())),
        _ => Ok(None),
    }
}

fn topmost_mount<'mounts, 'data>(
    mounts: &'mounts [MountInfo<'data>],
    mount_point: &Path,
) -> io::Result<Option<&'mounts MountInfo<'data>>> {
    // Walk from the root to the target, following each stack on the visible parent. A mount
    // below a covered ancestor stays in mountinfo but cannot be reached through the path.
    let ancestors: Vec<_> = mount_point.ancestors().collect();
    let mut visible: Option<&MountInfo<'_>> = None;
    for path in ancestors.into_iter().rev() {
        loop {
            let mut children = mounts.iter().filter(|entry| {
                if entry.mount_point != path {
                    return false;
                }
                match visible {
                    Some(parent) => entry.parent_id == parent.id && entry.id != parent.id,
                    // A chroot can hide the mount containing the process's root.
                    None => {
                        entry.parent_id == entry.id
                            || mounts
                                .binary_search_by_key(&entry.parent_id, |parent| parent.id)
                                .is_err()
                    }
                }
            });
            let Some(child) = children.next() else {
                break;
            };
            if children.next().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ambiguous mount stack",
                ));
            }
            visible = Some(child);
        }
    }

    Ok(visible)
}
