use super::session::SessionError;
use nix::{
    errno::Errno,
    libc::O_NONBLOCK,
    mount::{MntFlags, MsFlags, mount, umount2},
    unistd::{Gid, Uid},
};
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStringExt, fs::OpenOptionsExt},
    },
    path::{Path, PathBuf},
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
}

impl Mount {
    pub(super) fn new(
        mount_point: &Path,
        filesystem_name: &str,
        options: MountOptions,
    ) -> Result<(Self, File), (SessionError, io::Error)> {
        let fuse_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_NONBLOCK)
            .open("/dev/fuse")
            .map_err(|error| (SessionError::NoFuseFile, error))?;

        let mount_point = fs::canonicalize(mount_point)
            .map_err(|error| (SessionError::ResolveMountPointFailed, error))?;
        let suffix: u32 = std::random::random(..);
        let fsname = format!("{filesystem_name}-{suffix:08x}");

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
            Some(fsname.as_str()),
            &mount_point,
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

        Ok((Self { fsname }, fuse_file))
    }

    pub(super) fn current_mount_point(&self) -> Result<Option<PathBuf>, (SessionError, io::Error)> {
        let contents = fs::read("/proc/self/mountinfo")
            .map_err(|error| (SessionError::ReadMountInfoFailed, error))?;
        find_mount_point(&contents, &self.fsname)
    }

    pub(super) fn unmount(&self) -> Result<(), (SessionError, io::Error)> {
        if let Some(mount_point) = self.current_mount_point()? {
            umount2(&mount_point, MntFlags::MNT_DETACH)
                .map_err(|errno| (SessionError::UnmountFailed, errno.into()))?;
        }
        Ok(())
    }

    pub(super) fn rollback(&self, fuse_file: File, error: io::Error) -> io::Error {
        drop(fuse_file);

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
