use nix::libc;

/// The corresponding C structure is `fuse_entry_out`.
/// ```c
/// struct fuse_entry_out {
///     uint64_t nodeid, generation, entry_valid, attr_valid;
///     uint32_t entry_valid_nsec, attr_valid_nsec;
///     struct fuse_attr attr;
/// };
/// ```
/// Used by `FUSE_LOOKUP`, `FUSE_MKNOD`, `FUSE_MKDIR`, `FUSE_SYMLINK`, and `FUSE_LINK`. It also
/// appears in [`CreateResult`] and [`DirEntryPlus`].
///
/// The kernel addresses the inode by `nodeid`. Use a new `generation` each time you reuse a node
/// ID for a different inode.
///
/// `entry_valid` and `entry_valid_nsec` give the cache lifetime of the name lookup in seconds and
/// nanoseconds. For a `FUSE_LOOKUP` miss, return `nodeid` set to zero to let the kernel cache the
/// missing name for that duration. `attr_valid` and `attr_valid_nsec` give the separate cache
/// lifetime of `attr` in the same units.
///
/// Protocol versions before 7.9 use only the first 120 bytes of `fuse_entry_out`.
pub struct EntryResult {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

/// The corresponding C structure is `fuse_attr_out`.
/// ```c
/// struct fuse_attr_out {
///     uint64_t attr_valid;
///     uint32_t attr_valid_nsec, dummy;
///     struct fuse_attr attr;
/// };
/// ```
/// Used by `FUSE_GETATTR` and `FUSE_SETATTR`. `attr_valid` and `attr_valid_nsec` give the kernel's
/// cache lifetime for `attr` in seconds and nanoseconds. Encode `dummy` as zero.
///
/// Protocol versions before 7.9 use only the first 96 bytes of `fuse_attr_out`.
pub struct AttrResult {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

/// The corresponding C structure is `fuse_attr`.
/// ```c
/// struct fuse_attr {
///     uint64_t ino, size, blocks, atime, mtime, ctime;
///     uint32_t atimensec, mtimensec, ctimensec, mode, nlink, uid, gid, rdev, blksize, flags;
/// };
/// ```
/// Inode attributes carried by [`EntryResult`] and [`AttrResult`]. `ino` is the inode number
/// reported by `stat(2)`. `mode` holds the file type and permission bits. `flags` holds
/// `FUSE_ATTR_*` bits.
///
/// `atime`, `mtime`, and `ctime` are the access, modification, and status change times in seconds
/// since the Unix epoch. Each has a matching nanosecond field.
pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

/// The reply payload of `FUSE_READLINK` is `target` as a byte sequence.
/// The kernel adds the terminating NUL after it receives the payload.
/// You needn't add `\0` at end.
pub struct ReadlinkResult {
    pub target: Vec<u8>,
}

/// The corresponding C structure is `fuse_open_out`.
/// ```c
/// struct fuse_open_out {
///     uint64_t fh;
///     uint32_t open_flags;
///     int32_t backing_id;
///     uint64_t backing_id_64;
/// };
/// ```
/// Used by `FUSE_OPEN` and `FUSE_OPENDIR`.
/// Protocol versions before 7.47 use only the first 16 bytes. Before 7.40, the `backing_id` slot is
/// padding.
pub struct OpenResult {
    pub fh: u64,
    pub open_flags: u32,
    pub backing_id: i32,
    pub backing_id_64: u64,
}

/// The reply payload of `FUSE_READ` is `data` exactly as read, up to the `size` requested in
/// `fuse_read_in`. The reply header carries the total length.
/// You needn't add `\0` at end, as well.
pub struct ReadResult {
    pub data: Vec<u8>,
}

/// The corresponding C structure is `fuse_write_out`.
/// ```c
/// struct fuse_write_out {
///     uint32_t size;
///     uint32_t padding;
/// };
/// ```
/// Used by `FUSE_WRITE` and `FUSE_COPY_FILE_RANGE`. `size` is the number of bytes written or
/// copied. Encode `padding` as zero.
pub struct WriteResult {
    pub size: u32,
}

/// The corresponding C structure is `fuse_statfs_out`.
/// ```c
/// struct fuse_statfs_out {
///     struct fuse_kstatfs st;
/// };
/// ```
/// Used by `FUSE_STATFS`. The wire field `st` is represented by `stats`. Protocol versions before
/// 7.4 use the first 48 bytes of the 80-byte payload.
pub struct StatfsResult {
    pub stats: KernelStatfs,
}

/// The corresponding C structure is `fuse_kstatfs`.
/// ```c
/// struct fuse_kstatfs {
///     uint64_t blocks, bfree, bavail, files, ffree;
///     uint32_t bsize, namelen, frsize, padding;
///     uint32_t spare[6];
/// };
/// ```
/// `free_blocks` counts all free blocks. `available_blocks` counts those available to unprivileged
/// users. Encode `padding` and `spare` as zero.
pub struct KernelStatfs {
    pub blocks: u64,
    pub free_blocks: u64,
    pub available_blocks: u64,
    pub files: u64,
    pub free_inodes: u64,
    pub block_size: u32,
    pub namelen: u32,
    pub fragment_size: u32,
}

/// Used by `FUSE_GETXATTR` and `FUSE_LISTXATTR`.
/// When the request's `size` is zero, encode `Size` as `fuse_getxattr_out`.
/// ```c
/// struct fuse_getxattr_out {
///     uint32_t size;
///     uint32_t padding;
/// };
/// ```
/// Encode `padding` as zero.
/// If request size > 0, send `Data` as the reply payload if it fits in the requested buffer.
/// Reply with `ERANGE` if the buffer is too small.
pub enum XattrResult {
    /// The minimum buffer size, in bytes, needed to read the full result.
    /// For `FUSE_LISTXATTR`, count one terminating NUL byte per name.
    Size(u32),
    /// Raw value bytes for `FUSE_GETXATTR` or concatenated NUL-terminated attribute names for
    /// `FUSE_LISTXATTR`.
    Data(Vec<u8>),
}

/// The reply payload of `FUSE_READDIR` is a sequence of `fuse_dirent` records. The encoded entries
/// must fit within the `size` requested in `fuse_read_in`.
pub struct ReaddirResult {
    pub entries: Vec<DirEntry>,
}

/// The corresponding C structure is `fuse_dirent`.
/// ```c
/// struct fuse_dirent {
///     uint64_t ino, off;
///     uint32_t namelen, type;
///     char name[];
/// };
/// ```
/// The wire field `off` holds the position to resume reading after this entry. Encode `namelen`
/// from `name.len()`, then write the name bytes and pad the record with zeroes to an 8-byte
/// boundary. `file_type` is a `DT_*` value.
pub struct DirEntry {
    pub ino: u64,
    pub offset: u64,
    pub file_type: u32,
    pub name: Vec<u8>,
}

/// The corresponding C structure is `fuse_lk_out`.
/// ```c
/// struct fuse_lk_out {
///     struct fuse_file_lock lk;
/// };
/// ```
/// Used by `FUSE_GETLK`. Return the conflicting lock, or use
/// [`LockType::Unlock`](super::super::LockType::Unlock) when there is no conflict.
pub struct LockResult {
    pub lock: super::super::FileLock,
}

/// Used by `FUSE_CREATE` and `FUSE_TMPFILE`. Encode `entry` as `fuse_entry_out`, followed
/// immediately by `open` as `fuse_open_out`. Use the negotiated protocol version for both parts.
pub struct CreateResult {
    pub entry: EntryResult,
    pub open: OpenResult,
}

/// The corresponding C structure is `fuse_bmap_out`.
/// ```c
/// struct fuse_bmap_out {
///     uint64_t block;
/// };
/// ```
/// `block` is the mapped device block number in units of the requested block size.
pub struct BmapResult {
    pub block: u64,
}

/// The reply payload of `FUSE_IOCTL` starts with `fuse_ioctl_out`.
/// ```c
/// struct fuse_ioctl_out {
///     int32_t result;
///     uint32_t flags, in_iovs, out_iovs;
/// };
/// ```
pub enum IoctlResult {
    /// Return the ioctl result and up to `out_size` bytes of output. Encode zero flags and counts,
    /// then append `data` after the header.
    Complete { result: i32, data: Vec<u8> },
    /// Ask the kernel to retry with the specified caller buffers. Set `FUSE_IOCTL_RETRY`, derive
    /// the counts from the vectors, and append the input descriptors followed by the output
    /// descriptors. The request must have `FUSE_IOCTL_UNRESTRICTED`, and the two lists together may
    /// contain up to `FUSE_IOCTL_MAX_IOV` entries.
    Retry {
        input_iovecs: Vec<IoctlIovec>,
        output_iovecs: Vec<IoctlIovec>,
    },
}

/// The corresponding C structure is `fuse_ioctl_iovec`.
/// ```c
/// struct fuse_ioctl_iovec {
///     uint64_t base, len;
/// };
/// ```
/// `base` is an address in the calling process, and `len` is the buffer's length in bytes. This
/// fixed-width layout applies from FUSE 7.16 onward.
pub struct IoctlIovec {
    pub base: u64,
    pub len: u64,
}

/// The corresponding C structure is `fuse_poll_out`.
/// ```c
/// struct fuse_poll_out {
///     uint32_t revents, padding;
/// };
/// ```
/// Encode `revents` as the bitmask returned by [`PollEvents::bits`] and set `padding` to zero.
pub struct PollResult {
    pub revents: PollEvents,
}

bitflags::bitflags! {
    /// Linux `POLL*` event bits that can be returned by `FUSE_POLL`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PollEvents: u32 {
        const POLLIN = libc::POLLIN as u32;
        const POLLPRI = libc::POLLPRI as u32;
        const POLLOUT = libc::POLLOUT as u32;
        const POLLERR = libc::POLLERR as u32;
        const POLLHUP = libc::POLLHUP as u32;
        const POLLNVAL = libc::POLLNVAL as u32;
        const POLLRDNORM = libc::POLLRDNORM as u32;
        const POLLRDBAND = libc::POLLRDBAND as u32;
        const POLLWRNORM = libc::POLLWRNORM as u32;
        const POLLWRBAND = libc::POLLWRBAND as u32;
        // Linux defines POLLMSG in the UAPI with a different bit on SPARC.
        const POLLMSG = if cfg!(any(target_arch = "sparc", target_arch = "sparc64")) {
            0x0200
        } else {
            0x0400
        };
        const POLLRDHUP = libc::POLLRDHUP as u32;
    }
}

/// `FUSE_READDIRPLUS` returns a list of `fuse_direntplus` records.
/// Each record has a directory entry and optional inode attributes.
///
/// The reply payload must fit within the `size` in `fuse_read_in`.
pub struct ReaddirPlusResult {
    pub entries: Vec<DirEntryPlus>,
}

/// The corresponding C structure is `fuse_direntplus`.
/// ```c
/// struct fuse_direntplus {
///     struct fuse_entry_out entry_out;
///     struct fuse_dirent dirent;
/// };
/// ```
/// Write `fuse_entry_out` first.
///
/// - For `Some(entry)`, write the entry with a nonzero `nodeid`.
/// - For `None`, fill `fuse_entry_out` with zeroes. The kernel skips caching the inode attributes.
///
/// Then write `dirent` as `fuse_dirent`.
/// Pad the whole record with zeroes to a multiple of 8 bytes.
pub struct DirEntryPlus {
    pub entry: Option<EntryResult>,
    pub dirent: DirEntry,
}

/// The corresponding C structure is `fuse_lseek_out`.
/// ```c
/// struct fuse_lseek_out {
///     uint64_t offset;
/// };
/// ```
/// `offset` is the resulting file position in bytes from the start of the file.
pub struct LseekResult {
    pub offset: u64,
}

/// The corresponding C structure is `fuse_statx_out`.
/// ```c
/// struct fuse_statx_out {
///     uint64_t attr_valid;
///     uint32_t attr_valid_nsec, flags;
///     uint64_t spare[2];
///     struct fuse_statx stat;
/// };
/// ```
/// `attr_valid` and `attr_valid_nsec` are the seconds and nanoseconds of the attribute cache
/// lifetime. Encode `spare` as zero.
pub struct StatxResult {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub stat: Statx,
}

/// The corresponding C structure is `fuse_statx`.
/// ```c
/// struct fuse_statx {
///     uint32_t mask, blksize;
///     uint64_t attributes;
///     uint32_t nlink, uid, gid;
///     uint16_t mode, __spare0[1];
///     uint64_t ino, size, blocks, attributes_mask;
///     struct fuse_sx_time atime, btime, ctime, mtime;
///     uint32_t rdev_major, rdev_minor, dev_major, dev_minor;
///     uint64_t __spare2[14];
/// };
/// ```
/// `mask` indicates which fields are valid. `attributes_mask` indicates which bits in `attributes`
/// are supported. Encode `__spare0` and `__spare2` as zero.
pub struct Statx {
    pub mask: u32,
    pub blksize: u32,
    pub attributes: u64,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u16,
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub attributes_mask: u64,
    pub atime: SxTime,
    pub btime: SxTime,
    pub ctime: SxTime,
    pub mtime: SxTime,
    pub rdev_major: u32,
    pub rdev_minor: u32,
    pub dev_major: u32,
    pub dev_minor: u32,
}

/// The corresponding C structure is `fuse_sx_time`.
/// ```c
/// struct fuse_sx_time {
///     int64_t tv_sec;
///     uint32_t tv_nsec;
///     int32_t __reserved;
/// };
/// ```
/// `tv_sec` is the signed number of seconds from the Unix epoch, and `tv_nsec` is the nanosecond
/// offset within that second. Encode `__reserved` as zero.
pub struct SxTime {
    pub tv_sec: i64,
    pub tv_nsec: u32,
}

/// The corresponding C structure is `fuse_copy_file_range_out`.
/// ```c
/// struct fuse_copy_file_range_out {
///     uint64_t bytes_copied;
/// };
/// ```
/// Used by `FUSE_COPY_FILE_RANGE_64`. The original `FUSE_COPY_FILE_RANGE` uses [`WriteResult`].
pub struct CopyFileRangeResult {
    pub bytes_copied: u64,
}
