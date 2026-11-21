pub struct EntryResult {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

pub struct AttrResult {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

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
/// `revents` is the mask of reported events, such as `POLLIN`. Encode `padding` as zero.
pub struct PollResult {
    pub revents: u32,
}

pub struct ReaddirPlusResult {
    pub entries: Vec<DirEntryPlus>,
}

pub struct DirEntryPlus;
pub struct LseekResult;
pub struct StatxResult;
pub struct Statx;
pub struct SxTime;
pub struct CopyFileRangeResult;
