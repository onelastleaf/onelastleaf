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
    pub max_name_length: u32,
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

pub struct ReaddirResult {
    pub data: Vec<DirEntry>,
}
pub struct DirEntry;
pub struct LockResult;
pub struct CreateResult;
pub struct BmapResult;
pub struct IoctlResult;
pub struct PollResult;
pub struct ReaddirPlusResult {
    pub data: Vec<DirEntryPlus>,
}
pub struct DirEntryPlus;
pub struct LseekResult;
pub struct StatxResult;
pub struct Statx;
pub struct SxTime;
pub struct CopyFileRangeResult;
