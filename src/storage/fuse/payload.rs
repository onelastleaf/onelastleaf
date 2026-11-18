use super::Flags;
use super::session::ParseError;

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_in_header {
///     uint32_t len;
///     uint32_t opcode;
///     uint64_t unique;
///     uint64_t nodeid;
///     uint32_t uid;
///     uint32_t gid;
///     uint32_t pid;
///     uint16_t total_extlen;
///     uint16_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub(super) len: u32,
    pub(super) opcode: u32,
    pub(super) unique: u64,
    pub(super) nodeid: u64,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) total_extlen: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload<'a> {
    Init(InitPayload),
    Lookup(LookupPayload<'a>),
    Forget(ForgetPayload),
    Getattr(GetattrPayload),
    Setattr(SetattrPayload),
    Readlink,
    Symlink(SymlinkPayload<'a>),
    Mknod(MknodPayload<'a>),
    Mkdir(MkdirPayload<'a>),
    Unlink(UnlinkPayload<'a>),
    Rmdir(RmdirPayload<'a>),
    Rename(RenamePayload<'a>),
    Link(LinkPayload<'a>),
    Open(OpenPayload),
    Read(ReadPayload),
    Write(WritePayload<'a>),
    Statfs,
    Release(ReleasePayload),
    Fsync(FsyncPayload),
    Setxattr(SetxattrPayload<'a>),
    Getxattr(GetxattrPayload<'a>),
    Listxattr(GetxattrPayload<'a>),
    Removexattr(RemovexattrPayload<'a>),
    Flush(FlushPayload),
    Opendir(OpenPayload),
    Readdir(ReadPayload),
    Releasedir(ReleasePayload),
    Fsyncdir(FsyncPayload),
    Getlk(LockPayload),
    Setlk(LockPayload),
    Setlkw(LockPayload),
    Access(AccessPayload),
    Create(CreatePayload<'a>),
    Interrupt(InterruptPayload),
    Bmap(BmapPayload),
    Destroy,
    Ioctl(IoctlPayload<'a>),
    Poll(PollPayload),
    NotifyReply(NotifyReplyPayload<'a>),
    BatchForget(BatchForgetPayload),
    Fallocate(FallocatePayload),
    ReaddirPlus(ReadPayload),
    Rename2(Rename2Payload<'a>),
    Lseek(LseekPayload),
    CopyFileRange(CopyFileRangePayload),
    Setupmapping(SetupmappingPayload),
    Removemapping(RemovemappingPayload),
    Syncfs,
    Tmpfile(CreatePayload<'a>),
    Statx(StatxPayload),
    CopyFileRange64(CopyFileRangePayload),
}

/// The definition of the corresponding C struct in C.
/// ```c
/// struct fuse_init_in {
///     uint32_t major;
///     uint32_t minor;
///     uint32_t max_readahead;
///     uint32_t flags;
///     uint32_t flags2;
///     uint32_t unused[11];
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitPayload {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: Flags,
}
/// The definition of the corresponding underlying C structure:
/// `name` + "\0"
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupPayload<'a> {
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_forget_in {
///     uint64_t nlookup;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForgetPayload {
    nlookup: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_getattr_in {
///     uint32_t getattr_flags;
///     uint32_t dummy;
///     uint64_t fh;
/// };
/// ````
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetattrPayload {
    getattr_flags: u32,
    fh: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_setattr_in {
///     uint32_t valid;
///     uint32_t padding;
///     uint64_t fh;
///     uint64_t size;
///     uint64_t lock_owner;
///     uint64_t atime;
///     uint64_t mtime;
///     uint64_t ctime;
///     uint32_t atimensec;
///     uint32_t mtimensec;
///     uint32_t ctimensec;
///     uint32_t mode;
///     uint32_t unused4;
///     uint32_t uid;
///     uint32_t gid;
///     uint32_t unused5;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetattrPayload {
    valid: u32,
    fh: u64,
    size: u64,
    lock_owner: u64,
    atime: u64,
    mtime: u64,
    ctime: u64,
    atimensec: u32,
    mtimensec: u32,
    ctimensec: u32,
    mode: u32,
    uid: u32,
    gid: u32,
}

// `FUSE_READLINK` haven't request payload

/// `name` + \0 + `target` + \0
/// When negotiated, security-context and supplementary-group extensions follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymlinkPayload<'a> {
    name: &'a [u8],
    target: &'a [u8],
}

/// ```c
/// struct fuse_mknod_in {
///     uint32_t mode;
///     uint32_t rdev;
///     uint32_t umask;
///     uint32_t padding;
/// };
/// ```
/// then, add `name` + \0
/// When negotiated, security-context and supplementary-group extensions follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MknodPayload<'a> {
    mode: u32,
    rdev: u32,
    umask: Option<u32>,
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_mkdir_in {
///     uint32_t mode;
///     uint32_t umask;
/// };
/// ```
/// then, add `name` + \0
/// When negotiated, security-context and supplementary-group extensions follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MkdirPayload<'a> {
    mode: u32,
    umask: Option<u32>,
    name: &'a [u8],
}

/// The definition of the corresponding underlying C structure:
/// `name` + "\0"
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnlinkPayload<'a> {
    name: &'a [u8],
}

/// The definition of the corresponding underlying C structure:
/// `name` + "\0"
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RmdirPayload<'a> {
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_rename_in {
///     uint64_t newdir;
/// };
/// ```
/// then, add `old_name` + \0 + `new_name` + \0
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenamePayload<'a> {
    new_dir: u64,
    old_name: &'a [u8],
    new_name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_link_in {
///     uint64_t oldnodeid;
/// };
/// ```
/// then, add `name` + \0
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkPayload<'a> {
    old_nodeid: u64,
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_open_in {
///     uint32_t flags;
///     uint32_t open_flags;
/// };
/// ```
/// Used by `FUSE_OPEN` and `FUSE_OPENDIR`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenPayload {
    flags: u32,
    open_flags: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_read_in {
///     uint64_t fh;
///     uint64_t offset;
///     uint32_t size;
///     uint32_t read_flags;
///     uint64_t lock_owner;
///     uint32_t flags;
///     uint32_t padding;
/// };
/// ```
/// Used by `FUSE_READ`, `FUSE_READDIR`, and `FUSE_READDIRPLUS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPayload {
    fh: u64,
    offset: u64,
    size: u32,
    read_flags: u32,
    lock_owner: Option<u64>,
    flags: Option<u32>,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_write_in {
///     uint64_t fh;
///     uint64_t offset;
///     uint32_t size;
///     uint32_t write_flags;
///     uint64_t lock_owner;
///     uint32_t flags;
///     uint32_t padding;
/// };
/// ```
/// then, add `data` (`size` bytes, without an added trailing \0)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritePayload<'a> {
    fh: u64,
    offset: u64,
    write_flags: u32,
    lock_owner: Option<u64>,
    flags: Option<u32>,
    data: &'a [u8], // use `data.len()` instead of `size`
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_release_in {
///     uint64_t fh;
///     uint32_t flags;
///     uint32_t release_flags;
///     uint64_t lock_owner;
/// };
/// ```
/// Used by `FUSE_RELEASE` and `FUSE_RELEASEDIR`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleasePayload {
    fh: u64,
    flags: Option<u32>,
    release_flags: Option<u32>,
    lock_owner: Option<u64>,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_fsync_in {
///     uint64_t fh;
///     uint32_t fsync_flags;
///     uint32_t padding;
/// };
/// ```
/// Used by `FUSE_FSYNC` and `FUSE_FSYNCDIR`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsyncPayload {
    fh: u64,
    fsync_flags: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_setxattr_in {
///     uint32_t size;
///     uint32_t flags;
///     uint32_t setxattr_flags;
///     uint32_t padding;
/// };
/// ```
/// then, add `name` + \0 + `value` (`size` bytes, without an added trailing \0)
/// Without `FUSE_SETXATTR_EXT`, only the first two fields of the C struct are sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetxattrPayload<'a> {
    size: u32,
    flags: u32,
    setxattr_flags: u32,
    name: &'a [u8],
    value: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_getxattr_in {
///     uint32_t size;
///     uint32_t padding;
/// };
/// ```
/// For `FUSE_GETXATTR`, then add `name` + \0.
/// For `FUSE_LISTXATTR`, only the C struct is sent, without a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetxattrPayload<'a> {
    size: u32,
    name: &'a [u8],
}

/// The definition of the corresponding underlying C structure:
/// `name` + "\0"
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovexattrPayload<'a> {
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_flush_in {
///     uint64_t fh;
///     uint32_t unused;
///     uint32_t padding;
///     uint64_t lock_owner;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushPayload {
    fh: u64,
    unused: u32,
    lock_owner: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_lk_in {
///     uint64_t fh;
///     uint64_t owner;
///     struct fuse_file_lock lk;
///     uint32_t lk_flags;
///     uint32_t padding;
/// };
/// ```
/// Used by `FUSE_GETLK`, `FUSE_SETLK`, and `FUSE_SETLKW`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockPayload {
    fh: u64,
    owner: u64,
    lk: FileLock,
    lk_flags: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_file_lock {
///     uint64_t start;
///     uint64_t end;
///     uint32_t type;
///     uint32_t pid;
/// };
/// ```
/// The C field `type` is represented by `lock_type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileLock {
    start: u64,
    end: u64,
    lock_type: u32,
    pid: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_access_in {
///     uint32_t mask;
///     uint32_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessPayload {
    mask: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_create_in {
///     uint32_t flags;
///     uint32_t mode;
///     uint32_t umask;
///     uint32_t open_flags;
/// };
/// ```
/// then, add `name` + \0
/// Used by `FUSE_CREATE` and `FUSE_TMPFILE`.
/// When negotiated, security-context and supplementary-group extensions follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatePayload<'a> {
    flags: u32,
    mode: u32,
    umask: u32,
    open_flags: u32,
    name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_interrupt_in {
///     uint64_t unique;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterruptPayload {
    unique: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_bmap_in {
///     uint64_t block;
///     uint32_t blocksize;
///     uint32_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BmapPayload {
    block: u64,
    blocksize: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_ioctl_in {
///     uint64_t fh;
///     uint32_t flags;
///     uint32_t cmd;
///     uint64_t arg;
///     uint32_t in_size;
///     uint32_t out_size;
/// };
/// ```
/// then, add `input` (`in_size` bytes, without an added trailing \0)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoctlPayload<'a> {
    fh: u64,
    flags: u32,
    cmd: u32,
    arg: u64,
    in_size: u32,
    out_size: u32,
    input: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_poll_in {
///     uint64_t fh;
///     uint64_t kh;
///     uint32_t flags;
///     uint32_t events;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollPayload {
    fh: u64,
    kh: u64,
    flags: u32,
    events: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_notify_retrieve_in {
///     uint64_t dummy1;
///     uint64_t offset;
///     uint32_t size;
///     uint32_t dummy2;
///     uint64_t dummy3;
///     uint64_t dummy4;
/// };
/// ```
/// then, add `data` (`size` bytes, without an added trailing \0)
/// Used by `FUSE_NOTIFY_REPLY` for a `FUSE_NOTIFY_RETRIEVE` notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotifyReplyPayload<'a> {
    offset: u64,
    data: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_batch_forget_in {
///     uint32_t count;
///     uint32_t dummy;
/// };
/// ```
/// then, add `count` consecutive `struct fuse_forget_one` entries
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchForgetPayload {
    entries: Vec<ForgetOne>,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_forget_one {
///     uint64_t nodeid;
///     uint64_t nlookup;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForgetOne {
    nodeid: u64,
    nlookup: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_fallocate_in {
///     uint64_t fh;
///     uint64_t offset;
///     uint64_t length;
///     uint32_t mode;
///     uint32_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FallocatePayload {
    fh: u64,
    offset: u64,
    length: u64,
    mode: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_rename2_in {
///     uint64_t newdir;
///     uint32_t flags;
///     uint32_t padding;
/// };
/// ```
/// then, add `old_name` + \0 + `new_name` + \0
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rename2Payload<'a> {
    new_dir: u64,
    flags: u32,
    old_name: &'a [u8],
    new_name: &'a [u8],
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_lseek_in {
///     uint64_t fh;
///     uint64_t offset;
///     uint32_t whence;
///     uint32_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LseekPayload {
    fh: u64,
    offset: u64,
    whence: u32,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_copy_file_range_in {
///     uint64_t fh_in;
///     uint64_t off_in;
///     uint64_t nodeid_out;
///     uint64_t fh_out;
///     uint64_t off_out;
///     uint64_t len;
///     uint64_t flags;
/// };
/// ```
/// Used by `FUSE_COPY_FILE_RANGE` and `FUSE_COPY_FILE_RANGE_64`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyFileRangePayload {
    fh_in: u64,
    off_in: u64,
    nodeid_out: u64,
    fh_out: u64,
    off_out: u64,
    len: u64,
    flags: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_setupmapping_in {
///     uint64_t fh;
///     uint64_t foffset;
///     uint64_t len;
///     uint64_t flags;
///     uint64_t moffset;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetupmappingPayload {
    fh: u64,
    foffset: u64,
    len: u64,
    flags: u64,
    moffset: u64,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_removemapping_in {
///     uint32_t count;
/// };
/// ```
/// then, add `count` consecutive `struct fuse_removemapping_one` entries
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovemappingPayload {
    entries: Vec<RemovemappingOne>,
}

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_removemapping_one {
///     uint64_t moffset;
///     uint64_t len;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovemappingOne {
    moffset: u64,
    len: u64,
}

// pub struct SyncfsPayload {
//     padding: u64,
// }

/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_statx_in {
///     uint32_t getattr_flags;
///     uint32_t reserved;
///     uint64_t fh;
///     uint32_t sx_flags;
///     uint32_t sx_mask;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatxPayload {
    getattr_flags: u32,
    fh: u64,
    sx_flags: u32,
    sx_mask: u32,
}

// Now I will implementing some functions, for payload parsing

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, number: usize) -> Result<&'a [u8], ParseError> {
        let (taken, remaining) = self
            .bytes
            .split_at_checked(number)
            .ok_or(ParseError::InsufficientLength)?;

        self.bytes = remaining;
        Ok(taken)
    }

    fn skip(&mut self, number: usize) -> Result<(), ParseError> {
        self.take(number)?;
        Ok(())
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_ne_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ParseError> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_ne_bytes(bytes))
    }

    fn read_until_nul(&mut self) -> Result<&'a [u8], ParseError> {
        let end = self
            .bytes
            .iter()
            .position(|&byte| byte == 0)
            .ok_or(ParseError::InsufficientLength)?;

        let field = &self.bytes[..end];
        self.bytes = &self.bytes[end + 1..];
        Ok(field)
    }

    fn finish(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Generates a parser that reads one NUL-terminated byte slice per field,
/// in the order listed, and rejects trailing bytes.
///
/// For example, the `parse_lookup` invocation expands to this method inside
/// `impl<'a> Payload<'a>`.
///
/// ```rust,ignore
/// pub(super) fn parse_lookup(bytes: &'a [u8]) -> Result<Self, ParseError> {
///     let mut cursor = Cursor::new(bytes);
///     let name = cursor.read_until_nul()?;
///
///     if !cursor.finish() {
///         return Err(ParseError::ExcessiveLength);
///     }
///
///     Ok(Self::Lookup(LookupPayload { name }))
/// }
/// ```
macro_rules! parse_string_only_payload {
    ($function:ident => $variant:ident($payload:ident) {
        $($field:ident),+ $(,)?
    }) => {
        pub(super) fn $function(bytes: &'a [u8]) -> Result<Self, ParseError> {
            let mut cursor = Cursor::new(bytes);

            $(
                let $field = cursor.read_until_nul()?;
            )+

            if !cursor.finish() {
                return Err(ParseError::ExcessiveLength);
            }

            Ok(Self::$variant($payload { $($field),+ }))
        }
    };
}

/// Generates a parser that reads fields in the order listed and rejects
/// trailing bytes. Each `field: reader` entry calls `cursor.reader()`.
///
/// For example, the `parse_forget` invocation expands to this method inside
/// `impl<'a> Payload<'a>`.
///
/// ```rust,ignore
/// pub(super) fn parse_forget(bytes: &'a [u8]) -> Result<Self, ParseError> {
///     let mut cursor = Cursor::new(bytes);
///     let nlookup = cursor.u64()?;
///
///     if !cursor.finish() {
///         return Err(ParseError::ExcessiveLength);
///     }
///
///     Ok(Self::Forget(ForgetPayload { nlookup }))
/// }
/// ```
macro_rules! parse_struct_only_without_padding_payload  {
    ($function:ident => $variant:ident($payload:ident) {
        $($field:ident: $type:ident),+ $(,)?
    }) => {
        pub(super) fn $function(bytes: &'a [u8]) -> Result<Self, ParseError> {
            let mut cursor = Cursor::new(bytes);

            $(
                let $field = cursor.$type()?;
            )+

            if !cursor.finish() {
                return Err(ParseError::ExcessiveLength);
            }


            Ok(Self::$variant($payload { $($field),+ }))
        }
    };
}

macro_rules! parse_payload_with_trailing_names {
    (
        $function:ident => $variant:ident($payload:ident) {
            fixed {
                $($field:ident: $type:ident $(=> skip($padding:expr))?),+ $(,)?
            }
            names {
                $($name:ident),+ $(,)?
            }
        }
    ) => {
        pub(super) fn $function(bytes: &'a [u8]) -> Result<Self, ParseError> {
            let mut cursor = Cursor::new(bytes);

            $(
                let $field = cursor.$type()?;
                $(cursor.skip($padding)?;)?
            )+

            $(
                let $name = cursor.read_until_nul()?;
            )+

            if !cursor.finish() {
                return Err(ParseError::ExcessiveLength);
            }

            Ok(Self::$variant($payload {
                $($field,)+
                $($name,)+
            }))
        }
    };
}

impl<'a> Payload<'a> {
    pub(super) fn parse_init(bytes: &'a [u8]) -> Result<Self, ParseError> {
        let mut cursor = Cursor::new(bytes);

        let major = cursor.u32()?;
        let minor = cursor.u32()?;
        let max_readahead = cursor.u32()?;

        // what about `flags`?
        let mut flags = Flags::from_bits_retain(u64::from(cursor.u32()?));
        if flags.contains(Flags::INIT_EXT) {
            let high = Flags::from_bits_retain(u64::from(cursor.u32()?) << 32);
            cursor.skip(11 * 4)?; // unused[11]
            flags.insert(high);
        }

        Ok(Self::Init(InitPayload {
            major,
            minor,
            max_readahead,
            flags,
        }))
    }

    parse_string_only_payload!(parse_lookup => Lookup(LookupPayload) { name });

    parse_struct_only_without_padding_payload!(parse_forget => Forget(ForgetPayload) {
        nlookup: u64,
    });

    pub(super) fn parse_getattr(
        bytes: &'a [u8],
        protocol_version: u32,
    ) -> Result<Self, ParseError> {
        let (getattr_flags, fh) = match bytes.len() {
            0 | 16 if protocol_version < 9 => (0, 0),
            16 => {
                let mut cursor = Cursor::new(bytes);
                let getattr_flags = cursor.u32()?;
                cursor.skip(4)?; // dummy
                let fh = cursor.u64()?;
                (getattr_flags, fh)
            }
            0..=15 => return Err(ParseError::InsufficientLength),
            _ => return Err(ParseError::ExcessiveLength),
        };

        Ok(Self::Getattr(GetattrPayload { getattr_flags, fh }))
    }

    pub(super) fn parse_setattr(
        bytes: &'a [u8],
        procotol_version: u32,
    ) -> Result<Self, ParseError> {
        const FATTR_FH: u32 = 1 << 6;

        let mut cursor = Cursor::new(bytes);

        let mut valid = cursor.u32()?;
        cursor.skip(4)?; // padding

        let fh = if procotol_version == 2 {
            valid &= !FATTR_FH;
            cursor.skip(8)?; // legacy fuse_attr.ino
            0
        } else {
            cursor.u64()?
        };

        let size = cursor.u64()?;
        let lock_owner = cursor.u64()?;
        let atime = cursor.u64()?;
        let mtime = cursor.u64()?;
        let ctime = cursor.u64()?;
        let atimensec = cursor.u32()?;
        let mtimensec = cursor.u32()?;
        let ctimensec = cursor.u32()?;
        let mode = cursor.u32()?;
        cursor.skip(4)?; // unused4
        let uid = cursor.u32()?;
        let gid = cursor.u32()?;
        cursor.skip(4)?; // unused5

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Setattr(SetattrPayload {
            valid,
            fh,
            size,
            lock_owner,
            atime,
            mtime,
            ctime,
            atimensec,
            mtimensec,
            ctimensec,
            mode,
            uid,
            gid,
        }))
    }

    parse_string_only_payload!(parse_symlink => Symlink(SymlinkPayload) { name, target });

    pub(super) fn parse_mknod(bytes: &'a [u8], protocol_version: u32) -> Result<Self, ParseError> {
        let mut cursor = Cursor::new(bytes);

        let mode = cursor.u32()?;
        let rdev = cursor.u32()?;

        let umask = if protocol_version >= 12 {
            let umask = cursor.u32()?;
            cursor.skip(4)?; // padding
            Some(umask)
        } else {
            None
        };

        let name = cursor.read_until_nul()?;

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Mknod(MknodPayload {
            mode,
            rdev,
            umask,
            name,
        }))
    }

    pub(super) fn parse_mkdir(bytes: &'a [u8], protocol_version: u32) -> Result<Self, ParseError> {
        let mut cursor = Cursor::new(bytes);

        let mode = cursor.u32()?;

        let umask = if protocol_version >= 12 {
            let umask = cursor.u32()?;
            Some(umask)
        } else {
            None
        };

        let name = cursor.read_until_nul()?;

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Mkdir(MkdirPayload { mode, umask, name }))
    }

    parse_string_only_payload!(parse_unlink => Unlink(UnlinkPayload) { name });

    parse_string_only_payload!(parse_rmdir => Rmdir(RmdirPayload) { name });

    parse_payload_with_trailing_names!(parse_rename => Rename(RenamePayload) {
        fixed { new_dir: u64 }
        names { old_name, new_name }
    });

    parse_payload_with_trailing_names!(parse_link => Link(LinkPayload) {
        fixed { old_nodeid: u64 }
        names { name }
    });

    parse_struct_only_without_padding_payload!(parse_open => Open(OpenPayload) {
        flags: u32,
        open_flags: u32,
    });

    pub(super) fn parse_read(bytes: &'a [u8], protocol_version: u32) -> Result<Self, ParseError> {
        const FUSE_READ_LOCKOWNER: u32 = 1 << 1;

        match bytes.len() {
            24 if protocol_version < 9 => {}
            40 => {}
            0..=39 => return Err(ParseError::InsufficientLength),
            _ => return Err(ParseError::ExcessiveLength),
        }

        let mut cursor = Cursor::new(bytes);

        let fh = cursor.u64()?;
        let offset = cursor.u64()?;
        let size = cursor.u32()?;

        let (read_flags, lock_owner, flags) = if protocol_version < 9 {
            cursor.skip(bytes.len() - 20)?;
            (0, None, None)
        } else {
            let read_flags = cursor.u32()?;
            let raw_lock_owner = cursor.u64()?;
            let lock_owner = (read_flags & FUSE_READ_LOCKOWNER != 0).then_some(raw_lock_owner);
            let flags = Some(cursor.u32()?);
            cursor.skip(4)?; // padding
            (read_flags, lock_owner, flags)
        };

        Ok(Self::Read(ReadPayload {
            fh,
            offset,
            size,
            read_flags,
            lock_owner,
            flags,
        }))
    }

    pub(super) fn parse_write(bytes: &'a [u8], protocol_version: u32) -> Result<Self, ParseError> {
        const FUSE_WRITE_LOCKOWNER: u32 = 1 << 1;

        let mut cursor = Cursor::new(bytes);

        let fh = cursor.u64()?;
        let offset = cursor.u64()?;
        let size = cursor.u32()?;
        let write_flags = cursor.u32()?;

        let (lock_owner, flags) = if protocol_version < 9 {
            (None, None)
        } else {
            let raw_lock_owner = cursor.u64()?;
            let lock_owner = (write_flags & FUSE_WRITE_LOCKOWNER != 0).then_some(raw_lock_owner);
            let flags = Some(cursor.u32()?);
            cursor.skip(4)?; // padding
            (lock_owner, flags)
        };

        let data = cursor.take(size as usize)?;

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Write(WritePayload {
            fh,
            offset,
            write_flags,
            lock_owner,
            flags,
            data,
        }))
    }

    pub(super) fn parse_release(
        bytes: &'a [u8],
        protocol_version: u32,
    ) -> Result<Self, ParseError> {
        const FUSE_RELEASE_FLOCK_UNLOCK: u32 = 1 << 1;

        let mut cursor = Cursor::new(bytes);
        let fh = cursor.u64()?;
        let flags = Some(cursor.u32()?);

        let (release_flags, lock_owner) = if protocol_version < 8 {
            cursor.skip(4)?; // padding in the legacy layout

            if bytes.len() == 24 {
                cursor.skip(8)?; // modern kernel using an older negotiated version
            }

            (None, None)
        } else {
            let release_flags = cursor.u32()?;
            let lock_owner = cursor.u64()?;

            (
                Some(release_flags),
                (release_flags & FUSE_RELEASE_FLOCK_UNLOCK != 0).then_some(lock_owner),
            )
        };

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Release(ReleasePayload {
            fh,
            flags,
            release_flags,
            lock_owner,
        }))
    }

    pub(super) fn parse_fsync(bytes: &'a [u8]) -> Result<Self, ParseError> {
        let mut cursor = Cursor::new(bytes);

        let fh = cursor.u64()?;
        let fsync_flags = cursor.u32()?;

        if !cursor.finish() {
            return Err(ParseError::ExcessiveLength);
        }

        Ok(Self::Fsync(FsyncPayload { fh, fsync_flags }))
    }

    pub(super) fn parse_setxattr(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_getxattr(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_listxattr(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    parse_string_only_payload!(parse_removexattr => Removexattr(RemovexattrPayload) { name });

    pub(super) fn parse_flush(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_opendir(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_readdir(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_releasedir(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_fsyncdir(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_getlk(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_setlk(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_setlkw(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_access(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_create(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_interrupt(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_bmap(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_destroy(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_ioctl(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_poll(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_notify_reply(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_batch_forget(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_fallocate(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_readdir_plus(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    parse_payload_with_trailing_names!(parse_rename2 => Rename2(Rename2Payload) {
        fixed { new_dir: u64, flags: u32 => skip(4) }
        names { old_name, new_name }
    });

    pub(super) fn parse_lseek(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_copy_file_range(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_setupmapping(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_removemapping(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_syncfs(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_tmpfile(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_statx(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }

    pub(super) fn parse_copy_file_range64(bytes: &'a [u8]) -> Result<Self, ParseError> {
        todo!()
    }
}
