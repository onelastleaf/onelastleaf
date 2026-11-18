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
    unused4: u32,
    uid: u32,
    gid: u32,
    unused5: u32,
}

// `FUSE_READLINK` haven't request payload

/// `name` + \0 + `target` + \0
/// When negotiated, security-context and supplementary-group extensions follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymlinkPayload<'a> {
    name: &'a [u8],
    taregt: &'a [u8],
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
    umask: u32,
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
    umask: u32,
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
    newdir: u64,
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
    oldnodeid: u64,
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
    lock_owner: u64,
    flags: u32,
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
    size: u32,
    write_flags: u32,
    lock_owner: u64,
    flags: u32,
    data: &'a [u8],
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
    flags: u32,
    release_flags: u32,
    lock_owner: u64,
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
    newdir: u64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Flags {
    AsyncRead,
    PosixLocks,
    FileOps,
    AtomicOTrunc,
    ExportSupport,
    BigWrites,
    DontMask,
    SpliceWrite,
    SpliceMove,
    SpliceRead,
    FlockLocks,
    HasIoctlDir,
    AutoInvalData,
    DoReaddirplus,
    ReaddirplusAuto,
    AsyncDio,
    WritebackCache,
    NoOpenSupport,
    ParallelDirops,
    HandleKillpriv,
    PosixAcl,
    AbortError,
    MaxPages,
    CacheSymlinks,
    NoOpendirSupport,
    ExplicitInvalData,
    MapAlignment,
    Submounts,
    HandleKillprivV2,
    SetxattrExt,
    InitExt,
    InitReserved,
    SecurityCtx,
    HasInodeDax,
    CreateSuppGroup,
    HasExpireOnly,
    DirectIoAllowMmap,
    Passthrough,
    NoExportSupport,
    HasResend,
    AllowIdmap,
    OverIoUring,
    RequestTimeout,
    HasIoUringBufpool,
}
