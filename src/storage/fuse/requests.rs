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
    Create(CreatePayload),
    Interrupt(InterruptPayload),
    Bmap(BmapPayload),
    Destroy,
    Ioctl(IoctlPayload),
    Poll(PollPayload),
    NotifyReply(NotifyRetrievePayload),
    BatchForget(BatchForgetPayload),
    Fallocate(FallocatePayload),
    ReaddirPlus(ReadPayload),
    Rename2(Rename2Payload),
    Lseek(LseekPayload),
    CopyFileRange(CopyFileRangePayload),
    Setupmapping(SetupmappingPayload),
    Removemapping(RemovemappingPayload),
    Syncfs,
    Tmpfile(CreatePayload),
    Statx(StatxPayload),
    CopyFileRange64(CopyFileRangePayload),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitPayload {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: Flags,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForgetPayload {
    nlookup: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetattrPayload {
    getattr_flags: u32,
    fh: u64,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymlinkPayload<'a> {
    name: &'a [u8],
    taregt: &'a [u8],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MknodPayload<'a> {
    mode: u32,
    rdev: u32,
    umask: u32,
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MkdirPayload<'a> {
    mode: u32,
    umask: u32,
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnlinkPayload<'a> {
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RmdirPayload<'a> {
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenamePayload<'a> {
    newdir: u64,
    old_name: &'a [u8],
    new_name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkPayload<'a> {
    oldnodeid: u64,
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenPayload {
    flags: u32,
    open_flags: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPayload {
    fh: u64,
    offset: u64,
    size: u32,
    read_flags: u32,
    lock_owner: u64,
    flags: u32,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleasePayload {
    fh: u64,
    flags: u32,
    release_flags: u32,
    lock_owner: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsyncPayload {
    fh: u64,
    fsync_flags: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetxattrPayload<'a> {
    size: u32,
    flags: u32,
    setxattr_flags: u32,
    name: &'a [u8],
    value: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetxattrPayload<'a> {
    size: u32,
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovexattrPayload<'a> {
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushPayload {
    fh: u64,
    unused: u32,
    lock_owner: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockPayload {
    fh: u64,
    owner: u64,
    lk: FileLock,
    lk_flags: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileLock {
    start: u64,
    end: u64,
    lock_type: u32,
    pid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessPayload {
    mask: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatePayload<'a> {
    flags: u32,
    mode: u32,
    umask: u32,
    open_flags: u32,
    name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterruptPayload {
    unique: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BmapPayload {
    block: u64,
    blocksize: u32,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PollPayload {
    fh: u64,
    kh: u64,
    flags: u32,
    events: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotifyReplyPayload<'a> {
    offset: u64,
    data: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchForgetPayload {
    entries: Vec<ForgetOne>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForgetOne {
    nodeid: u64,
    nlookup: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FallocatePayload {
    fh: u64,
    offset: u64,
    length: u64,
    mode: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rename2Payload<'a> {
    newdir: u64,
    flags: u32,
    old_name: &'a [u8],
    new_name: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LseekPayload {
    fh: u64,
    offset: u64,
    whence: u32,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetupmappingPayload {
    fh: u64,
    foffset: u64,
    len: u64,
    flags: u64,
    moffset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovemappingPayload {
    entries: Vec<RemovemappingOne>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovemappingOne {
    moffset: u64,
    len: u64,
}

// pub struct SyncfsPayload {
//     padding: u64,
// }

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
