#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Init(InitRequest),
    Lookup,
    Forget,
    Getattr,
    Setattr,
}

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
pub struct InitRequest {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: Flags,
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
