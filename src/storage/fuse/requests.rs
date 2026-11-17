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
    len: u32,
    opcode: u32,
    unique: u64,
    nodeid: u64,
    uid: u32,
    gid: u32,
    total_extlen: u16,
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
    FUSE_ASYNC_READ,
    FUSE_POSIX_LOCKS,
    FUSE_FILE_OPS,
    FUSE_ATOMIC_O_TRUNC,
    FUSE_EXPORT_SUPPORT,
    FUSE_BIG_WRITES,
    FUSE_DONT_MASK,
    FUSE_SPLICE_WRITE,
    FUSE_SPLICE_MOVE,
    FUSE_SPLICE_READ,
    FUSE_FLOCK_LOCKS,
    FUSE_HAS_IOCTL_DIR,
    FUSE_AUTO_INVAL_DATA,
    FUSE_DO_READDIRPLUS,
    FUSE_READDIRPLUS_AUTO,
    FUSE_ASYNC_DIO,
    FUSE_WRITEBACK_CACHE,
    FUSE_NO_OPEN_SUPPORT,
    FUSE_PARALLEL_DIROPS,
    FUSE_HANDLE_KILLPRIV,
    FUSE_POSIX_ACL,
    FUSE_ABORT_ERROR,
    FUSE_MAX_PAGES,
    fUSE_CACHE_SYMLINKS,
    FUSE_NO_OPENDIR_SUPPORT,
    FUSE_EXPLICIT_INVAL_DATA,
    FUSE_MAP_ALIGNMENT,
    FUSE_SUBMOUNTS,
    FUSE_HANDLE_KILLPRIV_V2,
    FUSE_SETXATTR_EXT,
    FUSE_INIT_EXT,
    FUSE_INIT_RESERVED,
    FUSE_SECURITY_CTX,
    FUSE_HAS_INODE_DAX,
    FUSE_CREATE_SUPP_GROUP,
    FUSE_HAS_EXPIRE_ONLY,
    FUSE_DIRECT_IO_ALLOW_MMAP,
    FUSE_PASSTHROUGH,
    FUSE_NO_EXPORT_SUPPORT,
    FUSE_HAS_RESEND,
    FUSE_ALLOW_IDMAP,
    FUSE_OVER_IO_URING,
    FUSE_REQUEST_TIMEOUT,
    FUSE_HAS_IO_URING_BUFPOOL,
}
