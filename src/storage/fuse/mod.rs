mod helper;
mod request;
pub mod session;
mod transport;

use bitflags;

// Because `Arc<Filesystem>` must be `Sync`, `Filesystem` must be `Send + Sync`.
pub trait FileSystem: Send + Sync + 'static {
    /// Initialize filesystem.
    /// Corresponds to FUSE opcode 26.
    fn init() {
        todo!()
    }

    /// Clean up filesystem.
    /// Corresponds to FUSE opcode 38.
    fn destroy() {
        todo!()
    }

    /// Clean up filesystem.
    /// Corresponds to FUSE opcode 1.
    fn lookup() {
        todo!()
    }

    /// Corresponds to FUSE opcode 2.
    fn forget() {
        todo!()
    }

    /// Corresponds to FUSE opcode 3.
    fn getattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 4.
    fn setattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 5.
    fn readlink() {
        todo!()
    }

    /// Corresponds to FUSE opcode 6.
    fn symlink() {
        todo!()
    }

    /// Corresponds to FUSE opcode 8.
    fn mknod() {
        todo!()
    }

    /// Corresponds to FUSE opcode 9.
    fn mkdir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 10.
    fn unlink() {
        todo!()
    }

    /// Corresponds to FUSE opcode 11.
    fn rmdir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 12.
    fn rename() {
        todo!()
    }

    /// Corresponds to FUSE opcode 13.
    fn link() {
        todo!()
    }

    /// Corresponds to FUSE opcode 14.
    fn open() {
        todo!()
    }

    /// Corresponds to FUSE opcode 15.
    fn read() {
        todo!()
    }

    /// Corresponds to FUSE opcode 16.
    fn write() {
        todo!()
    }

    /// Corresponds to FUSE opcode 17.
    fn statfs() {
        todo!()
    }

    /// Corresponds to FUSE opcode 18.
    fn release() {
        todo!()
    }

    /// Corresponds to FUSE opcode 20.
    fn fsync() {
        todo!()
    }

    /// Corresponds to FUSE opcode 21.
    fn setxattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 22.
    fn getxattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 23.
    fn listxattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 24.
    fn removexattr() {
        todo!()
    }

    /// Corresponds to FUSE opcode 25.
    fn flush() {
        todo!()
    }

    /// Corresponds to FUSE opcode 27. (26 is INIT)
    fn opendir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 28.
    fn readdir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 29.
    fn releasedir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 30.
    fn fsyncdir() {
        todo!()
    }

    /// Corresponds to FUSE opcode 31.
    fn getlk() {
        todo!()
    }

    /// Corresponds to FUSE opcode 32.
    fn setlk() {
        todo!()
    }

    /// Corresponds to FUSE opcode 33.
    fn setlkw() {
        todo!()
    }

    /// Corresponds to FUSE opcode 34.
    fn access() {
        todo!()
    }

    /// Atomically create and open a regular file.
    /// Corresponds to FUSE opcode 35.
    fn create() {
        todo!()
    }

    /// Corresponds to FUSE opcode 36.
    fn interrupt() {
        todo!()
    }

    /// Corresponds to FUSE opcode 39.
    fn ioctl() {
        todo!()
    }

    /// Corresponds to FUSE opcode 40.
    fn poll() {
        todo!()
    }

    // Other util function
    fn get_name(&self) -> String {
        String::from("fuse_fs")
    }
}

bitflags::bitflags! {
    /// FUSE capabilities negotiated with the Linux kernel during `INIT`.
    ///
    /// Use `HandleState::negotiated_config` to choose which protocol features
    /// to use for this session. `None` means no negotiation result is available.
    /// With `Some(flags)`, check the capability before using its feature.
    ///
    /// For example, use `splice` to send replies to `/dev/fuse` when
    /// `SPLICE_WRITE` is set. Otherwise, send replies with a regular write.
    ///
    /// ```
    /// use onelastleaf::storage::fuse::session::Flags;
    ///
    /// fn can_splice_replies(negotiated_config: Option<Flags>) -> bool {
    ///     negotiated_config.is_some_and(|flags| flags.contains(Flags::SPLICE_WRITE))
    /// }
    /// ```
    ///
    /// Combine capabilities with `|`. Use [`contains`](Self::contains) to require
    /// all of them or [`intersects`](Self::intersects) to require any of them.
    /// For logging, use [`iter_names`](Self::iter_names) or [`bits`](Self::bits).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Flags: u64 {
        const ASYNC_READ   = 1 << 0;
        const POSIX_LOCKS = 1 << 1;
        const FILE_OPS = 1 << 2;
        const ATOMIC_O_TRUNC = 1 << 3;
        const EXPORT_SUPPORT = 1 << 4;
        const BIG_WRITES = 1 << 5;
        const DONT_MASK = 1 << 6;
        const SPLICE_WRITE = 1 << 7;
        const SPLICE_MOVE = 1 << 8;
        const SPLICE_READ = 1 << 9;
        const FLOCK_LOCKS = 1 << 10;
        const HAS_IOCTL_DIR = 1 << 11;
        const AUTO_INVAL_DATA = 1 << 12;
        const DO_READDIRPLUS = 1 << 13;
        const READDIRPLUS_AUTO = 1 << 14;
        const ASYNC_DIO = 1 << 15;
        const WRITEBACK_CACHE = 1 << 16;
        const NO_OPEN_SUPPORT = 1 << 17;
        const PARALLEL_DIROPS = 1 << 18;
        const HANDLE_KILLPRIV = 1 << 19;
        const POSIX_ACL = 1 << 20;
        const ABORT_ERROR = 1 << 21;
        const MAX_PAGES = 1 << 22;
        const CACHE_SYMLINKS = 1 << 23;
        const NO_OPENDIR_SUPPORT = 1 << 24;
        const EXPLICIT_INVAL_DATA = 1 << 25;
        const MAP_ALIGNMENT = 1 << 26;
        const SUBMOUNTS = 1 << 27;
        const HANDLE_KILLPRIV_V2 = 1 << 28;
        const SETXATTR_EXT = 1 << 29;
        const INIT_EXT     = 1 << 30;
        const INIT_RESERVED = 1 << 31;
        const SECURITY_CTX = 1 << 32;
        const HAS_INODE_DAX = 1 << 33;
        const CREATE_SUPP_GROUP = 1 << 34;
        const HAS_EXPIRE_ONLY = 1 << 35;
        const DIRECT_IO_ALLOW_MMAP = 1 << 36;
        const PASSTHROUGH = 1 << 37;
        const NO_EXPORT_SUPPORT = 1 << 38;
        const HAS_RESEND = 1 << 39;
        const DIRECT_IO_RELAX = Self::DIRECT_IO_ALLOW_MMAP.bits();
        const ALLOW_IDMAP = 1 << 40;
        const OVER_IO_URING = 1 << 41;
        const REQUEST_TIMEOUT = 1 << 42;
        const HAS_IO_URING_BUFPOOL = 1 << 43;
        const HAS_SYNCFS = 1 << 44;
        const PASSTHROUGH_V2 = 1 << 45;
    }
}
