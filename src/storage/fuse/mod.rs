mod helper;
mod request;
pub mod session;
mod transport;

use bitflags;
use nix::errno::Errno;

const SUPPORTED_PROTOCOL_MINOR: u32 = 46;

// Because `Arc<Filesystem>` must be `Sync`, `Filesystem` must be `Send + Sync`.
pub trait FileSystem: Send + Sync + 'static {
    /// Initialize filesystem.
    /// Corresponds to FUSE opcode 26.
    fn init(&self) -> Result<InitConfig, Errno> {
        Ok(InitConfig::default())
    }

    /// Clean up filesystem.
    /// Corresponds to FUSE opcode 38.
    fn destroy(&self) {
        todo!()
    }

    /// Clean up filesystem.
    /// Corresponds to FUSE opcode 1.
    fn lookup(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 2.
    fn forget(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 3.
    fn getattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 4.
    fn setattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 5.
    fn readlink(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 6.
    fn symlink(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 8.
    fn mknod(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 9.
    fn mkdir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 10.
    fn unlink(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 11.
    fn rmdir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 12.
    fn rename(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 13.
    fn link(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 14.
    fn open(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 15.
    fn read(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 16.
    fn write(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 17.
    fn statfs(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 18.
    fn release(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 20.
    fn fsync(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 21.
    fn setxattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 22.
    fn getxattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 23.
    fn listxattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 24.
    fn removexattr(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 25.
    fn flush(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 27. (26 is INIT)
    fn opendir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 28.
    fn readdir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 29.
    fn releasedir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 30.
    fn fsyncdir(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 31.
    fn getlk(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 32.
    fn setlk(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 33.
    fn setlkw(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 34.
    fn access(&self) {
        todo!()
    }

    /// Atomically create and open a regular file.
    /// Corresponds to FUSE opcode 35.
    fn create(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 36.
    fn interrupt(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 39.
    fn ioctl(&self) {
        todo!()
    }

    /// Corresponds to FUSE opcode 40.
    fn poll(&self) {
        todo!()
    }

    // Other util function
    fn get_name(&self) -> String {
        String::from("fuse_fs")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitConfig {
    // Version of protocol, only minor, the major is 7
    pub protocol_version: u32,

    // The INIT flags, contains `flag` and `flag2` in C.
    // Implemented by Rust crate `bitflags`. It can store a capabilities set by a u64.
    pub flags: Flags,

    // Config the readahead window per file, For example, if set to 128 KB, up to 128 KB may be
    // read ahead from the file each time, even if that part has not been read yet.
    pub max_readahead: u32,

    // You know, per file need save timestamp.What granularity do you store timestamps at? 100 ns?
    // 1 μs? 100 μs? Or 1 ms? You need tell kernel this.
    pub time_gran: u32,

    // A single write request can carry at most `max_write` bytes of data. If set to 0, the default
    // is used, which is 4 KiB (4096 bytes).
    pub max_write: u32,

    // A single write request can carry at most the number of bytes corresponding to `max_pages`,
    // which depends on the operating system. For example, assuming a Linux memory page is 4 KiB,
    // if `max_pages = 32`, the maximum amount of data in a single request is 32 * 4 KiB = 128 KiB.
    //
    // Of course, `max_write` also sets a maximum amount of data, and that limit applies as well;
    // the effective limit is the minimum of the two.
    //
    // This takes effect if and only if `FUSE_MAX_PAGES` is enabled.
    //
    // If `FUSE_MAX_PAGES` is not enabled, the limit still exists; the default value is 0, which is
    // actually set to 32 pages.
    //
    // If `FUSE_MAX_PAGES` is enabled and the value is 0, the Linux kernel treats it as 1 page.
    //
    // In this Rust implementation, if you set it to a meaningful value `value != 0` but do not
    // enable `MAX_PAGES` in the flags, the value will be ignored. It will be set to 0.
    //
    // If you enable `MAX_PAGES` in the flags and still set the value to `0`, we interpret
    // that as you wanting to use the kernel's default behavior. We won't stop you; this value will
    // be passed to the kernel exactly as is.
    pub max_pages: u16,

    // The maximum number of background requests that can be in progress at once.
    // Once this limit is reached, new background requests wait until one finishes.
    //
    // A value of 0 means the kernel default (12).
    pub max_background: u16,

    // The number of in-progress background requests at which the kernel treats the filesystem as
    // congested. It then skips asynchronous readahead and delays non-synchronous writeback.
    //
    // A value of 0 means the kernel default (9). This default stays 9 even if `max_background`
    // changes.
    pub congestion_threshold: u16,

    // How long (in seconds) a request may take to complete. If a request times out, the kernel may
    // abort the FUSE connection.
    //
    // Used when `REQUEST_TIMEOUT` is enabled. A value of 0 lets the kernel choose the timeout.
    pub request_timeout: u16,

    // The alignment (as a power of 2) for file and memory offsets in DAX mapping requests. For
    // example, 12 means 4 KiB alignment, and 0 means byte alignment.
    //
    // This is often needed for DAX mappings between a host and a guest, whose page sizes may
    // differ. For example, the host may use 4 KiB pages while the guest uses 1 KiB pages. A
    // write may then fall on a guest-side boundary, such as 1024 bytes, that is not aligned
    // on the host, which can cause problems.
    //
    // Used when `MAP_ALIGNMENT` is enabled.
    pub map_alignment: u16,

    // The maximum stacking depth for passthrough, counting this FUSE mount as one layer. A value
    // of 1 allows backing files on filesystems with no stacking. A value of 2 also allows backing
    // files on filesystems with one layer of stacking, such as overlayfs.
    //
    // Used when `PASSTHROUGH` or `PASSTHROUGH_V2` is enabled. If it's 0, passthrough is off.
    //
    // However, if you enable `PASSTHROUGH` or `PASSTHROUGH_V2` while setting `max_stack_depth` to
    // 0, that is an error, and we return `Errno::EINVAL`.
    pub max_stack_depth: u32,
}

impl InitConfig {
    pub fn new(protocol_version: u32, flags: Flags, max_readahead: u32, time_gran: u32) -> Self {
        Self {
            protocol_version,
            flags,
            max_readahead,
            time_gran,
            ..Self::default()
        }
    }
}

impl Default for InitConfig {
    fn default() -> Self {
        Self {
            protocol_version: SUPPORTED_PROTOCOL_MINOR,
            flags: Flags::empty(),
            max_readahead: 0,
            time_gran: 1,
            max_write: 0,
            max_pages: 0,
            max_background: 0,
            congestion_threshold: 0,
            request_timeout: 0,
            map_alignment: 0,
            max_stack_depth: 0,
        }
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
