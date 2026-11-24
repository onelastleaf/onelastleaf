mod helper;
pub mod reply;
pub mod request;
pub mod session;

use bitflags;
use nix::{errno::Errno, libc};

use reply::Reply;

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
    fn destroy(&self) {}

    /// This is a **Required Method**.
    /// Corresponds to FUSE opcode 1.
    fn lookup(
        &self,
        header: request::Header,
        payload: request::LookupPayload<'_>,
        reply: Reply<reply::EntryResult>,
    );

    /// Corresponds to FUSE opcode 2.
    ///
    /// Linux sends `FUSE_FORGET` to drop `payload.nlookup` lookup references for the inode
    /// identified by `header.nodeid`. Filesystems that track these references should override this
    /// method and [`Self::batch_forget`] and use the same reference accounting in both.
    ///
    /// The default is a no-op. Linux expects no reply.
    #[expect(unused_variables)]
    fn forget(&self, header: request::Header, payload: request::ForgetPayload) {}

    /// This is a **Required Method**.
    /// Corresponds to FUSE opcode 3.
    fn getattr(
        &self,
        header: request::Header,
        payload: request::GetattrPayload,
        reply: Reply<reply::AttrResult>,
    );

    /// Corresponds to FUSE opcode 4.
    ///
    /// The default replies with `ENOSYS`. The attribute change fails, and Linux can still send
    /// later `FUSE_SETATTR` requests.
    #[expect(unused_variables)]
    fn setattr(
        &self,
        header: request::Header,
        payload: request::SetattrPayload,
        reply: Reply<reply::AttrResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 5.
    ///
    /// The default replies with `ENOSYS`. Linux returns the error from this symlink target lookup
    /// and can still send later `FUSE_READLINK` requests.
    #[expect(unused_variables)]
    fn readlink(&self, header: request::Header, reply: Reply<reply::ReadlinkResult>) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 6.
    ///
    /// The default replies with `ENOSYS`. The symlink creation fails, and Linux can still send
    /// later `FUSE_SYMLINK` requests.
    #[expect(unused_variables)]
    fn symlink(
        &self,
        header: request::Header,
        payload: request::SymlinkPayload<'_>,
        extensions: Option<request::Extensions<'_>>,
        reply: Reply<reply::EntryResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 8.
    ///
    /// The default replies with `ENOSYS`. The node creation fails, and Linux can still send later
    /// `FUSE_MKNOD` requests.
    #[expect(unused_variables)]
    fn mknod(
        &self,
        header: request::Header,
        payload: request::MknodPayload<'_>,
        extensions: Option<request::Extensions<'_>>,
        reply: Reply<reply::EntryResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 9.
    ///
    /// The default replies with `ENOSYS`. The directory creation fails, and Linux can still send
    /// later `FUSE_MKDIR` requests.
    #[expect(unused_variables)]
    fn mkdir(
        &self,
        header: request::Header,
        payload: request::MkdirPayload<'_>,
        extensions: Option<request::Extensions<'_>>,
        reply: Reply<reply::EntryResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 10.
    ///
    /// The default replies with `ENOSYS`. This unlink fails, and Linux can still send later
    /// `FUSE_UNLINK` requests.
    #[expect(unused_variables)]
    fn unlink(
        &self,
        header: request::Header,
        payload: request::UnlinkPayload<'_>,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 11.
    ///
    /// The default replies with `ENOSYS`. The directory removal fails, and Linux can still send
    /// later `FUSE_RMDIR` requests.
    #[expect(unused_variables)]
    fn rmdir(&self, header: request::Header, payload: request::RmdirPayload<'_>, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 12.
    ///
    /// The default replies with `ENOSYS`. This rename without flags fails, and Linux can still send
    /// later `FUSE_RENAME` requests.
    #[expect(unused_variables)]
    fn rename(
        &self,
        header: request::Header,
        payload: request::RenamePayload<'_>,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 13.
    ///
    /// The default replies with `ENOSYS`. Upstream Linux 6.14 returns that error for the current
    /// request. Starting with Linux 6.15, the kernel returns `EPERM` and disables `FUSE_LINK` on
    /// the connection.
    #[expect(unused_variables)]
    fn link(
        &self,
        header: request::Header,
        payload: request::LinkPayload<'_>,
        reply: Reply<reply::EntryResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 14.
    ///
    /// The default replies with `EOPNOTSUPP` when the request carries `O_TRUNC`. Linux passes this
    /// flag in `FUSE_OPEN` only when `FUSE_ATOMIC_O_TRUNC` is negotiated. Filesystems that enable
    /// this capability should override this method to perform the truncation.
    ///
    /// Other opens succeed with `fh = 0` and zero flags and backing IDs. Later `read` and `write`
    /// methods can identify the file through `header.nodeid`.
    #[expect(unused_variables)]
    fn open(
        &self,
        header: request::Header,
        payload: request::OpenPayload,
        reply: Reply<reply::OpenResult>,
    ) {
        if payload.flags & (libc::O_TRUNC as u32) != 0 {
            let _ = reply.complete(Err(Errno::EOPNOTSUPP));
            return;
        }

        let _ = reply.complete(Ok(reply::OpenResult {
            fh: 0,
            open_flags: 0,
            backing_id: 0,
            backing_id_64: 0,
        }));
    }

    /// Corresponds to FUSE opcode 15.
    ///
    /// The default replies with `ENOSYS`. This read request fails, and Linux can still send later
    /// `FUSE_READ` requests.
    #[expect(unused_variables)]
    fn read(
        &self,
        header: request::Header,
        payload: request::ReadPayload,
        reply: Reply<reply::ReadResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 16.
    ///
    /// The default replies with `ENOSYS`. This write request fails, and Linux can still send later
    /// `FUSE_WRITE` requests. With asynchronous writeback, the error may surface on a later
    /// `fsync`.
    #[expect(unused_variables)]
    fn write(
        &self,
        header: request::Header,
        payload: request::WritePayload<'_>,
        reply: Reply<reply::WriteResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 17.
    ///
    /// The default replies with `ENOSYS`. Linux returns the error from this statistics query and
    /// can still send later `FUSE_STATFS` requests.
    #[expect(unused_variables)]
    fn statfs(&self, header: request::Header, reply: Reply<reply::StatfsResult>) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 18.
    ///
    /// The default acknowledges the file handle release with an empty success reply.
    #[expect(unused_variables)]
    fn release(&self, header: request::Header, payload: request::ReleasePayload, reply: Reply<()>) {
        let _ = reply.complete(Ok(()));
    }

    /// Corresponds to FUSE opcode 20.
    ///
    /// The default replies with `EOPNOTSUPP` to report that file synchronization is unsupported.
    /// Linux propagates this error. An `ENOSYS` reply would be treated as success and disable later
    /// `FUSE_FSYNC` requests on the connection.
    #[expect(unused_variables)]
    fn fsync(&self, header: request::Header, payload: request::FsyncPayload, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::EOPNOTSUPP));
    }

    /// Corresponds to FUSE opcode 21.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_SETXATTR` on the connection and
    /// returns `EOPNOTSUPP` for this and later `setxattr` requests.
    #[expect(unused_variables)]
    fn setxattr(
        &self,
        header: request::Header,
        payload: request::SetxattrPayload<'_>,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 22.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_GETXATTR` on the connection and
    /// returns `EOPNOTSUPP` for this and later `getxattr` requests.
    #[expect(unused_variables)]
    fn getxattr(
        &self,
        header: request::Header,
        payload: request::GetxattrPayload<'_>,
        reply: Reply<reply::XattrResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 23.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_LISTXATTR` on the connection and
    /// returns `EOPNOTSUPP` for this and later `listxattr` requests.
    #[expect(unused_variables)]
    fn listxattr(
        &self,
        header: request::Header,
        payload: request::ListxattrPayload,
        reply: Reply<reply::XattrResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 24.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_REMOVEXATTR` on the connection and
    /// returns `EOPNOTSUPP` for this and later `removexattr` requests.
    #[expect(unused_variables)]
    fn removexattr(
        &self,
        header: request::Header,
        payload: request::RemovexattrPayload<'_>,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 25.
    ///
    /// The default acknowledges the current `FUSE_FLUSH` request with an empty success reply. Linux
    /// can still send flush requests for later closes.
    #[expect(unused_variables)]
    fn flush(&self, header: request::Header, payload: request::FlushPayload, reply: Reply<()>) {
        let _ = reply.complete(Ok(()));
    }

    /// Corresponds to FUSE opcode 27. (26 is INIT)
    ///
    /// The default accepts the open with `fh = 0` and zero flags and backing IDs. Later `readdir`
    /// methods can enumerate the directory using `header.nodeid` and the requested offset.
    #[expect(unused_variables)]
    fn opendir(
        &self,
        header: request::Header,
        payload: request::OpenPayload,
        reply: Reply<reply::OpenResult>,
    ) {
        let _ = reply.complete(Ok(reply::OpenResult {
            fh: 0,
            open_flags: 0,
            backing_id: 0,
            backing_id_64: 0,
        }));
    }

    /// Corresponds to FUSE opcode 28.
    ///
    /// The default replies with `ENOSYS`. Linux returns the error from this directory read and can
    /// still send later `FUSE_READDIR` requests.
    #[expect(unused_variables)]
    fn readdir(
        &self,
        header: request::Header,
        payload: request::ReadPayload,
        reply: Reply<reply::ReaddirResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 29.
    ///
    /// The default acknowledges the directory handle release with an empty success reply.
    #[expect(unused_variables)]
    fn releasedir(
        &self,
        header: request::Header,
        payload: request::ReleasePayload,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Ok(()));
    }

    /// Corresponds to FUSE opcode 30.
    ///
    /// The default replies with `EOPNOTSUPP` to report that directory synchronization is
    /// unsupported. Linux propagates this error. An `ENOSYS` reply would be treated as success and
    /// disable later `FUSE_FSYNCDIR` requests on the connection.
    #[expect(unused_variables)]
    fn fsyncdir(&self, header: request::Header, payload: request::FsyncPayload, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::EOPNOTSUPP));
    }

    /// Corresponds to FUSE opcode 31.
    ///
    /// The default replies with `ENOSYS`. Linux returns that error for the current lock query.
    /// Kernel-local POSIX lock testing is selected at `INIT` when `FUSE_POSIX_LOCKS` is absent.
    #[expect(unused_variables)]
    fn getlk(
        &self,
        header: request::Header,
        payload: request::LockPayload,
        reply: Reply<reply::LockResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 32.
    ///
    /// The default replies with `ENOSYS`. Linux returns that error for the current nonblocking lock
    /// or unlock request. It selects kernel-local locking at `INIT` when the corresponding remote
    /// locking capability is absent.
    #[expect(unused_variables)]
    fn setlk(&self, header: request::Header, payload: request::LockPayload, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 33.
    ///
    /// The default replies with `ENOSYS`. The current blocking lock operation fails with that
    /// error. Linux selects local or remote locking at `INIT` and keeps that choice after this
    /// reply.
    #[expect(unused_variables)]
    fn setlkw(&self, header: request::Header, payload: request::LockPayload, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 34.
    ///
    /// The default replies with `EACCES`. Linux fails the current permission check and can still
    /// send `FUSE_ACCESS` requests for later checks.
    #[expect(unused_variables)]
    fn access(&self, header: request::Header, payload: request::AccessPayload, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::EACCES));
    }

    /// Atomically create and open a regular file.
    /// Corresponds to FUSE opcode 35.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_CREATE` on the connection and falls
    /// back to `FUSE_MKNOD` followed by the normal open path. Implement [`Self::mknod`] for regular
    /// files to support creation through this fallback.
    #[expect(unused_variables)]
    fn create(
        &self,
        header: request::Header,
        payload: request::CreatePayload<'_>,
        extensions: Option<request::Extensions<'_>>,
        reply: Reply<reply::CreateResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 36.
    #[expect(unused_variables)]
    fn interrupt(&self, header: request::Header, payload: request::InterruptPayload) {}

    /// Map a logical file block to a block on the backing device.
    /// Corresponds to FUSE opcode 37.
    ///
    /// Linux sends this request only for filesystems backed by a block device. Block numbers are in
    /// units of `payload.blocksize`.
    ///
    /// The default replies with `ENOSYS`. Linux returns block number zero to indicate that no
    /// mapping is available and disables further `FUSE_BMAP` requests on the connection.
    #[expect(unused_variables)]
    fn bmap(
        &self,
        header: request::Header,
        payload: request::BmapPayload,
        reply: Reply<reply::BmapResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 39.
    ///
    /// The default replies with `ENOTTY`. Linux reports the requested ioctl command as unsupported.
    #[expect(unused_variables)]
    fn ioctl(
        &self,
        header: request::Header,
        payload: request::IoctlPayload<'_>,
        reply: Reply<reply::IoctlResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOTTY));
    }

    /// Corresponds to FUSE opcode 40.
    ///
    /// The default replies with `ENOSYS`. Linux uses its default readable and writable event mask
    /// for this poll and skips further `FUSE_POLL` requests on the connection.
    #[expect(unused_variables)]
    fn poll(
        &self,
        header: request::Header,
        payload: request::PollPayload,
        reply: Reply<reply::PollResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 41.
    ///
    /// Linux sends `FUSE_NOTIFY_REPLY` with cached file data in response to `FUSE_NOTIFY_RETRIEVE`.
    /// The `header.unique` field carries the original notification's `notify_unique`. A session
    /// that sends retrieve notifications should match this ID to the waiting caller and deliver the
    /// offset and data from the payload.
    ///
    /// The default is a no-op. Linux expects no reply.
    #[expect(unused_variables)]
    fn notify_reply(&self, header: request::Header, payload: request::NotifyReplyPayload<'_>) {}

    /// Corresponds to FUSE opcode 42.
    ///
    /// Linux sends `FUSE_BATCH_FORGET` with a list of inode lookup reference decrements. Use each
    /// entry's own `nodeid` and `nlookup` and apply the same accounting as in [`Self::forget`].
    ///
    /// The default is a no-op. Linux expects no reply.
    #[expect(unused_variables)]
    fn batch_forget(&self, header: request::Header, payload: request::BatchForgetPayload) {}

    /// Corresponds to FUSE opcode 43.
    ///
    /// The default replies with `ENOSYS`. Linux returns `EOPNOTSUPP` and disables `FUSE_FALLOCATE`
    /// on the connection for every file and mode.
    #[expect(unused_variables)]
    fn fallocate(
        &self,
        header: request::Header,
        payload: request::FallocatePayload,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 44.
    ///
    /// The default replies with `ENOSYS`. Linux fails the current directory read and keeps the
    /// negotiated READDIRPLUS mode. Leave `FUSE_DO_READDIRPLUS` disabled at `INIT` to use ordinary
    /// `FUSE_READDIR`.
    #[expect(unused_variables)]
    fn readdir_plus(
        &self,
        header: request::Header,
        payload: request::ReadPayload,
        reply: Reply<reply::ReaddirPlusResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 45.
    ///
    /// The default replies with `ENOSYS`. Linux returns `EINVAL` and disables `FUSE_RENAME2` on the
    /// connection. Renames without flags can still use `FUSE_RENAME`.
    #[expect(unused_variables)]
    fn rename2(
        &self,
        header: request::Header,
        payload: request::Rename2Payload<'_>,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 46.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_LSEEK` on the connection and uses
    /// generic `SEEK_DATA` and `SEEK_HOLE` behavior. For offsets before EOF, the requested offset
    /// is treated as data and EOF is the next hole. Override this method to report actual sparse
    /// regions.
    #[expect(unused_variables)]
    fn lseek(
        &self,
        header: request::Header,
        payload: request::LseekPayload,
        reply: Reply<reply::LseekResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 47.
    ///
    /// The default replies with `ENOSYS`. Linux disables FUSE range copying on the connection and
    /// falls back to copying through splice. The fallback depends on read support on the source and
    /// write support on the destination.
    #[expect(unused_variables)]
    fn copy_file_range(
        &self,
        header: request::Header,
        payload: request::CopyFileRangePayload,
        reply: Reply<reply::WriteResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 48.
    ///
    /// The default replies with `ENOSYS`. Linux propagates the error from DAX mapping setup or from
    /// an upgrade of an existing mapping to writable.
    #[expect(unused_variables)]
    fn setupmapping(
        &self,
        header: request::Header,
        payload: request::SetupmappingPayload,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 49.
    ///
    /// The default replies with `ENOSYS`. Linux treats the DAX unmap as failed. During mapping
    /// reclaim or inode eviction it logs the failure and proceeds with local mapping cleanup.
    #[expect(unused_variables)]
    fn removemapping(
        &self,
        header: request::Header,
        payload: request::RemovemappingPayload,
        reply: Reply<()>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 50.
    ///
    /// The default replies with `EOPNOTSUPP` to report that filesystem-wide synchronization is
    /// unsupported. Linux propagates this error. An `ENOSYS` reply would be treated as success and
    /// disable later `FUSE_SYNCFS` requests on the connection.
    ///
    /// The default `INIT` configuration leaves `FUSE_HAS_SYNCFS` unset for kernels that negotiate
    /// it.
    #[expect(unused_variables)]
    fn syncfs(&self, header: request::Header, reply: Reply<()>) {
        let _ = reply.complete(Err(Errno::EOPNOTSUPP));
    }

    /// Corresponds to FUSE opcode 51.
    ///
    /// The default replies with `ENOSYS`. Linux returns `EOPNOTSUPP` and disables further
    /// `FUSE_TMPFILE` requests on the connection.
    #[expect(unused_variables)]
    fn tmpfile(
        &self,
        header: request::Header,
        payload: request::CreatePayload<'_>,
        extensions: Option<request::Extensions<'_>>,
        reply: Reply<reply::CreateResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 52.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_STATX` on the connection and falls
    /// back to `FUSE_GETATTR` for basic attributes. Override this method to provide additional stat
    /// fields such as birth time.
    #[expect(unused_variables)]
    fn statx(
        &self,
        header: request::Header,
        payload: request::StatxPayload,
        reply: Reply<reply::StatxResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
    }

    /// Corresponds to FUSE opcode 53.
    ///
    /// The default replies with `ENOSYS`. Linux disables `FUSE_COPY_FILE_RANGE_64` on the
    /// connection and retries with `FUSE_COPY_FILE_RANGE`, capping the request length at the
    /// largest page-aligned 32-bit unsigned value. If the legacy opcode also replies with `ENOSYS`,
    /// Linux falls back to copying through splice.
    #[expect(unused_variables)]
    fn copy_file_range64(
        &self,
        header: request::Header,
        payload: request::CopyFileRangePayload,
        reply: Reply<reply::CopyFileRangeResult>,
    ) {
        let _ = reply.complete(Err(Errno::ENOSYS));
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

    // Maximum number of data bytes in a single write request. Defaults to 4096 bytes (4 KiB).
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
            max_write: 4096,
            max_pages: 0,
            max_background: 0,
            congestion_threshold: 0,
            request_timeout: 0,
            map_alignment: 0,
            max_stack_depth: 0,
        }
    }
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
    pub start: u64,
    pub end: u64,
    pub lock_type: LockType,
    pub pid: u32,
}

/// The lock type carried in the `type` field of `fuse_file_lock`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum LockType {
    Read = libc::F_RDLCK as u32,
    Write = libc::F_WRLCK as u32,
    Unlock = libc::F_UNLCK as u32,
}

impl TryFrom<u32> for LockType {
    type Error = session::ParseError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value as libc::c_int {
            libc::F_RDLCK => Ok(Self::Read),
            libc::F_WRLCK => Ok(Self::Write),
            libc::F_UNLCK => Ok(Self::Unlock),
            _ => Err(session::ParseError::InvalidLockType(value)),
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
    /// use onelastleaf::storage::fuse::Flags;
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
