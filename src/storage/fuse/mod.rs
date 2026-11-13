mod transport;

pub fn mount() {
    todo!()
}

pub trait Filesystem {
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
}
