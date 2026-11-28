mod header;
mod payload;

use nix::sys::eventfd::EventFd;
use std::sync::{Arc, mpsc};

use super::{
    ACCESS, BMAP, COPY_FILE_RANGE, COPY_FILE_RANGE_64, CREATE, FALLOCATE, FLUSH, FSYNC, FSYNCDIR,
    GETATTR, GETLK, GETXATTR, IOCTL, LINK, LISTXATTR, LOOKUP, LSEEK, MKDIR, MKNOD, OPEN, OPENDIR,
    POLL, READ, READDIR, READDIRPLUS, READLINK, RELEASE, RELEASEDIR, REMOVEMAPPING, REMOVEXATTR,
    RENAME, RENAME2, RMDIR, SETATTR, SETLK, SETLKW, SETUPMAPPING, SETXATTR, STATFS, STATX, SYMLINK,
    SYNCFS, TMPFILE, UNLINK, WRITE,
};

use super::session::PendingReply;

pub use header::Header;
pub use payload::*;

use nix::errno::Errno;

pub(super) enum Payload {
    Empty,
    Entry(EntryResult),
    Attr(AttrResult),
    Readlink(ReadlinkResult),
    Open(OpenResult),
    Read(ReadResult),
    Write(WriteResult),
    Statfs(StatfsResult),
    Xattr(XattrResult),
    Readdir(ReaddirResult),
    Lock(LockResult),
    Create(CreateResult),
    Bmap(BmapResult),
    Ioctl(IoctlResult),
    Poll(PollResult),
    ReaddirPlus(ReaddirPlusResult),
    Lseek(LseekResult),
    Statx(StatxResult),
    CopyFileRange64(CopyFileRangeResult),
}

/// An error while handing a reply to the session thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyError {
    /// The reply queue's receiver was dropped before the reply could be queued.
    QueueClosed,
    /// The reply was queued, but waking the session failed.
    WakeFdFailed(Errno),
}

impl std::fmt::Display for ReplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueClosed => formatter.write_str("the FUSE reply queue has closed"),
            Self::WakeFdFailed(errno) => {
                write!(formatter, "failed to wake the FUSE session: {errno}")
            }
        }
    }
}

impl std::error::Error for ReplyError {}

#[must_use = "complete the reply or pass it to a worker"]
pub struct Reply<const OP: u32> {
    target: Option<Target>,
}

struct Target {
    unique: u64,
    sender: mpsc::Sender<PendingReply>,
    wake_fd: Arc<EventFd>,
}

impl<const OP: u32> Reply<OP> {
    pub(super) fn new(
        unique: u64,
        sender: mpsc::Sender<PendingReply>,
        wake_fd: Arc<EventFd>,
    ) -> Self {
        let reply_target = Target {
            unique,
            sender,
            wake_fd,
        };
        Self {
            target: Some(reply_target),
        }
    }

    fn submit(&mut self, result: Result<Payload, Errno>) -> Result<(), ReplyError> {
        let Some(target) = self.target.take() else {
            return Ok(());
        };

        target
            .sender
            .send((target.unique, result))
            .map_err(|_| ReplyError::QueueClosed)?;

        match target.wake_fd.write(1) {
            Ok(_) | Err(Errno::EAGAIN) => Ok(()),
            Err(errno) => Err(ReplyError::WakeFdFailed(errno)),
        }
    }
}

impl Reply<{ LOOKUP }> {
    pub fn complete(mut self, result: Result<EntryResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Entry))
    }
}

impl Reply<{ GETATTR }> {
    pub fn complete(mut self, result: Result<AttrResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Attr))
    }
}

impl Reply<{ SETATTR }> {
    pub fn complete(mut self, result: Result<AttrResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Attr))
    }
}

impl Reply<{ READLINK }> {
    pub fn complete(mut self, result: Result<ReadlinkResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Readlink))
    }
}

impl Reply<{ SYMLINK }> {
    pub fn complete(mut self, result: Result<EntryResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Entry))
    }
}

impl Reply<{ MKNOD }> {
    pub fn complete(mut self, result: Result<EntryResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Entry))
    }
}

impl Reply<{ MKDIR }> {
    pub fn complete(mut self, result: Result<EntryResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Entry))
    }
}

impl Reply<{ UNLINK }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ RMDIR }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ RENAME }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ LINK }> {
    pub fn complete(mut self, result: Result<EntryResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Entry))
    }
}

impl Reply<{ OPEN }> {
    pub fn complete(mut self, result: Result<OpenResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Open))
    }
}

impl Reply<{ READ }> {
    pub fn complete(mut self, result: Result<ReadResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Read))
    }
}

impl Reply<{ WRITE }> {
    pub fn complete(mut self, result: Result<WriteResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Write))
    }
}

impl Reply<{ STATFS }> {
    pub fn complete(mut self, result: Result<StatfsResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Statfs))
    }
}

impl Reply<{ RELEASE }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ FSYNC }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ SETXATTR }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ GETXATTR }> {
    pub fn complete(mut self, result: Result<XattrResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Xattr))
    }
}

impl Reply<{ LISTXATTR }> {
    pub fn complete(mut self, result: Result<XattrResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Xattr))
    }
}

impl Reply<{ REMOVEXATTR }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ FLUSH }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ OPENDIR }> {
    pub fn complete(mut self, result: Result<OpenResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Open))
    }
}

impl Reply<{ READDIR }> {
    pub fn complete(mut self, result: Result<ReaddirResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Readdir))
    }
}

impl Reply<{ RELEASEDIR }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ FSYNCDIR }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ GETLK }> {
    pub fn complete(mut self, result: Result<LockResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Lock))
    }
}

impl Reply<{ SETLK }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ SETLKW }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ ACCESS }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ CREATE }> {
    pub fn complete(mut self, result: Result<CreateResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Create))
    }
}

impl Reply<{ BMAP }> {
    pub fn complete(mut self, result: Result<BmapResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Bmap))
    }
}

impl Reply<{ IOCTL }> {
    pub fn complete(mut self, result: Result<IoctlResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Ioctl))
    }
}

impl Reply<{ POLL }> {
    pub fn complete(mut self, result: Result<PollResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Poll))
    }
}

impl Reply<{ FALLOCATE }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ READDIRPLUS }> {
    pub fn complete(mut self, result: Result<ReaddirPlusResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::ReaddirPlus))
    }
}

impl Reply<{ RENAME2 }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ LSEEK }> {
    pub fn complete(mut self, result: Result<LseekResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Lseek))
    }
}

impl Reply<{ COPY_FILE_RANGE }> {
    pub fn complete(mut self, result: Result<WriteResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Write))
    }
}

impl Reply<{ SETUPMAPPING }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ REMOVEMAPPING }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ SYNCFS }> {
    pub fn complete(mut self, result: Result<(), Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(|()| Payload::Empty))
    }
}

impl Reply<{ TMPFILE }> {
    pub fn complete(mut self, result: Result<CreateResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Create))
    }
}

impl Reply<{ STATX }> {
    pub fn complete(mut self, result: Result<StatxResult, Errno>) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::Statx))
    }
}

impl Reply<{ COPY_FILE_RANGE_64 }> {
    pub fn complete(
        mut self,
        result: Result<CopyFileRangeResult, Errno>,
    ) -> Result<(), ReplyError> {
        self.submit(result.map(Payload::CopyFileRange64))
    }
}

impl<const OP: u32> Drop for Reply<OP> {
    fn drop(&mut self) {
        let _ = self.submit(Err(Errno::EIO));
    }
}
