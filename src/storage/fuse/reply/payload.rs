pub struct EntryResult {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

pub struct AttrResult {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

pub struct ReadlinkResult;
pub struct OpenResult;
pub struct ReadResult;
pub struct WriteResult;
pub struct StatfsResult;
pub struct XattrResult;
pub struct ReaddirResult {
    pub data: Vec<DirEntry>,
}
pub struct DirEntry;
pub struct LockResult;
pub struct CreateResult;
pub struct BmapResult;
pub struct IoctlResult;
pub struct PollResult;
pub struct ReaddirPlusResult {
    pub data: Vec<DirEntryPlus>,
}
pub struct DirEntryPlus;
pub struct LseekResult;
pub struct StatxResult;
pub struct Statx;
pub struct SxTime;
pub struct CopyFileRangeResult;
