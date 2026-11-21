pub struct EntryResult;
pub struct AttrResult;
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
