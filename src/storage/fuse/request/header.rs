/// The definition of the corresponding C struct in C:
/// ```c
/// struct fuse_in_header {
///     uint32_t len;
///     uint32_t opcode;
///     uint64_t unique;
///     uint64_t nodeid;
///     uint32_t uid;
///     uint32_t gid;
///     uint32_t pid;
///     uint16_t total_extlen;
///     uint16_t padding;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub total_extlen: u16,
}
