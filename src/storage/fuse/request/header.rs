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
pub(in super::super) struct Header {
    pub(in super::super) len: u32,
    pub(in super::super) opcode: u32,
    pub(in super::super) unique: u64,
    pub(in super::super) nodeid: u64,
    pub(in super::super) uid: u32,
    pub(in super::super) gid: u32,
    pub(in super::super) pid: u32,
    pub(in super::super) total_extlen: u16,
}
