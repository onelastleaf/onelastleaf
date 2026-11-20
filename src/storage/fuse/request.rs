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
    pub(super) len: u32,
    pub(super) opcode: u32,
    pub(super) unique: u64,
    pub(super) nodeid: u64,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) pid: u32,
    pub(super) total_extlen: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extensions<'a> {
    pub(super) security_contexts: Option<Vec<SecurityContext<'a>>>,
    pub(super) supplementary_groups: Option<Vec<u32>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SecurityContext<'a> {
    pub(super) name: &'a [u8],
    pub(super) label: &'a [u8],
}
