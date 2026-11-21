/// The definition of the corresponding C struct.
/// ```c
/// struct fuse_out_header {
///     uint32_t len;
///     int32_t error;
///     uint64_t unique;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct Header {
    pub(in super::super) len: u32,
    pub(in super::super) error: i32,
    pub(in super::super) unique: u64,
}
