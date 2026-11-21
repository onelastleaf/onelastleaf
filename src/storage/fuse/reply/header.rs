/// The definition of the corresponding C struct.
/// ```c
/// struct fuse_out_header {
///     uint32_t len;
///     int32_t error;
///     uint64_t unique;
/// };
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}
