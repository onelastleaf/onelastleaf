pub mod oll {
    #![allow(clippy::result_large_err)]

    tonic::include_proto!("oll.protocol");

    #[cfg(debug_assertions)]
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/oll-protocol.pb"));
}

include!(concat!(env!("OUT_DIR"), "/protocol_schema.rs"));
