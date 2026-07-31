use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

const PROTO_FILES: &[&str] = &[
    "proto/oll/admin.proto",
    "proto/oll/common.proto",
    "proto/oll/config.proto",
    "proto/oll/document.proto",
    "proto/oll/plugin.proto",
    "proto/oll/replication.proto",
];

const RUNTIME_PROTO_FILES: &[&str] = &[
    "proto/oll/admin.proto",
    "proto/oll/common.proto",
    "proto/oll/document.proto",
];

fn main() -> Result<(), Box<dyn Error>> {
    for path in PROTO_FILES {
        println!("cargo:rerun-if-changed={path}");
    }

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // The build script is single-threaded and sets this before tonic-prost-build
    // starts any work, so no concurrent environment access is possible.
    unsafe {
        env::set_var("PROTOC", &protoc);
    }
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(RUNTIME_PROTO_FILES, &["proto"])?;

    let descriptor = out_dir.join("oll-protocol.pb");
    let status = Command::new(protoc)
        .args(["--fatal_warnings", "-I", "proto", "--include_imports"])
        .arg(format!("--descriptor_set_out={}", descriptor.display()))
        .args(PROTO_FILES)
        .status()?;
    if !status.success() {
        return Err("protoc failed to create the canonical descriptor set".into());
    }

    let hash = Sha256::digest(fs::read(&descriptor)?);
    write_schema_hash(&out_dir.join("protocol_schema.rs"), &hash)?;
    Ok(())
}

fn write_schema_hash(path: &Path, hash: &[u8]) -> Result<(), Box<dyn Error>> {
    let values = hash
        .iter()
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(", ");
    fs::write(
        path,
        format!("pub const PROTOCOL_SCHEMA_SHA256: [u8; 32] = [{values}];\n"),
    )?;
    Ok(())
}
