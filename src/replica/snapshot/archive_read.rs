use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

use sha2::{Digest, Sha256};

use super::super::{ReplicaError, lower_hex};

pub(super) fn validate_regular_entry<R: Read>(
    entry: &tar::Entry<'_, R>,
    expected_path: &str,
) -> Result<(), ReplicaError> {
    if !entry.header().entry_type().is_file() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "snapshot entry {expected_path} is not a regular file"
        )));
    }
    let path = entry.path_bytes();
    if path.as_ref() != expected_path.as_bytes() {
        return Err(ReplicaError::InvalidSnapshot(format!(
            "expected snapshot entry {expected_path}"
        )));
    }
    Ok(())
}

pub(super) fn copy_entry<R: Read>(
    entry: &mut tar::Entry<'_, R>,
    destination: &Path,
) -> Result<String, ReplicaError> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .map_err(|error| ReplicaError::io("create staged snapshot entry", error))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = entry
            .read(&mut buffer)
            .map_err(|error| ReplicaError::io("read snapshot archive entry", error))?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        output
            .write_all(&buffer[..count])
            .map_err(|error| ReplicaError::io("write staged snapshot entry", error))?;
    }
    output
        .sync_all()
        .map_err(|error| ReplicaError::io("sync staged snapshot entry", error))?;
    Ok(lower_hex(&hash.finalize()))
}
