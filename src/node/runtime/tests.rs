use std::{fs, os::unix::fs::symlink};

use tempfile::TempDir;

use super::{NodeError, daemon::ensure_replica_slot};

#[test]
fn replica_slot_must_be_a_real_directory() {
    let directory = TempDir::new().unwrap();
    let target = directory.path().join("target");
    let link = directory.path().join("replica");
    fs::create_dir(&target).unwrap();
    symlink(&target, &link).unwrap();

    assert!(matches!(
        ensure_replica_slot(&link),
        Err(NodeError::Config(_))
    ));
}
