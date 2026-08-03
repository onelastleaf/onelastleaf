use super::*;

#[test]
fn archive_contract_rejects_zstd_corruption_and_trailing_data() {
    let directory = TempDir::new().unwrap();
    let fixture = test_snapshot();
    let valid = directory.path().join("valid.ollsnap");
    write_test_archive(
        &valid,
        manifest_source(&fixture.manifest),
        &fixture.entries(),
    );
    verify_snapshot(&valid).unwrap();

    let mut corrupted = fs::read(&valid).unwrap();
    let final_byte = corrupted.last_mut().unwrap();
    *final_byte ^= 0xff;
    let corrupt_path = directory.path().join("corrupt.ollsnap");
    fs::write(&corrupt_path, corrupted).unwrap();
    assert!(matches!(
        verify_snapshot(&corrupt_path),
        Err(ReplicaError::InvalidSnapshot(_))
    ));

    let trailing_path = directory.path().join("trailing.ollsnap");
    fs::copy(&valid, &trailing_path).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&trailing_path)
        .unwrap()
        .write_all(b"trailing payload")
        .unwrap();
    assert!(matches!(
        verify_snapshot(&trailing_path),
        Err(ReplicaError::InvalidSnapshot(_))
    ));
}

#[test]
fn malformed_archive_path_cannot_escape_verification_staging() {
    let directory = TempDir::new().unwrap();
    let snapshot = directory.path().join("malicious.ollsnap");
    let escaped = directory.path().join("escaped");
    let manifest = Manifest {
        format: SNAPSHOT_FORMAT.to_owned(),
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id: Uuid::new_v4().to_string(),
        replica_id: Uuid::new_v4().to_string(),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
        catalog: ManifestObject {
            entry: CATALOG_ENTRY.to_owned(),
            size_bytes: 0,
            sha256: hex_sha256(&[]),
        },
        documents: Vec::new(),
        blobs: Vec::new(),
    };
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    manifest_bytes.push(b'\n');

    let output = File::create(&snapshot).unwrap();
    let mut encoder = zstd::stream::write::Encoder::new(output, 1).unwrap();
    {
        let mut archive = Builder::new(&mut encoder);
        let mut header = Header::new_ustar();
        header.set_entry_type(EntryType::Regular);
        header.set_size(manifest_bytes.len() as u64);
        header.set_mode(0o600);
        header.set_path(MANIFEST_ENTRY).unwrap();
        header.set_cksum();
        archive
            .append(&header, Cursor::new(manifest_bytes))
            .unwrap();

        let malicious_path = escaped.as_os_str().as_encoded_bytes();
        assert!(malicious_path.len() < 100);
        let mut header = Header::new_old();
        header.set_entry_type(EntryType::Regular);
        header.set_size(0);
        header.set_mode(0o600);
        header.as_mut_bytes()[..100].fill(0);
        header.as_mut_bytes()[..malicious_path.len()].copy_from_slice(malicious_path);
        header.set_cksum();
        archive
            .append(&header, Cursor::new(Vec::<u8>::new()))
            .unwrap();
        archive.finish().unwrap();
    }
    encoder.finish().unwrap();

    assert!(matches!(
        verify_snapshot(&snapshot),
        Err(ReplicaError::InvalidSnapshot(_))
    ));
    assert!(!escaped.exists());

    let fixture = test_snapshot();
    let mut entries = fixture.entries();
    entries[0].path = b"../catalog.loro".to_vec();
    assert_invalid(
        &directory,
        "traversal.ollsnap",
        manifest_source(&fixture.manifest),
        entries,
    );
}
