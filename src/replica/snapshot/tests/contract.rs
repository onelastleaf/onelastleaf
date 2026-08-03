use super::*;

#[test]
fn archive_contract_rejects_order_set_type_size_and_hash_violations() {
    let directory = TempDir::new().unwrap();
    let fixture = test_snapshot();
    let canonical = fixture.entries();
    let source = manifest_source(&fixture.manifest);
    let valid = directory.path().join("valid.ollsnap");
    write_test_archive(&valid, source.clone(), &canonical);
    verify_snapshot(&valid).unwrap();

    let mut wrong_order = canonical.clone();
    wrong_order.swap(0, 1);
    assert_invalid(
        &directory,
        "wrong-order.ollsnap",
        source.clone(),
        wrong_order,
    );

    let mut duplicate = canonical.clone();
    duplicate.insert(1, canonical[0].clone());
    assert_invalid(
        &directory,
        "duplicate-entry.ollsnap",
        source.clone(),
        duplicate,
    );

    let mut undeclared = canonical.clone();
    undeclared.push(TestArchiveEntry::regular("extra", b"extra".to_vec()));
    assert_invalid(
        &directory,
        "undeclared-entry.ollsnap",
        source.clone(),
        undeclared,
    );

    for (name, entry_type) in [
        ("link-entry.ollsnap", EntryType::Symlink),
        ("special-entry.ollsnap", EntryType::Fifo),
    ] {
        let mut entries = canonical.clone();
        entries[0] = TestArchiveEntry::typed(CATALOG_ENTRY, entry_type);
        assert_invalid(&directory, name, source.clone(), entries);
    }

    let mut wrong_size = fixture.manifest.clone();
    wrong_size.catalog.size_bytes += 1;
    assert_invalid(
        &directory,
        "wrong-size.ollsnap",
        manifest_source(&wrong_size),
        canonical.clone(),
    );

    let mut wrong_hash = fixture.manifest.clone();
    wrong_hash.catalog.sha256 = hex_sha256(b"wrong catalog");
    assert_invalid(
        &directory,
        "wrong-hash.ollsnap",
        manifest_source(&wrong_hash),
        canonical,
    );
}

#[test]
fn archive_contract_rejects_reference_schema_and_loro_violations() {
    let directory = TempDir::new().unwrap();
    let fixture = test_snapshot();

    let mut missing_document = fixture.clone();
    missing_document.manifest.documents.clear();
    let entries = missing_document.entries();
    assert_invalid(
        &directory,
        "missing-document-reference.ollsnap",
        manifest_source(&missing_document.manifest),
        entries,
    );

    let mut missing_blob = fixture.clone();
    missing_blob.manifest.blobs.clear();
    let entries = missing_blob.entries();
    assert_invalid(
        &directory,
        "missing-blob-reference.ollsnap",
        manifest_source(&missing_blob.manifest),
        entries,
    );

    let mut extra_blob = fixture.clone();
    let bytes = b"unreferenced blob".to_vec();
    let sha256 = hex_sha256(&bytes);
    let entry = format!("blobs/{sha256}");
    extra_blob.manifest.blobs.push(ManifestBlob {
        entry: entry.clone(),
        size_bytes: bytes.len() as u64,
        sha256,
    });
    extra_blob
        .manifest
        .blobs
        .sort_by(|left, right| left.sha256.cmp(&right.sha256));
    extra_blob.payloads.insert(entry, bytes);
    let entries = extra_blob.entries();
    assert_invalid(
        &directory,
        "extra-blob.ollsnap",
        manifest_source(&extra_blob.manifest),
        entries,
    );

    let wrong_catalog = {
        let doc = loro::LoroDoc::new();
        doc.set_peer_id(7).unwrap();
        doc.get_map("wrong").insert("field", 1).unwrap();
        doc.commit();
        doc.export(loro::ExportMode::Snapshot).unwrap()
    };
    let mut invalid_catalog = fixture.clone();
    invalid_catalog.manifest.catalog.size_bytes = wrong_catalog.len() as u64;
    invalid_catalog.manifest.catalog.sha256 = hex_sha256(&wrong_catalog);
    invalid_catalog
        .payloads
        .insert(CATALOG_ENTRY.to_owned(), wrong_catalog);
    let entries = invalid_catalog.entries();
    assert_invalid(
        &directory,
        "invalid-catalog-schema.ollsnap",
        manifest_source(&invalid_catalog.manifest),
        entries,
    );

    let wrong_document = {
        let doc = loro::LoroDoc::new();
        doc.set_peer_id(8).unwrap();
        doc.get_map("content").insert("wrong", true).unwrap();
        doc.get_map("data");
        doc.commit();
        doc.export(loro::ExportMode::Snapshot).unwrap()
    };
    let mut invalid_document = fixture.clone();
    let declared = invalid_document.manifest.documents.first_mut().unwrap();
    declared.size_bytes = wrong_document.len() as u64;
    declared.sha256 = hex_sha256(&wrong_document);
    invalid_document
        .payloads
        .insert(declared.entry.clone(), wrong_document);
    let entries = invalid_document.entries();
    assert_invalid(
        &directory,
        "invalid-document-schema.ollsnap",
        manifest_source(&invalid_document.manifest),
        entries,
    );

    let undecodable = b"not a Loro snapshot".to_vec();
    let mut invalid_loro = fixture;
    invalid_loro.manifest.catalog.size_bytes = undecodable.len() as u64;
    invalid_loro.manifest.catalog.sha256 = hex_sha256(&undecodable);
    invalid_loro
        .payloads
        .insert(CATALOG_ENTRY.to_owned(), undecodable);
    let entries = invalid_loro.entries();
    assert_invalid(
        &directory,
        "undecodable-loro.ollsnap",
        manifest_source(&invalid_loro.manifest),
        entries,
    );
}

#[test]
fn archive_contract_validates_catalog_document_and_binary_payload_sizes() {
    let directory = TempDir::new().unwrap();
    let fixture = test_snapshot();

    let mut wrong_document_size = fixture.clone();
    wrong_document_size.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.size_bytes += 1;
    });
    assert_invalid(
        &directory,
        "catalog-document-size.ollsnap",
        manifest_source(&wrong_document_size.manifest),
        wrong_document_size.entries(),
    );

    let mut wrong_binary_size = fixture.clone();
    wrong_binary_size.mutate_catalog(|entries| {
        let version = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Binary(binary) => binary.versions.values_mut().next(),
                _ => None,
            })
            .unwrap();
        version.size_bytes += 1;
    });
    assert_invalid(
        &directory,
        "catalog-binary-size.ollsnap",
        manifest_source(&wrong_binary_size.manifest),
        wrong_binary_size.entries(),
    );

    let mut utf16 = fixture.clone();
    utf16.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.encoding = "UTF-16LE".to_owned();
        document.has_byte_order_mark = true;
        document.size_bytes = encode_text("snapshot document", "UTF-16LE", true)
            .unwrap()
            .0
            .len() as u64;
    });
    let valid_utf16 = directory.path().join("valid-utf16-bom.ollsnap");
    write_test_archive(
        &valid_utf16,
        manifest_source(&utf16.manifest),
        &utf16.entries(),
    );
    verify_snapshot(&valid_utf16).unwrap();

    let mut wrong_utf16_size = utf16;
    wrong_utf16_size.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.size_bytes += 1;
    });
    assert_invalid(
        &directory,
        "wrong-utf16-bom-size.ollsnap",
        manifest_source(&wrong_utf16_size.manifest),
        wrong_utf16_size.entries(),
    );

    let mut invalid_bom = fixture.clone();
    invalid_bom.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.encoding = "windows-1252".to_owned();
        document.has_byte_order_mark = true;
    });
    assert_invalid(
        &directory,
        "invalid-bom-encoding.ollsnap",
        manifest_source(&invalid_bom.manifest),
        invalid_bom.entries(),
    );

    let mut unrepresentable = fixture;
    unrepresentable.replace_first_document_text("snapshot 🍃");
    unrepresentable.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.encoding = "windows-1252".to_owned();
        document.has_byte_order_mark = false;
        document.size_bytes = "snapshot 🍃".len() as u64;
    });
    assert_invalid(
        &directory,
        "unrepresentable-document.ollsnap",
        manifest_source(&unrepresentable.manifest),
        unrepresentable.entries(),
    );
}
