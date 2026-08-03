use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Cursor, Write},
    path::Path,
    sync::Arc,
};

use tar::{Builder, EntryType, Header};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use super::{
    manifest::parse_manifest,
    support::hex_sha256,
    types::{Manifest, ManifestBlob, ManifestDocument, ManifestDocumentState, ManifestObject},
};
use crate::{
    configuration::ReplicaStoreConfig,
    node::{NodeIdentity, logging::NodeLogger},
    replica::{
        ReplicaError,
        classification::encode_text,
        model::{
            decode_catalog_snapshot, get_entry_record, import_loro_doc, initialize_from_disk,
            scan_working_tree, write_entry_record,
        },
        store::NewBlobSource,
        types::EntryData,
        watcher::ReplicaRuntime,
    },
};

#[derive(Clone)]
struct TestArchiveEntry {
    path: Vec<u8>,
    entry_type: EntryType,
    body: Vec<u8>,
    link_name: Option<String>,
}

impl TestArchiveEntry {
    fn regular(path: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            path: path.into().into_bytes(),
            entry_type: EntryType::Regular,
            body,
            link_name: None,
        }
    }

    fn typed(path: impl Into<String>, entry_type: EntryType) -> Self {
        Self {
            path: path.into().into_bytes(),
            entry_type,
            body: Vec::new(),
            link_name: (entry_type == EntryType::Symlink).then(|| "target".to_owned()),
        }
    }
}

#[derive(Clone)]
struct TestSnapshot {
    manifest: Manifest,
    payloads: BTreeMap<String, Vec<u8>>,
}

impl TestSnapshot {
    fn entries(&self) -> Vec<TestArchiveEntry> {
        let mut entries = Vec::new();
        entries.push(TestArchiveEntry::regular(
            CATALOG_ENTRY,
            self.payloads[CATALOG_ENTRY].clone(),
        ));
        entries.extend(self.manifest.documents.iter().map(|document| {
            TestArchiveEntry::regular(
                document.entry.clone(),
                self.payloads[&document.entry].clone(),
            )
        }));
        entries.extend(self.manifest.blobs.iter().map(|blob| {
            TestArchiveEntry::regular(blob.entry.clone(), self.payloads[&blob.entry].clone())
        }));
        entries
    }

    fn mutate_catalog(
        &mut self,
        mutate: impl FnOnce(&mut BTreeMap<Uuid, super::super::types::CatalogEntry>),
    ) {
        let source = self.payloads[CATALOG_ENTRY].clone();
        let (_, mut entries) = decode_catalog_snapshot(&source).unwrap();
        mutate(&mut entries);
        let catalog = import_loro_doc(&source, 17).unwrap();
        catalog.set_next_commit_origin("snapshot_test");
        let records = catalog.get_map("entries");
        for entry in entries.values() {
            write_entry_record(
                &get_entry_record(&records, entry.catalog_node_id).unwrap(),
                entry,
            )
            .unwrap();
        }
        catalog.commit();
        let encoded = catalog.export(loro::ExportMode::Snapshot).unwrap();
        self.manifest.catalog.size_bytes = encoded.len() as u64;
        self.manifest.catalog.sha256 = hex_sha256(&encoded);
        self.payloads.insert(CATALOG_ENTRY.to_owned(), encoded);
    }

    fn replace_first_document_text(&mut self, text: &str) {
        let declared = self.manifest.documents.first_mut().unwrap();
        let source = self.payloads[&declared.entry].clone();
        let document = import_loro_doc(&source, 19).unwrap();
        document.set_next_commit_origin("snapshot_test");
        document
            .get_text("content")
            .update(text, loro::UpdateOptions::default())
            .unwrap();
        document.commit();
        let encoded = document.export(loro::ExportMode::Snapshot).unwrap();
        declared.size_bytes = encoded.len() as u64;
        declared.sha256 = hex_sha256(&encoded);
        self.payloads.insert(declared.entry.clone(), encoded);
    }
}

fn test_snapshot() -> TestSnapshot {
    let working = TempDir::new().unwrap();
    fs::write(working.path().join("a.md"), "snapshot document").unwrap();
    let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff";
    fs::write(working.path().join("image.gif"), binary).unwrap();
    let disk = scan_working_tree(working.path()).unwrap();
    let change =
        initialize_from_disk(&disk, Uuid::new_v4(), "snapshot-fixture-correlation").unwrap();
    let replica = change.replica;

    let documents = replica
        .documents
        .iter()
        .map(|(document_id, document)| ManifestDocument {
            document_id: document_id.to_string(),
            state: ManifestDocumentState::Live,
            entry: format!("documents/{document_id}.loro"),
            size_bytes: document.loro.len() as u64,
            sha256: hex_sha256(&document.loro),
        })
        .collect::<Vec<_>>();
    let mut payloads = BTreeMap::from([(CATALOG_ENTRY.to_owned(), replica.catalog_loro)]);
    for (document, object) in documents.iter().zip(replica.documents.values()) {
        payloads.insert(document.entry.clone(), object.loro.clone());
    }

    let mut blobs = Vec::new();
    for blob in change.blobs {
        let NewBlobSource::Bytes(bytes) = blob.source else {
            panic!("initial scan fixture must retain blob bytes");
        };
        let entry = format!("blobs/{}", blob.sha256);
        blobs.push(ManifestBlob {
            entry: entry.clone(),
            size_bytes: bytes.len() as u64,
            sha256: blob.sha256,
        });
        payloads.insert(entry, bytes);
    }
    blobs.sort_by(|left, right| left.sha256.cmp(&right.sha256));

    let catalog = &payloads[CATALOG_ENTRY];
    TestSnapshot {
        manifest: Manifest {
            format: SNAPSHOT_FORMAT.to_owned(),
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id: Uuid::new_v4().to_string(),
            replica_id: replica.replica_id.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            catalog: ManifestObject {
                entry: CATALOG_ENTRY.to_owned(),
                size_bytes: catalog.len() as u64,
                sha256: hex_sha256(catalog),
            },
            documents,
            blobs,
        },
        payloads,
    }
}

fn manifest_source(manifest: &Manifest) -> Vec<u8> {
    let mut source = serde_json::to_vec_pretty(manifest).unwrap();
    source.push(b'\n');
    source
}

fn write_test_archive(path: &Path, manifest: Vec<u8>, entries: &[TestArchiveEntry]) {
    let output = File::create(path).unwrap();
    let mut encoder = zstd::stream::write::Encoder::new(output, 1).unwrap();
    encoder.include_checksum(true).unwrap();
    {
        let mut archive = Builder::new(&mut encoder);
        append_test_entry(
            &mut archive,
            &TestArchiveEntry::regular(MANIFEST_ENTRY, manifest),
        );
        for entry in entries {
            append_test_entry(&mut archive, entry);
        }
        archive.finish().unwrap();
    }
    encoder.finish().unwrap();
}

fn append_test_entry<W: Write>(archive: &mut Builder<W>, entry: &TestArchiveEntry) {
    assert!(entry.path.len() < 100);
    let mut header = Header::new_ustar();
    header.as_mut_bytes()[..100].fill(0);
    header.as_mut_bytes()[..entry.path.len()].copy_from_slice(&entry.path);
    header.set_entry_type(entry.entry_type);
    header.set_size(if entry.entry_type.is_file() {
        entry.body.len() as u64
    } else {
        0
    });
    header.set_mode(0o600);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    if let Some(link_name) = &entry.link_name {
        header.set_link_name(link_name).unwrap();
    }
    header.set_cksum();
    archive
        .append(&header, Cursor::new(entry.body.clone()))
        .unwrap();
}

fn assert_invalid(
    directory: &TempDir,
    name: &str,
    manifest: Vec<u8>,
    entries: Vec<TestArchiveEntry>,
) {
    let path = directory.path().join(name);
    write_test_archive(&path, manifest, &entries);
    assert!(
        matches!(
            verify_snapshot(&path),
            Err(ReplicaError::InvalidSnapshot(_))
        ),
        "{name} unexpectedly verified"
    );
}

#[test]
fn strict_manifest_rejects_unknown_and_duplicate_fields() {
    let duplicate = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format":"onelastleaf-replica-snapshot"
        }"#;
    assert!(parse_manifest(duplicate).is_err());

    let unknown = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":1,
          "snapshot_id":"00000000-0000-4000-8000-000000000001",
          "replica_id":"00000000-0000-4000-8000-000000000002",
          "created_at":"2026-01-01T00:00:00Z",
          "catalog":{"entry":"catalog.loro","size_bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"},
          "documents":[],
          "blobs":[],
          "unknown":true
        }"#;
    assert!(parse_manifest(unknown).is_err());

    let wrong_type = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":"1"
        }"#;
    assert!(parse_manifest(wrong_type).is_err());
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_catalog_payload_metadata_cannot_replace_the_active_replica() {
    let directory = TempDir::new().unwrap();
    let mut fixture = test_snapshot();
    fixture.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.size_bytes += 1;
    });
    let snapshot = directory.path().join("invalid-metadata.ollsnap");
    write_test_archive(
        &snapshot,
        manifest_source(&fixture.manifest),
        &fixture.entries(),
    );

    let root = directory.path().join("working");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("original.md"), "authoritative").unwrap();
    let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
    let identities = crate::node::identity::IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
    let runtime = ReplicaRuntime::start(
        directory.path().to_owned(),
        root.clone(),
        &ReplicaStoreConfig::Sqlite {
            path: directory.path().join("store/replica.sqlite3"),
        },
        identities,
        logger,
    )
    .await
    .unwrap();
    let before = runtime.status().await;

    assert!(matches!(
        runtime
            .import_snapshot(&snapshot, "invalid-import-correlation")
            .await,
        Err(ReplicaError::InvalidSnapshot(_))
    ));
    assert_eq!(runtime.status().await, before);
    assert_eq!(
        fs::read_to_string(root.join("original.md")).unwrap(),
        "authoritative"
    );
    runtime
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_export_cleans_its_owned_temporary_archive() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("working");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("document.md"), "snapshot content").unwrap();
    let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
    let identities = crate::node::identity::IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&directory.path().join("log"), identity.clone()).unwrap();
    let runtime = ReplicaRuntime::start(
        directory.path().to_owned(),
        root,
        &ReplicaStoreConfig::Sqlite {
            path: directory.path().join("store/replica.sqlite3"),
        },
        identities,
        logger,
    )
    .await
    .unwrap();
    let destination = directory.path().join("cancelled.ollsnap");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *EXPORT_ARCHIVE_TEST_HOOK.lock().unwrap() = Some(ExportArchiveTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });

    let export_runtime = Arc::clone(&runtime);
    let export_destination = destination.clone();
    let export = tokio::spawn(async move {
        export_runtime
            .export_snapshot(&export_destination, "cancelled-export-correlation")
            .await
    });
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();
    assert!(directory.path().read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".oll-snapshot-")
    }));

    export.abort();
    assert!(export.await.unwrap_err().is_cancelled());
    assert!(!destination.exists());
    release_tx.send(()).unwrap();
    for _ in 0..100 {
        let has_temporary = directory.path().read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".oll-snapshot-")
        });
        if !has_temporary {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!directory.path().read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".oll-snapshot-")
    }));

    runtime
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

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
