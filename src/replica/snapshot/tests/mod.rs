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

mod archive_safety;
mod contract;
mod manifest;
mod runtime;
