use std::collections::BTreeSet;

use time::format_description::well_known::Rfc3339;

use super::{
    super::ReplicaError,
    CATALOG_ENTRY, SNAPSHOT_FORMAT, SNAPSHOT_FORMAT_VERSION,
    support::{hex_sha256, parse_manifest_uuid},
    types::{Manifest, ManifestDocumentState, ManifestObject, SnapshotInspection},
};

pub(super) fn validate_manifest(manifest: &Manifest) -> Result<(), ReplicaError> {
    if manifest.format != SNAPSHOT_FORMAT {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot format marker is not recognized".to_owned(),
        ));
    }
    if manifest.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(ReplicaError::InvalidSnapshot(
            "snapshot format_version is not supported".to_owned(),
        ));
    }
    parse_manifest_uuid(&manifest.snapshot_id, "snapshot_id")?;
    parse_manifest_uuid(&manifest.replica_id, "replica_id")?;
    let created = time::OffsetDateTime::parse(&manifest.created_at, &Rfc3339).map_err(|_| {
        ReplicaError::InvalidSnapshot("created_at is not an RFC 3339 timestamp".to_owned())
    })?;
    if created.offset() != time::UtcOffset::UTC {
        return Err(ReplicaError::InvalidSnapshot(
            "created_at must use UTC".to_owned(),
        ));
    }
    if manifest.catalog.entry != CATALOG_ENTRY {
        return Err(ReplicaError::InvalidSnapshot(
            "catalog entry must be catalog.loro".to_owned(),
        ));
    }
    validate_sha256(&manifest.catalog.sha256)?;

    let mut previous_document = None;
    let mut document_ids = BTreeSet::new();
    for document in &manifest.documents {
        let id = parse_manifest_uuid(&document.document_id, "document_id")?;
        if !document_ids.insert(id) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest repeats a document_id".to_owned(),
            ));
        }
        if previous_document.is_some_and(|previous| previous >= id) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest documents are not in canonical order".to_owned(),
            ));
        }
        previous_document = Some(id);
        if document.entry != format!("documents/{id}.loro") {
            return Err(ReplicaError::InvalidSnapshot(
                "document archive entry does not match its DocumentId".to_owned(),
            ));
        }
        validate_sha256(&document.sha256)?;
    }

    let mut previous_blob: Option<&str> = None;
    let mut hashes = BTreeSet::new();
    for blob in &manifest.blobs {
        validate_sha256(&blob.sha256)?;
        if !hashes.insert(blob.sha256.as_str()) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest repeats a blob hash".to_owned(),
            ));
        }
        if previous_blob.is_some_and(|previous| previous >= blob.sha256.as_str()) {
            return Err(ReplicaError::InvalidSnapshot(
                "manifest blobs are not in canonical order".to_owned(),
            ));
        }
        previous_blob = Some(&blob.sha256);
        if blob.entry != format!("blobs/{}", blob.sha256) {
            return Err(ReplicaError::InvalidSnapshot(
                "blob archive entry does not match its SHA-256".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(super) fn inspection(manifest: &Manifest) -> Result<SnapshotInspection, ReplicaError> {
    validate_manifest(manifest)?;
    let mut live_documents = 0_u64;
    let mut tombstoned_documents = 0_u64;
    let mut document_bytes = 0_u64;
    for document in &manifest.documents {
        match document.state {
            ManifestDocumentState::Live => {
                live_documents = live_documents.checked_add(1).ok_or_else(size_overflow)?;
            }
            ManifestDocumentState::Tombstoned => {
                tombstoned_documents = tombstoned_documents
                    .checked_add(1)
                    .ok_or_else(size_overflow)?;
            }
        }
        document_bytes = document_bytes
            .checked_add(document.size_bytes)
            .ok_or_else(size_overflow)?;
    }
    let mut blob_bytes = 0_u64;
    for blob in &manifest.blobs {
        blob_bytes = blob_bytes
            .checked_add(blob.size_bytes)
            .ok_or_else(size_overflow)?;
    }
    Ok(SnapshotInspection {
        format: manifest.format.clone(),
        format_version: manifest.format_version,
        snapshot_id: manifest.snapshot_id.clone(),
        replica_id: manifest.replica_id.clone(),
        created_at: manifest.created_at.clone(),
        live_documents,
        tombstoned_documents,
        blobs: u64::try_from(manifest.blobs.len()).map_err(|_| size_overflow())?,
        catalog_bytes: manifest.catalog.size_bytes,
        document_bytes,
        blob_bytes,
    })
}

pub(super) fn parse_manifest(source: &[u8]) -> Result<Manifest, ReplicaError> {
    serde_json::from_slice(source).map_err(|_| {
        ReplicaError::InvalidSnapshot("manifest.json is not strict versioned JSON".to_owned())
    })
}

pub(super) fn manifest_object(entry: &str, bytes: &[u8]) -> Result<ManifestObject, ReplicaError> {
    Ok(ManifestObject {
        entry: entry.to_owned(),
        size_bytes: u64::try_from(bytes.len())
            .map_err(|_| ReplicaError::Internal("snapshot entry size overflow".to_owned()))?,
        sha256: hex_sha256(bytes),
    })
}

fn validate_sha256(value: &str) -> Result<(), ReplicaError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplicaError::InvalidSnapshot(
            "SHA-256 must be lower-case 64-character hex".to_owned(),
        ));
    }
    Ok(())
}

fn size_overflow() -> ReplicaError {
    ReplicaError::InvalidSnapshot("snapshot manifest size total overflows u64".to_owned())
}
