use sha2::{Digest, Sha256};
use sqlx::{Any, Transaction};
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use super::{
    super::{
        ReplicaError, lower_hex,
        types::{ActiveReplica, EntryData, OperationRecord},
    },
    NewBlob, NewBlobSource,
    state::state_token,
    support::{parse_u64, store_error, validate_blob_hash},
};

const BLOB_CHUNK_BYTES: usize = 1024 * 1024;

pub(super) async fn write_generation(
    transaction: &mut Transaction<'_, Any>,
    replica: &ActiveReplica,
    blobs: &[NewBlob],
    operations: &[OperationRecord],
) -> Result<(), ReplicaError> {
    let generation = replica.generation_id.to_string();
    sqlx::query(
        "INSERT INTO replica_generations (
            generation_id, replica_id, loro_peer_id, root_catalog_node_id,
            catalog_loro, lamport_clock, projection_generation
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (generation_id) DO UPDATE SET
            replica_id = excluded.replica_id,
            loro_peer_id = excluded.loro_peer_id,
            root_catalog_node_id = excluded.root_catalog_node_id,
            catalog_loro = excluded.catalog_loro,
            lamport_clock = excluded.lamport_clock,
            projection_generation = excluded.projection_generation",
    )
    .bind(&generation)
    .bind(replica.replica_id.to_string())
    .bind(replica.loro_peer_id.to_string())
    .bind(replica.root_catalog_node_id.to_string())
    .bind(&replica.catalog_loro)
    .bind(replica.lamport_clock.to_string())
    .bind(replica.projection_generation.to_string())
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;
    sqlx::query(
        "INSERT INTO generation_state_tokens (generation_id, state_token)
         VALUES ($1, $2)
         ON CONFLICT (generation_id) DO UPDATE SET state_token = excluded.state_token",
    )
    .bind(&generation)
    .bind(state_token(replica).as_slice())
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;

    for statement in [
        "DELETE FROM catalog_entries WHERE generation_id = $1",
        "DELETE FROM document_objects WHERE generation_id = $1",
        "DELETE FROM binary_versions WHERE generation_id = $1",
    ] {
        sqlx::query(statement)
            .bind(&generation)
            .execute(&mut **transaction)
            .await
            .map_err(store_error)?;
    }

    for entry in replica.entries.values() {
        let (kind, document_id, binary_id, media_type, encoding, has_bom, size_bytes) =
            match &entry.data {
                EntryData::Directory => ("directory", None, None, None, None, None, None),
                EntryData::Document(document) => (
                    "document",
                    Some(document.document_id.to_string()),
                    None,
                    Some(document.media_type.clone()),
                    Some(document.encoding.clone()),
                    Some(i64::from(document.has_byte_order_mark)),
                    Some(document.size_bytes.to_string()),
                ),
                EntryData::Binary(binary) => {
                    let size = binary
                        .winning_version()
                        .map(|(_, version)| version.size_bytes.to_string());
                    (
                        "binary",
                        None,
                        Some(binary.binary_id.to_string()),
                        Some(binary.media_type.clone()),
                        None,
                        None,
                        size,
                    )
                }
            };
        sqlx::query(
            "INSERT INTO catalog_entries (
                generation_id, catalog_node_id, parent_catalog_node_id,
                loro_tree_id, name, kind, deleted, catalog_revision,
                document_id, binary_id, media_type, encoding, has_bom,
                size_bytes
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14
             )",
        )
        .bind(&generation)
        .bind(entry.catalog_node_id.to_string())
        .bind(entry.parent_catalog_node_id.to_string())
        .bind(&entry.loro_tree_id)
        .bind(&entry.name)
        .bind(kind)
        .bind(i64::from(entry.deleted))
        .bind(entry.catalog_revision.to_vec())
        .bind(document_id)
        .bind(binary_id)
        .bind(media_type)
        .bind(encoding)
        .bind(has_bom)
        .bind(size_bytes)
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;

        if let EntryData::Binary(binary) = &entry.data {
            for (stamp, version) in &binary.versions {
                sqlx::query(
                    "INSERT INTO binary_versions (
                        generation_id, binary_id, lamport_clock,
                        writer_node_id, sha256, size_bytes, media_type
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(&generation)
                .bind(binary.binary_id.to_string())
                .bind(stamp.lamport_clock.to_string())
                .bind(stamp.writer_node_id.to_string())
                .bind(&version.sha256)
                .bind(version.size_bytes.to_string())
                .bind(&version.media_type)
                .execute(&mut **transaction)
                .await
                .map_err(store_error)?;
            }
        }
    }

    for document in replica.documents.values() {
        sqlx::query(
            "INSERT INTO document_objects (
                generation_id, document_id, loro, revision
             ) VALUES ($1, $2, $3, $4)",
        )
        .bind(&generation)
        .bind(document.document_id.to_string())
        .bind(&document.loro)
        .bind(document.revision.to_vec())
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    }

    for blob in blobs {
        validate_blob_hash(&blob.sha256)?;
        let size = blob.size_bytes()?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT size_bytes FROM blobs WHERE sha256 = $1")
                .bind(&blob.sha256)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(store_error)?;
        if let Some(existing) = existing {
            if parse_u64(&existing, "blob size")? != size {
                return Err(ReplicaError::CorruptStore(
                    "content-addressed blob hash has contradictory sizes".to_owned(),
                ));
            }
            continue;
        }
        sqlx::query("INSERT INTO blobs (sha256, size_bytes) VALUES ($1, $2)")
            .bind(&blob.sha256)
            .bind(size.to_string())
            .execute(&mut **transaction)
            .await
            .map_err(store_error)?;
        match &blob.source {
            NewBlobSource::Bytes(bytes) => {
                if lower_hex(&Sha256::digest(bytes)) != blob.sha256 {
                    return Err(ReplicaError::CorruptStore(
                        "new blob bytes differ from their content address".to_owned(),
                    ));
                }
                for (index, chunk) in bytes.chunks(BLOB_CHUNK_BYTES).enumerate() {
                    insert_blob_chunk(transaction, &blob.sha256, index, chunk).await?;
                }
            }
            NewBlobSource::File { path, size_bytes } => {
                use tokio::io::AsyncReadExt;

                let mut file = tokio::fs::File::open(path)
                    .await
                    .map_err(|error| ReplicaError::io("open staged blob", error))?;
                let mut buffer = vec![0_u8; BLOB_CHUNK_BYTES];
                let mut read_total = 0_u64;
                let mut index = 0_usize;
                let mut hash = Sha256::new();
                loop {
                    let count = file
                        .read(&mut buffer)
                        .await
                        .map_err(|error| ReplicaError::io("read staged blob", error))?;
                    if count == 0 {
                        break;
                    }
                    insert_blob_chunk(transaction, &blob.sha256, index, &buffer[..count]).await?;
                    hash.update(&buffer[..count]);
                    read_total = read_total
                        .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
                        .ok_or_else(|| {
                            ReplicaError::InvalidSnapshot("blob size overflow".to_owned())
                        })?;
                    index += 1;
                }
                if read_total != *size_bytes {
                    return Err(ReplicaError::InvalidSnapshot(
                        "staged blob size changed during import".to_owned(),
                    ));
                }
                if lower_hex(&hash.finalize()) != blob.sha256 {
                    return Err(ReplicaError::InvalidSnapshot(
                        "staged blob bytes differ from their content address".to_owned(),
                    ));
                }
            }
        }
    }

    for operation in operations {
        sqlx::query(
            "INSERT INTO replica_operations (
                generation_id, timestamp, operation_id, source, kind,
                catalog_node_id, document_id, path_before, path_after,
                correlation_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (generation_id, operation_id, document_id) DO NOTHING",
        )
        .bind(&generation)
        .bind(
            operation
                .timestamp
                .format(&Rfc3339)
                .map_err(|_| ReplicaError::Internal("cannot format operation time".to_owned()))?,
        )
        .bind(&operation.operation_id)
        .bind(operation.source.as_str())
        .bind(operation.kind.as_str())
        .bind(operation.catalog_node_id.to_string())
        .bind(operation.document_id.to_string())
        .bind(&operation.path_before)
        .bind(&operation.path_after)
        .bind(&operation.correlation_id)
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

async fn insert_blob_chunk(
    transaction: &mut Transaction<'_, Any>,
    sha256: &str,
    index: usize,
    data: &[u8],
) -> Result<(), ReplicaError> {
    sqlx::query(
        "INSERT INTO blob_chunks (sha256, chunk_index, data)
         VALUES ($1, $2, $3)",
    )
    .bind(sha256)
    .bind(i64::try_from(index).unwrap_or(i64::MAX))
    .bind(data)
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;
    Ok(())
}

pub(super) async fn require_active_generation(
    transaction: &mut Transaction<'_, Any>,
    generation_id: Uuid,
) -> Result<(), ReplicaError> {
    let active: Option<String> =
        sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await
            .map_err(store_error)?;
    let expected = generation_id.to_string();
    if active.as_deref() != Some(expected.as_str()) {
        return Err(ReplicaError::RevisionConflict(
            "active replica generation changed".to_owned(),
        ));
    }
    Ok(())
}

pub(super) async fn delete_generation_rows(
    transaction: &mut Transaction<'_, Any>,
    generation_id: Uuid,
) -> Result<(), ReplicaError> {
    let generation = generation_id.to_string();
    sqlx::query("DELETE FROM generation_state_tokens WHERE generation_id = $1")
        .bind(&generation)
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    for statement in [
        "DELETE FROM replica_operations WHERE generation_id = $1",
        "DELETE FROM retained_commits WHERE generation_id = $1",
        "DELETE FROM projection_paths WHERE generation_id = $1",
        "DELETE FROM binary_versions WHERE generation_id = $1",
        "DELETE FROM document_objects WHERE generation_id = $1",
        "DELETE FROM catalog_entries WHERE generation_id = $1",
        "DELETE FROM replica_generations WHERE generation_id = $1",
    ] {
        sqlx::query(statement)
            .bind(&generation)
            .execute(&mut **transaction)
            .await
            .map_err(store_error)?;
    }
    Ok(())
}
