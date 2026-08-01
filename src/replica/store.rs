use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    str::FromStr,
};

use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::{
    Any, AnyPool, Row, Transaction,
    any::{AnyConnectOptions, AnyPoolOptions},
};
use time::format_description::well_known::Rfc3339;
use url::Url;
use uuid::Uuid;

use crate::configuration::ReplicaStoreConfig;

use super::{
    ReplicaError,
    model::validate_loaded_replica,
    types::{
        ActiveReplica, BinaryEntry, BinaryStamp, BinaryVersion, CatalogEntry, DocumentEntry,
        DocumentObject, EntryData, OperationKind, OperationRecord, OperationSource, parse_uuid_v4,
    },
};

const BLOB_CHUNK_BYTES: usize = 1024 * 1024;

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS oll_meta (
        singleton BIGINT PRIMARY KEY,
        active_generation TEXT,
        projection_pending BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS replica_generations (
        generation_id TEXT PRIMARY KEY,
        replica_id TEXT NOT NULL,
        loro_peer_id TEXT NOT NULL,
        root_catalog_node_id TEXT NOT NULL,
        catalog_loro BYTEA NOT NULL,
        lamport_clock TEXT NOT NULL,
        projection_generation TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS catalog_entries (
        generation_id TEXT NOT NULL,
        catalog_node_id TEXT NOT NULL,
        parent_catalog_node_id TEXT NOT NULL,
        loro_tree_id TEXT NOT NULL,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,
        deleted BIGINT NOT NULL,
        catalog_revision BYTEA NOT NULL,
        document_id TEXT,
        binary_id TEXT,
        media_type TEXT,
        encoding TEXT,
        has_bom BIGINT,
        size_bytes TEXT,
        PRIMARY KEY (generation_id, catalog_node_id)
    )",
    "CREATE TABLE IF NOT EXISTS document_objects (
        generation_id TEXT NOT NULL,
        document_id TEXT NOT NULL,
        loro BYTEA NOT NULL,
        revision BYTEA NOT NULL,
        PRIMARY KEY (generation_id, document_id)
    )",
    "CREATE TABLE IF NOT EXISTS binary_versions (
        generation_id TEXT NOT NULL,
        binary_id TEXT NOT NULL,
        lamport_clock TEXT NOT NULL,
        writer_node_id TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        size_bytes TEXT NOT NULL,
        media_type TEXT NOT NULL,
        PRIMARY KEY (
            generation_id,
            binary_id,
            lamport_clock,
            writer_node_id
        )
    )",
    "CREATE TABLE IF NOT EXISTS blobs (
        sha256 TEXT PRIMARY KEY,
        size_bytes TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS blob_chunks (
        sha256 TEXT NOT NULL,
        chunk_index BIGINT NOT NULL,
        data BYTEA NOT NULL,
        PRIMARY KEY (sha256, chunk_index)
    )",
    "CREATE TABLE IF NOT EXISTS replica_operations (
        generation_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        source TEXT NOT NULL,
        kind TEXT NOT NULL,
        catalog_node_id TEXT NOT NULL,
        document_id TEXT NOT NULL,
        path_before TEXT,
        path_after TEXT,
        correlation_id TEXT NOT NULL,
        PRIMARY KEY (generation_id, operation_id, document_id)
    )",
    "CREATE INDEX IF NOT EXISTS replica_operations_document_time
        ON replica_operations (generation_id, document_id, timestamp)",
    "CREATE TABLE IF NOT EXISTS retained_commits (
        generation_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        request BYTEA NOT NULL,
        response BYTEA NOT NULL,
        PRIMARY KEY (generation_id, operation_id)
    )",
    "CREATE TABLE IF NOT EXISTS projection_paths (
        generation_id TEXT NOT NULL,
        path TEXT NOT NULL,
        PRIMARY KEY (generation_id, path)
    )",
];

#[derive(Debug)]
pub struct NewBlob {
    pub sha256: String,
    pub source: NewBlobSource,
}

#[derive(Debug)]
pub enum NewBlobSource {
    Bytes(Vec<u8>),
    File { path: PathBuf, size_bytes: u64 },
}

impl NewBlob {
    fn size_bytes(&self) -> Result<u64, ReplicaError> {
        match &self.source {
            NewBlobSource::Bytes(bytes) => u64::try_from(bytes.len())
                .map_err(|_| ReplicaError::InvalidArgument("blob is too large".to_owned())),
            NewBlobSource::File { size_bytes, .. } => Ok(*size_bytes),
        }
    }
}

#[derive(Debug)]
pub struct RetainedCommit {
    pub operation_id: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
}

#[derive(Debug)]
pub struct ReplicaStore {
    pool: AnyPool,
}

impl ReplicaStore {
    pub async fn open(config: &ReplicaStoreConfig) -> Result<Self, ReplicaError> {
        sqlx::any::install_default_drivers();
        let (database_url, sqlite_path) = match config {
            ReplicaStoreConfig::Sqlite { path } => {
                prepare_sqlite_path(path)?;
                (sqlite_url(path)?, Some(path.clone()))
            }
            ReplicaStoreConfig::Postgres { url } => (url.expose().to_owned(), None),
        };
        let options = AnyConnectOptions::from_str(&database_url)
            .map_err(|_| ReplicaError::Store("invalid replica-store connection".to_owned()))?;
        let pool = AnyPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|_| {
                ReplicaError::Store("cannot connect to the configured replica store".to_owned())
            })?;
        for statement in SCHEMA {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .map_err(store_error)?;
        }
        sqlx::query(
            "INSERT INTO oll_meta (singleton, active_generation, projection_pending)
             VALUES (1, NULL, 0)
             ON CONFLICT (singleton) DO NOTHING",
        )
        .execute(&pool)
        .await
        .map_err(store_error)?;
        if let Some(path) = &sqlite_path {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .map_err(|error| ReplicaError::io("set SQLite store permissions", error))?;
        }
        Ok(Self { pool })
    }

    pub async fn load_active(&self) -> Result<Option<ActiveReplica>, ReplicaError> {
        let row = sqlx::query("SELECT active_generation FROM oll_meta WHERE singleton = 1")
            .fetch_one(&self.pool)
            .await
            .map_err(store_error)?;
        let Some(generation_id) = row
            .try_get::<Option<String>, _>("active_generation")
            .map_err(store_error)?
        else {
            return Ok(None);
        };
        self.load_generation(&generation_id).await.map(Some)
    }

    pub async fn projection_pending(&self) -> Result<bool, ReplicaError> {
        let value: i64 =
            sqlx::query_scalar("SELECT projection_pending FROM oll_meta WHERE singleton = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(store_error)?;
        match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReplicaError::CorruptStore(
                "projection_pending is not boolean".to_owned(),
            )),
        }
    }

    pub async fn initialize(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if active.is_some() {
            return Err(ReplicaError::RevisionConflict(
                "replica was initialized concurrently".to_owned(),
            ));
        }
        write_generation(&mut transaction, replica, blobs, operations).await?;
        write_projection_paths(&mut transaction, replica.generation_id, projection_paths).await?;
        sqlx::query(
            "UPDATE oll_meta
             SET active_generation = $1, projection_pending = 0
             WHERE singleton = 1 AND active_generation IS NULL",
        )
        .bind(replica.generation_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn save_active(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
    ) -> Result<(), ReplicaError> {
        self.save_active_inner(replica, blobs, operations, projection_paths, None)
            .await
    }

    pub async fn save_active_commit(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
        commit: &RetainedCommit,
    ) -> Result<(), ReplicaError> {
        self.save_active_inner(replica, blobs, operations, projection_paths, Some(commit))
            .await
    }

    async fn save_active_inner(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
        commit: Option<&RetainedCommit>,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, replica.generation_id).await?;
        write_generation(&mut transaction, replica, blobs, operations).await?;
        write_projection_paths(&mut transaction, replica.generation_id, projection_paths).await?;
        if let Some(commit) = commit {
            sqlx::query(
                "INSERT INTO retained_commits (
                    generation_id, operation_id, request, response
                 ) VALUES ($1, $2, $3, $4)",
            )
            .bind(replica.generation_id.to_string())
            .bind(&commit.operation_id)
            .bind(&commit.request)
            .bind(&commit.response)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        }
        transaction.commit().await.map_err(store_error)
    }

    pub async fn retained_commit(
        &self,
        generation_id: Uuid,
        operation_id: &str,
    ) -> Result<Option<RetainedCommit>, ReplicaError> {
        let row = sqlx::query(
            "SELECT request, response
             FROM retained_commits
             WHERE generation_id = $1 AND operation_id = $2",
        )
        .bind(generation_id.to_string())
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| {
            Ok(RetainedCommit {
                operation_id: operation_id.to_owned(),
                request: row.try_get::<Vec<u8>, _>("request").map_err(store_error)?,
                response: row.try_get::<Vec<u8>, _>("response").map_err(store_error)?,
            })
        })
        .transpose()
    }

    pub async fn build_inactive_generation(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        write_generation(&mut transaction, replica, blobs, operations).await?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn activate_generation(
        &self,
        expected_active: Option<Uuid>,
        generation_id: Uuid,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        let expected = expected_active.map(|value| value.to_string());
        if active != expected {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed while snapshot import was staged".to_owned(),
            ));
        }
        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM replica_generations WHERE generation_id = $1")
                .bind(generation_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if exists != 1 {
            return Err(ReplicaError::CorruptStore(
                "snapshot candidate generation is missing".to_owned(),
            ));
        }
        sqlx::query(
            "UPDATE oll_meta
             SET active_generation = $1, projection_pending = 1
             WHERE singleton = 1",
        )
        .bind(generation_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn clear_projection_pending(&self, generation_id: Uuid) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, generation_id).await?;
        sqlx::query(
            "UPDATE oll_meta SET projection_pending = 0
             WHERE singleton = 1",
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        sqlx::query("DELETE FROM projection_paths WHERE generation_id = $1")
            .bind(generation_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn projection_paths(&self, generation_id: Uuid) -> Result<Vec<String>, ReplicaError> {
        sqlx::query_scalar(
            "SELECT path FROM projection_paths
             WHERE generation_id = $1 ORDER BY path",
        )
        .bind(generation_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)
    }

    pub async fn clear_projection_path(
        &self,
        generation_id: Uuid,
        path: &str,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, generation_id).await?;
        sqlx::query(
            "DELETE FROM projection_paths
             WHERE generation_id = $1 AND path = $2",
        )
        .bind(generation_id.to_string())
        .bind(path)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn load_generation(&self, generation: &str) -> Result<ActiveReplica, ReplicaError> {
        let row = sqlx::query(
            "SELECT generation_id, replica_id, loro_peer_id,
                    root_catalog_node_id, catalog_loro, lamport_clock,
                    projection_generation
             FROM replica_generations
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ReplicaError::CorruptStore("active replica generation is missing".to_owned())
        })?;
        let generation_id = parse_uuid_v4(
            &row.try_get::<String, _>("generation_id")
                .map_err(store_error)?,
            "generation_id",
        )?;
        let replica_id = parse_uuid_v4(
            &row.try_get::<String, _>("replica_id")
                .map_err(store_error)?,
            "replica_id",
        )?;
        let root_catalog_node_id = parse_uuid_v4(
            &row.try_get::<String, _>("root_catalog_node_id")
                .map_err(store_error)?,
            "root_catalog_node_id",
        )?;
        let loro_peer_id = parse_u64(
            &row.try_get::<String, _>("loro_peer_id")
                .map_err(store_error)?,
            "loro_peer_id",
        )?;
        if loro_peer_id == u64::MAX {
            return Err(ReplicaError::CorruptStore(
                "loro_peer_id uses Loro's reserved root identity".to_owned(),
            ));
        }
        let lamport_clock = parse_u64(
            &row.try_get::<String, _>("lamport_clock")
                .map_err(store_error)?,
            "lamport_clock",
        )?;
        let projection_generation = parse_u64(
            &row.try_get::<String, _>("projection_generation")
                .map_err(store_error)?,
            "projection_generation",
        )?;
        let catalog_loro = row
            .try_get::<Vec<u8>, _>("catalog_loro")
            .map_err(store_error)?;

        let mut entries = BTreeMap::new();
        let rows = sqlx::query(
            "SELECT catalog_node_id, parent_catalog_node_id, loro_tree_id,
                    name, kind, deleted, catalog_revision, document_id,
                    binary_id, media_type, encoding, has_bom, size_bytes
             FROM catalog_entries
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        for row in rows {
            let catalog_node_id = parse_uuid_v4(
                &row.try_get::<String, _>("catalog_node_id")
                    .map_err(store_error)?,
                "catalog_node_id",
            )?;
            let parent_catalog_node_id = parse_uuid_v4(
                &row.try_get::<String, _>("parent_catalog_node_id")
                    .map_err(store_error)?,
                "parent_catalog_node_id",
            )?;
            let kind = row.try_get::<String, _>("kind").map_err(store_error)?;
            let media_type = row
                .try_get::<Option<String>, _>("media_type")
                .map_err(store_error)?;
            let document_id = row
                .try_get::<Option<String>, _>("document_id")
                .map_err(store_error)?;
            let binary_id = row
                .try_get::<Option<String>, _>("binary_id")
                .map_err(store_error)?;
            let encoding = row
                .try_get::<Option<String>, _>("encoding")
                .map_err(store_error)?;
            let has_bom = row
                .try_get::<Option<i64>, _>("has_bom")
                .map_err(store_error)?;
            let size_bytes = row
                .try_get::<Option<String>, _>("size_bytes")
                .map_err(store_error)?;
            let data = match kind.as_str() {
                "directory" => {
                    if media_type.is_some()
                        || document_id.is_some()
                        || binary_id.is_some()
                        || encoding.is_some()
                        || has_bom.is_some()
                        || size_bytes.is_some()
                    {
                        return Err(kind_fields_error());
                    }
                    EntryData::Directory
                }
                "document" => EntryData::Document(DocumentEntry {
                    document_id: if binary_id.is_none() {
                        parse_optional_uuid(document_id, "document_id")?
                            .ok_or_else(kind_fields_error)?
                    } else {
                        return Err(kind_fields_error());
                    },
                    media_type: media_type.ok_or_else(kind_fields_error)?,
                    encoding: encoding.ok_or_else(kind_fields_error)?,
                    has_byte_order_mark: parse_bool(
                        has_bom.ok_or_else(kind_fields_error)?,
                        "has_bom",
                    )?,
                    size_bytes: parse_u64(
                        &size_bytes.ok_or_else(kind_fields_error)?,
                        "size_bytes",
                    )?,
                }),
                "binary" => {
                    if document_id.is_some()
                        || encoding.is_some()
                        || has_bom.is_some()
                        || size_bytes.is_none()
                    {
                        return Err(kind_fields_error());
                    }
                    EntryData::Binary(BinaryEntry {
                        binary_id: parse_optional_uuid(binary_id, "binary_id")?
                            .ok_or_else(kind_fields_error)?,
                        media_type: media_type.ok_or_else(kind_fields_error)?,
                        versions: BTreeMap::new(),
                    })
                }
                _ => {
                    return Err(ReplicaError::CorruptStore(
                        "catalog entry has an unknown kind".to_owned(),
                    ));
                }
            };
            let entry = CatalogEntry {
                catalog_node_id,
                parent_catalog_node_id,
                loro_tree_id: row
                    .try_get::<String, _>("loro_tree_id")
                    .map_err(store_error)?,
                name: row.try_get::<String, _>("name").map_err(store_error)?,
                deleted: parse_bool(
                    row.try_get::<i64, _>("deleted").map_err(store_error)?,
                    "deleted",
                )?,
                catalog_revision: revision_array(
                    row.try_get::<Vec<u8>, _>("catalog_revision")
                        .map_err(store_error)?,
                    "catalog_revision",
                )?,
                data,
            };
            if entries.insert(catalog_node_id, entry).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate catalog_node_id".to_owned(),
                ));
            }
        }

        let rows = sqlx::query(
            "SELECT binary_id, lamport_clock, writer_node_id, sha256,
                    size_bytes, media_type
             FROM binary_versions
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        for row in rows {
            let binary_id = parse_uuid_v4(
                &row.try_get::<String, _>("binary_id").map_err(store_error)?,
                "binary_id",
            )?;
            let stamp = BinaryStamp {
                lamport_clock: parse_u64(
                    &row.try_get::<String, _>("lamport_clock")
                        .map_err(store_error)?,
                    "lamport_clock",
                )?,
                writer_node_id: parse_uuid_v4(
                    &row.try_get::<String, _>("writer_node_id")
                        .map_err(store_error)?,
                    "writer_node_id",
                )?,
            };
            let version = BinaryVersion {
                sha256: row.try_get::<String, _>("sha256").map_err(store_error)?,
                size_bytes: parse_u64(
                    &row.try_get::<String, _>("size_bytes")
                        .map_err(store_error)?,
                    "size_bytes",
                )?,
                media_type: row
                    .try_get::<String, _>("media_type")
                    .map_err(store_error)?,
            };
            let entry = entries
                .values_mut()
                .find(|entry| {
                    entry
                        .binary()
                        .is_some_and(|binary| binary.binary_id == binary_id)
                })
                .ok_or_else(|| {
                    ReplicaError::CorruptStore(
                        "binary version has no matching catalog entry".to_owned(),
                    )
                })?;
            let EntryData::Binary(binary) = &mut entry.data else {
                unreachable!()
            };
            if binary.versions.insert(stamp, version).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate binary version stamp".to_owned(),
                ));
            }
        }

        let rows = sqlx::query(
            "SELECT document_id, loro, revision
             FROM document_objects
             WHERE generation_id = $1",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        let mut documents = BTreeMap::new();
        for row in rows {
            let document_id = parse_uuid_v4(
                &row.try_get::<String, _>("document_id")
                    .map_err(store_error)?,
                "document_id",
            )?;
            let document = DocumentObject {
                document_id,
                loro: row.try_get::<Vec<u8>, _>("loro").map_err(store_error)?,
                revision: revision_array(
                    row.try_get::<Vec<u8>, _>("revision").map_err(store_error)?,
                    "document_revision",
                )?,
            };
            if documents.insert(document_id, document).is_some() {
                return Err(ReplicaError::CorruptStore(
                    "duplicate document_id".to_owned(),
                ));
            }
        }
        for entry in entries.values() {
            if let Some(document) = entry.document()
                && !documents.contains_key(&document.document_id)
            {
                return Err(ReplicaError::CorruptStore(
                    "catalog document has no retained Loro object".to_owned(),
                ));
            }
            if let Some(binary) = entry.binary()
                && binary.versions.is_empty()
            {
                return Err(ReplicaError::CorruptStore(
                    "catalog binary has no retained version".to_owned(),
                ));
            }
        }

        let replica = ActiveReplica {
            generation_id,
            replica_id,
            loro_peer_id,
            root_catalog_node_id,
            catalog_loro,
            lamport_clock,
            projection_generation,
            entries,
            documents,
        };
        validate_loaded_replica(&replica)?;
        for binary in replica.entries.values().filter_map(CatalogEntry::binary) {
            for version in binary.versions.values() {
                validate_blob_hash(&version.sha256)?;
                if self.blob_size(&version.sha256).await? != version.size_bytes {
                    return Err(ReplicaError::CorruptStore(
                        "binary version size differs from its blob metadata".to_owned(),
                    ));
                }
            }
        }
        Ok(replica)
    }

    #[cfg(test)]
    pub async fn read_blob(&self, sha256: &str) -> Result<Vec<u8>, ReplicaError> {
        let declared = self.blob_size(sha256).await?;
        let rows = sqlx::query(
            "SELECT chunk_index, data FROM blob_chunks
             WHERE sha256 = $1 ORDER BY chunk_index",
        )
        .bind(sha256)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        let capacity = usize::try_from(declared).map_err(|_| {
            ReplicaError::Store("blob is too large for this process address space".to_owned())
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        for (expected, row) in rows.into_iter().enumerate() {
            let index = row.try_get::<i64, _>("chunk_index").map_err(store_error)?;
            if index != i64::try_from(expected).unwrap_or(i64::MAX) {
                return Err(ReplicaError::CorruptStore(
                    "blob chunks are not contiguous".to_owned(),
                ));
            }
            bytes.extend_from_slice(&row.try_get::<Vec<u8>, _>("data").map_err(store_error)?);
        }
        if u64::try_from(bytes.len()).ok() != Some(declared) {
            return Err(ReplicaError::CorruptStore(
                "blob byte count differs from metadata".to_owned(),
            ));
        }
        if super::lower_hex(&Sha256::digest(&bytes)) != sha256 {
            return Err(ReplicaError::CorruptStore(
                "blob bytes differ from their content address".to_owned(),
            ));
        }
        Ok(bytes)
    }

    pub async fn write_blob_to_path(&self, sha256: &str, path: &Path) -> Result<(), ReplicaError> {
        use tokio::io::AsyncWriteExt;

        let declared = self.blob_size(sha256).await?;
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| ReplicaError::io("create staged blob", error))?;
        let mut output = tokio::fs::File::from_std(output);
        let mut rows = sqlx::query(
            "SELECT chunk_index, data FROM blob_chunks
             WHERE sha256 = $1 ORDER BY chunk_index",
        )
        .bind(sha256)
        .fetch(&self.pool);
        let mut written = 0_u64;
        let mut hash = Sha256::new();
        let mut expected = 0_i64;
        while let Some(row) = rows.try_next().await.map_err(store_error)? {
            let index = row.try_get::<i64, _>("chunk_index").map_err(store_error)?;
            if index != expected {
                return Err(ReplicaError::CorruptStore(
                    "blob chunks are not contiguous".to_owned(),
                ));
            }
            expected = expected.checked_add(1).ok_or_else(|| {
                ReplicaError::CorruptStore("blob chunk index overflow".to_owned())
            })?;
            let data = row.try_get::<Vec<u8>, _>("data").map_err(store_error)?;
            output
                .write_all(&data)
                .await
                .map_err(|error| ReplicaError::io("write staged blob", error))?;
            hash.update(&data);
            written = written
                .checked_add(u64::try_from(data.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| ReplicaError::CorruptStore("blob size overflow".to_owned()))?;
        }
        output
            .flush()
            .await
            .map_err(|error| ReplicaError::io("flush staged blob", error))?;
        if written != declared {
            return Err(ReplicaError::CorruptStore(
                "blob byte count differs from metadata".to_owned(),
            ));
        }
        if super::lower_hex(&hash.finalize()) != sha256 {
            return Err(ReplicaError::CorruptStore(
                "blob bytes differ from their content address".to_owned(),
            ));
        }
        output
            .sync_all()
            .await
            .map_err(|error| ReplicaError::io("sync staged blob", error))?;
        Ok(())
    }

    pub async fn blob_size(&self, sha256: &str) -> Result<u64, ReplicaError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT size_bytes FROM blobs WHERE sha256 = $1")
                .bind(sha256)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        let value = value.ok_or_else(|| {
            ReplicaError::CorruptStore(format!("referenced blob {sha256} is missing"))
        })?;
        parse_u64(&value, "blob size")
    }

    pub async fn list_operations(
        &self,
        generation_id: Uuid,
        document_id: Uuid,
        limit: usize,
    ) -> Result<Vec<OperationRecord>, ReplicaError> {
        let limit = i64::try_from(limit).map_err(|_| {
            ReplicaError::InvalidArgument("operation limit is too large".to_owned())
        })?;
        let rows = sqlx::query(
            "SELECT timestamp, operation_id, source, kind, catalog_node_id,
                    document_id, path_before, path_after, correlation_id
             FROM replica_operations
             WHERE generation_id = $1 AND document_id = $2
             ORDER BY timestamp DESC, operation_id DESC
             LIMIT $3",
        )
        .bind(generation_id.to_string())
        .bind(document_id.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(decode_operation).collect()
    }
}

async fn write_generation(
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

    for table in ["catalog_entries", "document_objects", "binary_versions"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE generation_id = $1"))
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
                if super::lower_hex(&Sha256::digest(bytes)) != blob.sha256 {
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
                if super::lower_hex(&hash.finalize()) != blob.sha256 {
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

async fn require_active_generation(
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

async fn write_projection_paths(
    transaction: &mut Transaction<'_, Any>,
    generation_id: Uuid,
    paths: &[String],
) -> Result<(), ReplicaError> {
    for path in paths {
        sqlx::query(
            "INSERT INTO projection_paths (generation_id, path)
             VALUES ($1, $2)
             ON CONFLICT (generation_id, path) DO NOTHING",
        )
        .bind(generation_id.to_string())
        .bind(path)
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

fn decode_operation(row: sqlx::any::AnyRow) -> Result<OperationRecord, ReplicaError> {
    let timestamp = row.try_get::<String, _>("timestamp").map_err(store_error)?;
    Ok(OperationRecord {
        timestamp: time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|_| {
            ReplicaError::CorruptStore("operation timestamp is not RFC 3339".to_owned())
        })?,
        operation_id: row
            .try_get::<String, _>("operation_id")
            .map_err(store_error)?,
        source: OperationSource::parse(&row.try_get::<String, _>("source").map_err(store_error)?)?,
        kind: OperationKind::parse(&row.try_get::<String, _>("kind").map_err(store_error)?)?,
        catalog_node_id: parse_uuid_v4(
            &row.try_get::<String, _>("catalog_node_id")
                .map_err(store_error)?,
            "catalog_node_id",
        )?,
        document_id: parse_uuid_v4(
            &row.try_get::<String, _>("document_id")
                .map_err(store_error)?,
            "document_id",
        )?,
        path_before: row
            .try_get::<Option<String>, _>("path_before")
            .map_err(store_error)?,
        path_after: row
            .try_get::<Option<String>, _>("path_after")
            .map_err(store_error)?,
        correlation_id: row
            .try_get::<String, _>("correlation_id")
            .map_err(store_error)?,
    })
}

fn prepare_sqlite_path(path: &Path) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::InvalidArgument("SQLite replica-store path has no parent".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create SQLite store directory", error))?;
    if !path.exists() {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| ReplicaError::io("create SQLite replica store", error))?;
    }
    Ok(())
}

fn sqlite_url(path: &Path) -> Result<String, ReplicaError> {
    let file = Url::from_file_path(path).map_err(|()| {
        ReplicaError::InvalidArgument("SQLite replica-store path must be absolute".to_owned())
    })?;
    Ok(file.as_str().replacen("file:", "sqlite:", 1))
}

fn parse_optional_uuid(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<Uuid>, ReplicaError> {
    value.map(|value| parse_uuid_v4(&value, field)).transpose()
}

fn revision_array(bytes: Vec<u8>, field: &'static str) -> Result<[u8; 32], ReplicaError> {
    bytes
        .try_into()
        .map_err(|_| ReplicaError::CorruptStore(format!("{field} is not 32 bytes")))
}

fn parse_bool(value: i64, field: &'static str) -> Result<bool, ReplicaError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ReplicaError::CorruptStore(format!(
            "{field} is not boolean"
        ))),
    }
}

fn parse_u64(value: &str, field: &'static str) -> Result<u64, ReplicaError> {
    value
        .parse()
        .map_err(|_| ReplicaError::CorruptStore(format!("{field} is not a u64")))
}

fn validate_blob_hash(value: &str) -> Result<(), ReplicaError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ReplicaError::CorruptStore(
            "blob content address is not lower-case SHA-256".to_owned(),
        ))
    }
}

fn kind_fields_error() -> ReplicaError {
    ReplicaError::CorruptStore("catalog entry fields do not match its kind".to_owned())
}

fn store_error(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::Store(format!("replica-store operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::replica::model::{initialize_from_disk, scan_working_tree};

    #[tokio::test]
    #[ignore = "requires OLL_TEST_POSTGRES_URL and an externally managed PostgreSQL database"]
    async fn postgres_implements_the_logical_store_contract_when_configured() {
        let base_url = std::env::var("OLL_TEST_POSTGRES_URL")
            .expect("explicit PostgreSQL contract test requires UTF-8 OLL_TEST_POSTGRES_URL");
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
            .expect("connect to OLL_TEST_POSTGRES_URL");
        let schema = format!("oll_test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .expect("create isolated PostgreSQL test schema");

        let mut scoped = Url::parse(&base_url).expect("parse OLL_TEST_POSTGRES_URL");
        scoped
            .query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let exercise = async {
            let config = ReplicaStoreConfig::Postgres {
                url: scoped.as_str().parse().map_err(|error: String| error)?,
            };
            let directory = TempDir::new().map_err(|error| error.to_string())?;
            fs::write(directory.path().join("a.md"), "postgres")
                .map_err(|error| error.to_string())?;
            let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
            fs::write(directory.path().join("image.gif"), &binary)
                .map_err(|error| error.to_string())?;
            let disk = scan_working_tree(directory.path()).map_err(|error| error.to_string())?;
            let change = initialize_from_disk(&disk, Uuid::new_v4(), "postgres-test-correlation")
                .map_err(|error| error.to_string())?;
            let store = ReplicaStore::open(&config)
                .await
                .map_err(|error| error.to_string())?;
            store
                .initialize(
                    &change.replica,
                    &change.blobs,
                    &change.operations,
                    &["/initial-projection-marker".to_owned()],
                )
                .await
                .map_err(|error| error.to_string())?;
            let loaded = store
                .load_active()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "PostgreSQL active generation is missing".to_owned())?;
            if loaded.replica_id != change.replica.replica_id
                || loaded.documents.len() != change.replica.documents.len()
                || loaded.entries.len() != change.replica.entries.len()
            {
                return Err("PostgreSQL logical round trip changed replica state".to_owned());
            }

            let document_id = *loaded
                .documents
                .keys()
                .next()
                .ok_or_else(|| "PostgreSQL document object is missing".to_owned())?;
            let operations = store
                .list_operations(loaded.generation_id, document_id, 10)
                .await
                .map_err(|error| error.to_string())?;
            if operations.len() != 1 {
                return Err("PostgreSQL operation history did not round trip".to_owned());
            }

            let blob_hash = loaded
                .entries
                .values()
                .find_map(|entry| {
                    entry
                        .binary()
                        .and_then(|binary| binary.winning_version())
                        .map(|(_, version)| version.sha256.clone())
                })
                .ok_or_else(|| "PostgreSQL binary version is missing".to_owned())?;
            if store
                .read_blob(&blob_hash)
                .await
                .map_err(|error| error.to_string())?
                != binary
            {
                return Err("PostgreSQL blob chunks did not round trip".to_owned());
            }
            let projected_blob = directory.path().join("projected.gif");
            store
                .write_blob_to_path(&blob_hash, &projected_blob)
                .await
                .map_err(|error| error.to_string())?;
            if fs::read(projected_blob).map_err(|error| error.to_string())? != binary {
                return Err("PostgreSQL streamed blob projection changed bytes".to_owned());
            }

            let retained = RetainedCommit {
                operation_id: "postgres-retained-commit".to_owned(),
                request: vec![1, 2, 3],
                response: vec![4, 5, 6],
            };
            store
                .save_active_commit(
                    &loaded,
                    &[],
                    &[],
                    &["/saved-projection-marker".to_owned()],
                    &retained,
                )
                .await
                .map_err(|error| error.to_string())?;
            let restored = store
                .retained_commit(loaded.generation_id, &retained.operation_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "PostgreSQL retained commit is missing".to_owned())?;
            if restored.request != retained.request || restored.response != retained.response {
                return Err("PostgreSQL retained commit changed bytes".to_owned());
            }
            store
                .clear_projection_path(loaded.generation_id, "/saved-projection-marker")
                .await
                .map_err(|error| error.to_string())?;
            if store
                .projection_paths(loaded.generation_id)
                .await
                .map_err(|error| error.to_string())?
                != ["/initial-projection-marker"]
            {
                return Err(
                    "PostgreSQL path acknowledgement cleared an unrelated marker".to_owned(),
                );
            }

            let mut candidate = loaded.clone();
            candidate.generation_id = Uuid::new_v4();
            store
                .build_inactive_generation(&candidate, &[], &[])
                .await
                .map_err(|error| error.to_string())?;
            store
                .activate_generation(Some(loaded.generation_id), candidate.generation_id)
                .await
                .map_err(|error| error.to_string())?;
            if !store
                .projection_pending()
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("PostgreSQL generation switch omitted projection_pending".to_owned());
            }
            let switched = store
                .load_active()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "PostgreSQL switched generation is missing".to_owned())?;
            if switched.generation_id != candidate.generation_id {
                return Err("PostgreSQL generation switch selected the wrong state".to_owned());
            }
            store
                .clear_projection_pending(candidate.generation_id)
                .await
                .map_err(|error| error.to_string())?;
            if store
                .projection_pending()
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("PostgreSQL projection_pending did not clear".to_owned());
            }
            if !store
                .projection_paths(candidate.generation_id)
                .await
                .map_err(|error| error.to_string())?
                .is_empty()
            {
                return Err("PostgreSQL projection paths did not clear".to_owned());
            }
            drop(store);
            Ok::<(), String>(())
        }
        .await;

        let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await;
        admin.close().await;
        cleanup.expect("drop isolated PostgreSQL test schema");
        if let Err(error) = exercise {
            panic!("{error}");
        }
    }
}
