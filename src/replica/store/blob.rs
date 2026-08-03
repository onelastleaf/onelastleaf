use std::{
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::Row;

use super::{
    super::{ReplicaError, lower_hex},
    ReplicaStore,
    support::{parse_u64, store_error},
};

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
    pub(crate) fn size_bytes(&self) -> Result<u64, ReplicaError> {
        match &self.source {
            NewBlobSource::Bytes(bytes) => u64::try_from(bytes.len())
                .map_err(|_| ReplicaError::InvalidArgument("blob is too large".to_owned())),
            NewBlobSource::File { size_bytes, .. } => Ok(*size_bytes),
        }
    }
}

impl ReplicaStore {
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
        if lower_hex(&Sha256::digest(&bytes)) != sha256 {
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
        if lower_hex(&hash.finalize()) != sha256 {
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
}
