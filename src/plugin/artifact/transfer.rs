use std::{
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tokio::{fs::File, io::AsyncWriteExt};
use uuid::Uuid;

use crate::{
    plugin::{
        ArtifactPublishIntent, JobState, PluginArtifactId, PluginError, PluginId, PluginInstanceId,
        PluginJobId, PluginStore,
        protocol::{decode_plugin_artifact_id, decode_plugin_job_id},
    },
    protocol::oll,
};

use super::filesystem::{unchanged_cached_directory, validate_file_name};

pub(super) struct PendingTransfer {
    artifact_id: PluginArtifactId,
    job_id: PluginJobId,
    plugin_id: PluginId,
    file_name: String,
    media_type: String,
    size_bytes: u64,
    sha256: [u8; 32],
    chunk_count: u32,
    next_chunk: u32,
    bytes_written: u64,
    hasher: Sha256,
    download_root: PathBuf,
    staging_path: PathBuf,
    staging_owned: bool,
    file: Option<File>,
    correlation_id: String,
}

impl PendingTransfer {
    pub(super) async fn start(
        request: &oll::ArtifactTransferStart,
        session_plugin_id: &PluginId,
        session_instance_id: PluginInstanceId,
        store: &PluginStore,
        download_dir: &Path,
        maximum_chunk_bytes: usize,
        correlation_id: &str,
    ) -> Result<Self, PluginError> {
        let descriptor = request.artifact.as_ref().ok_or_else(|| {
            PluginError::InvalidArgument("artifact descriptor is required".to_owned())
        })?;
        let artifact_id =
            decode_plugin_artifact_id(descriptor.artifact_id.as_ref(), "artifact.artifact_id")?;
        let job_id = decode_plugin_job_id(request.job_id.as_ref(), "job_id")?;
        validate_file_name(&descriptor.file_name)?;
        if descriptor.media_type.is_empty() {
            return Err(PluginError::InvalidArgument(
                "artifact media type must not be empty".to_owned(),
            ));
        }
        let sha256: [u8; 32] = descriptor.sha256.as_slice().try_into().map_err(|_| {
            PluginError::InvalidArgument("artifact SHA-256 must be exactly 32 bytes".to_owned())
        })?;
        validate_chunk_plan(
            descriptor.size_bytes,
            request.chunk_count,
            maximum_chunk_bytes,
        )?;
        if !unchanged_cached_directory(download_dir)? {
            return Err(PluginError::FailedPrecondition(
                "artifact download directory changed after startup".to_owned(),
            ));
        }

        let job = store.get_job(job_id).await?;
        if &job.payload.plugin_id != session_plugin_id
            || job.plugin_instance_id != session_instance_id
        {
            return Err(PluginError::FailedPrecondition(
                "artifact transfer belongs to another plugin session".to_owned(),
            ));
        }
        if job.state != JobState::Running {
            return Err(PluginError::FailedPrecondition(
                "artifact transfer requires a running job".to_owned(),
            ));
        }
        if job.correlation_id != correlation_id {
            return Err(PluginError::FailedPrecondition(
                "artifact transfer correlation context differs from its job".to_owned(),
            ));
        }
        if store.artifact_publish_intent(artifact_id).await?.is_some() {
            return Err(PluginError::AlreadyExists(format!(
                "plugin artifact {artifact_id} already exists"
            )));
        }
        match store.get_artifact(artifact_id).await {
            Ok(_) => {
                return Err(PluginError::AlreadyExists(format!(
                    "plugin artifact {artifact_id} already exists"
                )));
            }
            Err(PluginError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }

        let (staging_path, file) = create_staging_file(download_dir, artifact_id)?;
        Ok(Self {
            artifact_id,
            job_id,
            plugin_id: session_plugin_id.clone(),
            file_name: descriptor.file_name.clone(),
            media_type: descriptor.media_type.clone(),
            size_bytes: descriptor.size_bytes,
            sha256,
            chunk_count: request.chunk_count,
            next_chunk: 0,
            bytes_written: 0,
            hasher: Sha256::new(),
            download_root: download_dir.to_owned(),
            staging_path,
            staging_owned: true,
            file: Some(file),
            correlation_id: job.correlation_id,
        })
    }

    pub(super) fn artifact_id(&self) -> PluginArtifactId {
        self.artifact_id
    }

    pub(super) fn job_id(&self) -> PluginJobId {
        self.job_id
    }

    pub(super) fn file_name(&self) -> &str {
        &self.file_name
    }

    pub(super) fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(super) fn chunk_count(&self) -> u32 {
        self.chunk_count
    }

    pub(super) fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    pub(super) fn validate_correlation(&self, correlation_id: &str) -> Result<(), PluginError> {
        if self.correlation_id == correlation_id {
            Ok(())
        } else {
            Err(PluginError::FailedPrecondition(
                "artifact transfer correlation context differs from its job".to_owned(),
            ))
        }
    }

    pub(super) fn validate_chunk(
        &self,
        index: u32,
        data: &[u8],
        maximum_chunk_bytes: usize,
    ) -> Result<(), PluginError> {
        if index != self.next_chunk {
            return Err(PluginError::InvalidArgument(
                "artifact chunks must have contiguous zero-based indexes".to_owned(),
            ));
        }
        if data.is_empty() || data.len() > maximum_chunk_bytes {
            return Err(PluginError::InvalidArgument(
                "artifact chunk is empty or exceeds the advertised limit".to_owned(),
            ));
        }
        let next_bytes = self
            .bytes_written
            .checked_add(u64::try_from(data.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                PluginError::InvalidArgument("artifact byte count overflowed".to_owned())
            })?;
        if next_bytes > self.size_bytes {
            return Err(PluginError::InvalidArgument(
                "artifact chunks exceed the declared size".to_owned(),
            ));
        }
        let consumed_chunks = self.next_chunk.checked_add(1).ok_or_else(|| {
            PluginError::InvalidArgument("artifact chunk count overflowed".to_owned())
        })?;
        let remaining_chunks = self
            .chunk_count
            .checked_sub(consumed_chunks)
            .ok_or_else(|| {
                PluginError::InvalidArgument("artifact has too many chunks".to_owned())
            })?;
        let remaining_bytes = self.size_bytes - next_bytes;
        if remaining_bytes < u64::from(remaining_chunks)
            || u128::from(remaining_bytes)
                > u128::from(remaining_chunks) * maximum_chunk_bytes as u128
        {
            return Err(PluginError::InvalidArgument(
                "artifact chunk sizes cannot satisfy the declared size and count".to_owned(),
            ));
        }
        Ok(())
    }

    pub(super) async fn write_chunk(&mut self, data: &[u8]) -> Result<(), PluginError> {
        self.file
            .as_mut()
            .expect("active artifact staging file")
            .write_all(data)
            .await
            .map_err(|error| PluginError::io("write artifact staging file", error))?;
        self.hasher.update(data);
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(data.len()).unwrap_or(u64::MAX))
            .expect("validated artifact byte count");
        self.next_chunk = self
            .next_chunk
            .checked_add(1)
            .expect("validated artifact chunk count");
        Ok(())
    }

    pub(super) async fn finish_staging(&mut self) -> Result<(), PluginError> {
        if self.next_chunk != self.chunk_count || self.bytes_written != self.size_bytes {
            return Err(PluginError::InvalidArgument(
                "artifact transfer ended before its declared chunks and bytes arrived".to_owned(),
            ));
        }
        let actual: [u8; 32] = self.hasher.clone().finalize().into();
        if actual != self.sha256 {
            return Err(PluginError::InvalidArgument(
                "artifact SHA-256 does not match its declaration".to_owned(),
            ));
        }
        let mut file = self.file.take().expect("active artifact staging file");
        file.flush()
            .await
            .map_err(|error| PluginError::io("flush artifact staging file", error))?;
        file.sync_all()
            .await
            .map_err(|error| PluginError::io("sync artifact staging file", error))?;
        drop(file);
        Ok(())
    }

    pub(super) fn publish_intent(&self, destination: PathBuf) -> ArtifactPublishIntent {
        ArtifactPublishIntent {
            artifact_id: self.artifact_id,
            job_id: self.job_id,
            plugin_id: self.plugin_id.clone(),
            file_name: self.file_name.clone(),
            media_type: self.media_type.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256,
            staging_path: self.staging_path.clone(),
            destination,
            correlation_id: self.correlation_id.clone(),
        }
    }

    pub(super) fn download_root_is_unchanged(&self) -> Result<bool, PluginError> {
        unchanged_cached_directory(&self.download_root)
    }

    pub(super) fn relinquish_staging(&mut self) {
        self.staging_owned = false;
    }

    pub(super) fn remove_staging(&self) -> Result<(), PluginError> {
        if self.download_root_is_unchanged()? {
            super::filesystem::remove_staging_if_present(&self.staging_path)
        } else {
            Ok(())
        }
    }
}

impl Drop for PendingTransfer {
    fn drop(&mut self) {
        if self.staging_owned && self.download_root_is_unchanged().unwrap_or(false) {
            let _ = super::filesystem::remove_staging_if_present(&self.staging_path);
        }
    }
}

fn validate_chunk_plan(
    size_bytes: u64,
    chunk_count: u32,
    maximum_chunk_bytes: usize,
) -> Result<(), PluginError> {
    if size_bytes == 0 {
        if chunk_count != 0 {
            return Err(PluginError::InvalidArgument(
                "an empty artifact must declare zero chunks".to_owned(),
            ));
        }
        return Ok(());
    }
    if chunk_count == 0
        || u64::from(chunk_count) > size_bytes
        || u128::from(size_bytes) > u128::from(chunk_count) * maximum_chunk_bytes as u128
    {
        return Err(PluginError::InvalidArgument(
            "artifact chunk count cannot represent the declared size".to_owned(),
        ));
    }
    Ok(())
}

fn create_staging_file(
    download_dir: &Path,
    artifact_id: PluginArtifactId,
) -> Result<(PathBuf, File), PluginError> {
    for _ in 0..8 {
        let path = download_dir.join(format!(
            ".oll-artifact-{artifact_id}-{}.part",
            Uuid::new_v4()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => return Ok((path, File::from_std(file))),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(PluginError::io("create artifact staging file", error)),
        }
    }
    Err(PluginError::AlreadyExists(
        "cannot allocate a private artifact staging file".to_owned(),
    ))
}
