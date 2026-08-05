use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use sha2::{Digest, Sha256};
use sqlx::{AnyPool, any::AnyPoolOptions};
use tempfile::TempDir;
use time::OffsetDateTime;
use url::Url;
use uuid::Uuid;

use crate::{
    node::{NodeIdentity, logging::NodeLogger},
    plugin::{
        ArtifactPublishIntent, ArtifactPublisher, DesiredPluginState, InstallMode, JobAdmission,
        MAX_ARTIFACT_CHUNK_BYTES, NormalizedJobPayload, PackagePublishIntent, PluginArtifactId,
        PluginId, PluginInstanceId, PluginJob, PluginOperationId, PluginStore,
    },
    protocol::oll,
};

static PUBLISH_HOOK_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) struct Fixture {
    pub(super) _directory: TempDir,
    pub(super) pool: AnyPool,
    pub(super) store: PluginStore,
    pub(super) publisher: ArtifactPublisher,
    pub(super) plugin_id: PluginId,
    pub(super) instance_id: PluginInstanceId,
    pub(super) download_dir: PathBuf,
    pub(super) now: OffsetDateTime,
    pub(super) logger: Arc<NodeLogger>,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
        let store = PluginStore::initialize(pool.clone()).await.unwrap();
        let plugin_id: PluginId = "oll.artifact-tests".parse().unwrap();
        let generation = Uuid::new_v4();
        let package = PackagePublishIntent {
            plugin_id: plugin_id.clone(),
            plugin_name: "artifact-tests".parse().unwrap(),
            operation_id: "install-artifact-tests".to_owned(),
            expected_current_generation: None,
            candidate_generation: generation,
            normalized_declaration: b"artifact-test-declaration".to_vec(),
            declaration_sha256: Sha256::digest(b"artifact-test-declaration").into(),
            effective_manifest: b"artifact-test-manifest".to_vec(),
            selected_commit: Some("0123456789abcdef".to_owned()),
            install_mode: InstallMode::Source,
            release_id: None,
            correlation_id: "install-artifact-tests".to_owned(),
        };
        store.prepare_package_publish(&package).await.unwrap();
        store
            .finalize_package_publish(&plugin_id, generation)
            .await
            .unwrap();
        let instance_id = PluginInstanceId::new();
        store
            .set_desired_state(&plugin_id, DesiredPluginState::Running)
            .await
            .unwrap();
        store
            .record_running_instance(&plugin_id, generation, instance_id)
            .await
            .unwrap();
        let logger = NodeLogger::open(
            &directory.path().join("logs"),
            NodeIdentity::generate("artifact-tests".parse().unwrap()),
        )
        .unwrap();
        let download_dir = directory.path().join("downloads");
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let (publisher, report) = ArtifactPublisher::initialize(
            store.clone(),
            &download_dir,
            MAX_ARTIFACT_CHUNK_BYTES,
            Arc::clone(&logger),
            now,
            "artifact-test-startup",
        )
        .await
        .unwrap();
        assert_eq!(report.recovered, 0);
        assert_eq!(report.failed, 0);
        Self {
            _directory: directory,
            pool,
            store,
            publisher,
            plugin_id,
            instance_id,
            download_dir,
            now,
            logger,
        }
    }

    pub(super) async fn job(&self, operation: &str) -> PluginJob {
        let payload =
            NormalizedJobPayload::new(self.plugin_id.clone(), "render".to_owned(), vec![], None)
                .unwrap();
        let operation: PluginOperationId = operation.parse().unwrap();
        let JobAdmission::Created(job) = self
            .store
            .admit_job(
                &operation,
                &payload,
                self.instance_id,
                self.now,
                &format!("correlation-{operation}"),
            )
            .await
            .unwrap()
        else {
            panic!("test operation ID was unexpectedly reused")
        };
        self.store
            .mark_job_accepted(job.job_id, self.instance_id, self.now)
            .await
            .unwrap()
    }
}

async fn sqlite_pool(path: &Path) -> AnyPool {
    sqlx::any::install_default_drivers();
    fs::File::create(path).unwrap();
    let url = Url::from_file_path(path)
        .unwrap()
        .as_str()
        .replacen("file:", "sqlite:", 1);
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap()
}

pub(super) fn start(
    job: &PluginJob,
    artifact_id: PluginArtifactId,
    file_name: &str,
    bytes: &[u8],
    chunk_count: u32,
) -> oll::ArtifactTransferStart {
    oll::ArtifactTransferStart {
        job_id: Some(oll::PluginJobId {
            value: job.job_id.to_string(),
        }),
        artifact: Some(oll::ArtifactDescriptor {
            artifact_id: Some(oll::PluginArtifactId {
                value: artifact_id.to_string(),
            }),
            file_name: file_name.to_owned(),
            media_type: "application/octet-stream".to_owned(),
            size_bytes: u64::try_from(bytes.len()).unwrap(),
            sha256: Sha256::digest(bytes).to_vec(),
        }),
        chunk_count,
    }
}

pub(super) fn chunk(
    artifact_id: PluginArtifactId,
    index: u32,
    bytes: &[u8],
) -> oll::ArtifactTransferChunk {
    oll::ArtifactTransferChunk {
        artifact_id: Some(oll::PluginArtifactId {
            value: artifact_id.to_string(),
        }),
        chunk_index: index,
        data: bytes.to_vec(),
    }
}

pub(super) fn complete(artifact_id: PluginArtifactId) -> oll::ArtifactTransferComplete {
    oll::ArtifactTransferComplete {
        artifact_id: Some(oll::PluginArtifactId {
            value: artifact_id.to_string(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn intent(
    fixture: &Fixture,
    job: &PluginJob,
    artifact_id: PluginArtifactId,
    file_name: &str,
    bytes: &[u8],
    staging_path: PathBuf,
    destination: PathBuf,
) -> ArtifactPublishIntent {
    ArtifactPublishIntent {
        artifact_id,
        job_id: job.job_id,
        plugin_id: fixture.plugin_id.clone(),
        file_name: file_name.to_owned(),
        media_type: "application/octet-stream".to_owned(),
        size_bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: Sha256::digest(bytes).into(),
        staging_path,
        destination,
        correlation_id: job.correlation_id.clone(),
    }
}

pub(super) fn fixed_artifact_id(number: u8) -> PluginArtifactId {
    format!("123e4567-e89b-42d3-a456-4266141740{number:02}")
        .parse()
        .unwrap()
}

pub(super) fn staging_files(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".oll-artifact-") && name.ends_with(".part"))
        })
        .collect()
}

pub(super) async fn lock_publish_hook() -> tokio::sync::MutexGuard<'static, ()> {
    PUBLISH_HOOK_TEST_LOCK.lock().await
}
