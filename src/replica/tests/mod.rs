use std::{
    collections::HashMap,
    fs,
    os::unix::fs::{MetadataExt, symlink},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    configuration::ReplicaStoreConfig,
    node::{NodeIdentity, identity::IdentityCoordinator, logging::NodeLogger},
    plugin::{
        ArtifactPublishIntent, DesiredPluginState, InstallMode, InstalledPlugin, JobAdmission,
        JobState, NormalizedJobPayload, PackagePublishIntent, PluginArtifact, PluginArtifactId,
        PluginId, PluginInstanceId, PluginJob, PluginOperationId, PluginSelector, PluginStore,
    },
    protocol::oll,
};

use super::{
    BootstrapCandidate, BootstrapClaim, OperationSource, ReplicaError, ReplicaRuntime,
    ReplicaStatus, ReplicationCandidate, ReplicationCommit, StagedBlob, identity,
    store::{IdentityTransition, IdentityTransitionKind, NewBlob, NewBlobSource},
    types::EntryData,
};

struct Deployment {
    _directory: TempDir,
    root: PathBuf,
    config_root: PathBuf,
    store_path: PathBuf,
    log_dir: PathBuf,
    identity: NodeIdentity,
}

#[derive(Debug)]
struct PluginIsolationState {
    plugin_id: PluginId,
    installed: InstalledPlugin,
    job: PluginJob,
    artifact: PluginArtifact,
    download_dir: PathBuf,
    sentinel: Vec<u8>,
}

impl Deployment {
    fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("working");
        let log_dir = directory.path().join("logs");
        let config_root = directory.path().join("config");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&config_root).unwrap();
        Self {
            store_path: directory.path().join("store/replica.sqlite3"),
            identity: NodeIdentity::generate("replica-test".parse().unwrap()),
            _directory: directory,
            root,
            config_root,
            log_dir,
        }
    }

    async fn start(&self) -> Arc<ReplicaRuntime> {
        let logger = NodeLogger::open(&self.log_dir, self.identity.clone(), None).unwrap();
        let runtime = ReplicaRuntime::start(
            self.config_root.clone(),
            self.root.clone(),
            &ReplicaStoreConfig::Sqlite {
                path: self.store_path.clone(),
            },
            IdentityCoordinator::new(self.identity.clone()),
            logger,
        )
        .await
        .unwrap();
        runtime
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(2))
            .unwrap();
        runtime
    }

    fn native(&self, namespace: &str) -> PathBuf {
        self.root
            .join(namespace.strip_prefix('/').unwrap_or(namespace))
    }
}

async fn seed_plugin_isolation_state(
    runtime: &ReplicaRuntime,
    directory: &Path,
    label: &str,
) -> PluginIsolationState {
    let store = PluginStore::initialize(runtime.database_pool())
        .await
        .unwrap();
    let plugin_id: PluginId = format!("oll.{label}").parse().unwrap();
    let sentinel = format!("plugin-only-{label}-{}", Uuid::new_v4()).into_bytes();
    let generation = Uuid::new_v4();
    let declaration = format!("declaration:{label}").into_bytes();
    let package = PackagePublishIntent {
        plugin_id: plugin_id.clone(),
        plugin_name: label.parse().unwrap(),
        operation_id: format!("install-{label}"),
        expected_current_generation: None,
        candidate_generation: generation,
        normalized_declaration: declaration.clone(),
        declaration_sha256: Sha256::digest(&declaration).into(),
        effective_manifest: sentinel.clone(),
        selected_commit: Some("0123456789abcdef".to_owned()),
        install_mode: InstallMode::Source,
        release_id: None,
        correlation_id: format!("install-{label}-correlation"),
    };
    store.prepare_package_publish(&package).await.unwrap();
    store
        .finalize_package_publish(&plugin_id, generation)
        .await
        .unwrap();
    store
        .set_desired_state(&plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    store.request_restart(&plugin_id).await.unwrap();
    let instance_id = PluginInstanceId::new();
    store
        .record_running_instance(&plugin_id, generation, instance_id)
        .await
        .unwrap();

    let admitted_at = time::OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let operation_id: PluginOperationId = format!("job-{label}").parse().unwrap();
    let payload = NormalizedJobPayload::new(
        plugin_id.clone(),
        "isolation".to_owned(),
        vec![String::from_utf8(sentinel.clone()).unwrap()],
        None,
    )
    .unwrap();
    let JobAdmission::Created(job) = store
        .admit_job(
            &operation_id,
            &payload,
            instance_id,
            admitted_at,
            &format!("job-{label}-correlation"),
        )
        .await
        .unwrap()
    else {
        panic!("isolation job was unexpectedly retained")
    };
    store
        .mark_job_accepted(job.job_id, instance_id, admitted_at)
        .await
        .unwrap();

    let download_dir = directory.join(format!("{label}-artifacts"));
    fs::create_dir(&download_dir).unwrap();
    store
        .cache_artifact_download_dir(&download_dir)
        .await
        .unwrap();
    let artifact_id = PluginArtifactId::new();
    let destination = download_dir.join("isolation.bin");
    fs::write(&destination, &sentinel).unwrap();
    let artifact_intent = ArtifactPublishIntent {
        artifact_id,
        job_id: job.job_id,
        plugin_id: plugin_id.clone(),
        file_name: "isolation.bin".to_owned(),
        media_type: "application/octet-stream".to_owned(),
        size_bytes: u64::try_from(sentinel.len()).unwrap(),
        sha256: Sha256::digest(&sentinel).into(),
        staging_path: download_dir.join(".isolation.staging"),
        destination,
        correlation_id: job.correlation_id.clone(),
    };
    store
        .prepare_artifact_publish(&artifact_intent)
        .await
        .unwrap();
    let artifact = store
        .finalize_artifact_publish(artifact_id, admitted_at)
        .await
        .unwrap();
    let job = store
        .finish_job(
            job.job_id,
            instance_id,
            JobState::Succeeded,
            Some(&sentinel),
            None,
            None,
            admitted_at,
        )
        .await
        .unwrap();
    let installed = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    PluginIsolationState {
        plugin_id,
        installed,
        job,
        artifact,
        download_dir,
        sentinel,
    }
}

async fn assert_plugin_isolation_state(runtime: &ReplicaRuntime, expected: &PluginIsolationState) {
    let store = PluginStore::initialize(runtime.database_pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(expected.plugin_id.clone()))
            .await
            .unwrap(),
        expected.installed
    );
    assert_eq!(
        store.get_job(expected.job.job_id).await.unwrap(),
        expected.job
    );
    assert_eq!(
        store
            .get_artifact(expected.artifact.artifact_id)
            .await
            .unwrap(),
        expected.artifact
    );
    assert_eq!(
        store.artifact_download_dir().await.unwrap().as_deref(),
        Some(expected.download_dir.as_path())
    );
    assert_eq!(
        fs::read(&expected.artifact.destination).unwrap(),
        expected.sentinel
    );
}

fn document_path(value: &str) -> Option<oll::DocumentPath> {
    Some(oll::DocumentPath {
        value: value.to_owned(),
    })
}

fn document_revision_precondition(
    inspection: &super::watcher::DocumentInspection,
) -> oll::CommitPrecondition {
    oll::CommitPrecondition {
        condition: Some(oll::commit_precondition::Condition::DocumentUnchanged(
            oll::DocumentRevisionPrecondition {
                document_id: Some(oll::DocumentId {
                    value: inspection.document_id.to_string(),
                }),
                unchanged_since: Some(oll::DocumentRevision {
                    token: inspection.document_revision.to_vec(),
                }),
            },
        )),
    }
}

fn catalog_revision_precondition(
    inspection: &super::watcher::DocumentInspection,
) -> oll::CommitPrecondition {
    oll::CommitPrecondition {
        condition: Some(oll::commit_precondition::Condition::CatalogUnchanged(
            oll::CatalogRevisionPrecondition {
                catalog_node_id: Some(oll::CatalogNodeId {
                    value: inspection.catalog_node_id.to_string(),
                }),
                unchanged_since: Some(oll::CatalogRevision {
                    token: inspection.catalog_revision.to_vec(),
                }),
            },
        )),
    }
}

fn replace_mutation(path: &str, content: &str) -> oll::DocumentMutation {
    oll::DocumentMutation {
        mutation: Some(oll::document_mutation::Mutation::ReplaceDocument(
            oll::ReplaceDocument {
                path: document_path(path),
                content: content.to_owned(),
                media_type: None,
            },
        )),
    }
}

fn read_content(response: oll::ReadDocumentResponse) -> String {
    match response.document.unwrap().representation.unwrap() {
        oll::document_snapshot::Representation::Content(content) => content,
        oll::document_snapshot::Representation::Crdt(_) => {
            panic!("expected content projection")
        }
    }
}

async fn wait_for_document(
    runtime: &ReplicaRuntime,
    path: &Path,
) -> super::watcher::DocumentInspection {
    for _ in 0..50 {
        if let Ok(inspection) = runtime.inspect_document(path).await {
            return inspection;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("document was not reconciled before the test deadline");
}

async fn wait_for_path(runtime: &ReplicaRuntime, namespace: &str) {
    for _ in 0..50 {
        let state = runtime.state.read().await;
        if state
            .as_ref()
            .and_then(|replica| replica.entry_at_path(namespace).ok().flatten())
            .is_some()
        {
            return;
        }
        drop(state);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{namespace} was not reconciled before the test deadline");
}

async fn shutdown_runtime(runtime: &ReplicaRuntime) {
    runtime
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    runtime
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
}

fn reconciliation_start_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|record| record["event"] == "working_tree_reconciliation_started")
        .count()
}

async fn wait_for_reconciliation_start_count(path: &Path, expected: usize) {
    for _ in 0..100 {
        if reconciliation_start_count(path) >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("watcher did not start the expected reconciliation");
}

mod commit;
mod crdt;
mod encoding;
mod projection;
mod recovery;
mod replication;
mod snapshot;
mod watcher;
