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
        let logger = NodeLogger::open(&self.log_dir, self.identity.clone()).unwrap();
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
