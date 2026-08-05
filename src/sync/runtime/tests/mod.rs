use std::{fs, net::SocketAddr, path::PathBuf, str::FromStr};

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    configuration::{NetworkKey, ReplicaStoreConfig},
    node::{NodeIdentity, logging::NodeLogger},
    protocol::oll::{
        CommitDocumentsRequest, CreateDocument, DeleteNode, DocumentMutation, DocumentPath,
        DocumentProjection, MoveNode, ReadDocumentRequest, ReplaceDocument, document_mutation,
    },
    replica::OperationSource,
};

use super::*;

struct SyncDeployment {
    _directory: TempDir,
    root: PathBuf,
    config_root: PathBuf,
    store: ReplicaStoreConfig,
    log_dir: PathBuf,
    identity: NodeIdentity,
    identities: Arc<IdentityCoordinator>,
}

impl SyncDeployment {
    fn new(name: &str) -> Self {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("working");
        let config_root = directory.path().join("config");
        let log_dir = directory.path().join("logs");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&config_root).unwrap();
        let identity = NodeIdentity::generate(name.parse().unwrap());
        Self {
            store: ReplicaStoreConfig::Sqlite {
                path: directory.path().join("store/replica.sqlite3"),
            },
            identities: IdentityCoordinator::new(identity.clone()),
            identity,
            _directory: directory,
            root,
            config_root,
            log_dir,
        }
    }

    async fn start_replica(&self) -> (Arc<ReplicaRuntime>, Arc<NodeLogger>) {
        let logger = NodeLogger::open(&self.log_dir, self.identity.clone(), None).unwrap();
        let replica = ReplicaRuntime::start(
            self.config_root.clone(),
            self.root.clone(),
            &self.store,
            Arc::clone(&self.identities),
            Arc::clone(&logger),
        )
        .await
        .unwrap();
        (replica, logger)
    }

    fn sync_config(
        &self,
        listen: Option<SocketAddr>,
        connect: Vec<ConnectUrl>,
    ) -> ResolvedNodeConfig {
        ResolvedNodeConfig {
            replica_root: self.root.clone(),
            replica_store: self.store.clone(),
            log_dir: self.log_dir.clone(),
            artifact_download_dir: self
                .config_root
                .parent()
                .expect("test config root has a parent")
                .join("downloads/oll"),
            listen,
            connect,
            network_key: Some(NetworkKey::new_for_test(vec![7; 32])),
        }
    }
}

fn unused_loopback_address() -> SocketAddr {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn read_text(replica: &ReplicaRuntime, path: &str) -> String {
    let response = replica
        .read_document(ReadDocumentRequest {
            path: Some(DocumentPath {
                value: path.to_owned(),
            }),
            projection: DocumentProjection::Content as i32,
        })
        .await
        .unwrap();
    match response.document.unwrap().representation.unwrap() {
        crate::protocol::oll::document_snapshot::Representation::Content(content) => content,
        _ => panic!("expected content projection"),
    }
}

async fn create_text(replica: &ReplicaRuntime, operation_id: &str, path: &str, text: &str) {
    replica
        .commit_documents(
            CommitDocumentsRequest {
                operation_id: operation_id.to_owned(),
                preconditions: Vec::new(),
                mutations: vec![DocumentMutation {
                    mutation: Some(document_mutation::Mutation::CreateDocument(
                        CreateDocument {
                            path: Some(DocumentPath {
                                value: path.to_owned(),
                            }),
                            media_type: "text/plain".to_owned(),
                            content: text.to_owned(),
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            operation_id,
        )
        .await
        .unwrap();
}

async fn replace_text(replica: &ReplicaRuntime, operation_id: &str, path: &str, text: &str) {
    replica
        .commit_documents(
            CommitDocumentsRequest {
                operation_id: operation_id.to_owned(),
                preconditions: Vec::new(),
                mutations: vec![DocumentMutation {
                    mutation: Some(document_mutation::Mutation::ReplaceDocument(
                        ReplaceDocument {
                            path: Some(DocumentPath {
                                value: path.to_owned(),
                            }),
                            content: text.to_owned(),
                            media_type: None,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            operation_id,
        )
        .await
        .unwrap();
}

async fn move_node(replica: &ReplicaRuntime, operation_id: &str, source: &str, destination: &str) {
    replica
        .commit_documents(
            CommitDocumentsRequest {
                operation_id: operation_id.to_owned(),
                preconditions: Vec::new(),
                mutations: vec![DocumentMutation {
                    mutation: Some(document_mutation::Mutation::MoveNode(MoveNode {
                        source: Some(DocumentPath {
                            value: source.to_owned(),
                        }),
                        destination: Some(DocumentPath {
                            value: destination.to_owned(),
                        }),
                    })),
                }],
            },
            OperationSource::Plugin,
            operation_id,
        )
        .await
        .unwrap();
}

async fn delete_node(replica: &ReplicaRuntime, operation_id: &str, path: &str) {
    replica
        .commit_documents(
            CommitDocumentsRequest {
                operation_id: operation_id.to_owned(),
                preconditions: Vec::new(),
                mutations: vec![DocumentMutation {
                    mutation: Some(document_mutation::Mutation::DeleteNode(DeleteNode {
                        path: Some(DocumentPath {
                            value: path.to_owned(),
                        }),
                        recursive: false,
                    })),
                }],
            },
            OperationSource::Plugin,
            operation_id,
        )
        .await
        .unwrap();
}

mod bootstrap;
mod convergence;
mod lifecycle;
mod ping;
