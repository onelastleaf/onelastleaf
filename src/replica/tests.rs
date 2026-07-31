use std::{
    collections::HashMap,
    fs,
    os::unix::fs::{MetadataExt, symlink},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    configuration::ReplicaStoreConfig,
    node::{NodeIdentity, logging::NodeLogger},
    protocol::oll,
};

use super::{
    OperationSource, ReplicaError, ReplicaRuntime, ReplicaStatus,
    store::{NewBlob, NewBlobSource},
    types::EntryData,
};

struct Deployment {
    _directory: TempDir,
    root: PathBuf,
    store_path: PathBuf,
    log_dir: PathBuf,
    identity: NodeIdentity,
}

impl Deployment {
    fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("working");
        let log_dir = directory.path().join("logs");
        fs::create_dir(&root).unwrap();
        Self {
            store_path: directory.path().join("store/replica.sqlite3"),
            identity: NodeIdentity::generate("replica-test".parse().unwrap()),
            _directory: directory,
            root,
            log_dir,
        }
    }

    async fn start(&self) -> Arc<ReplicaRuntime> {
        let logger = NodeLogger::open(&self.log_dir, self.identity.clone()).unwrap();
        ReplicaRuntime::start(
            self.root.clone(),
            &ReplicaStoreConfig::Sqlite {
                path: self.store_path.clone(),
            },
            self.identity.node_id(),
            logger,
        )
        .await
        .unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_initializes_once_deduplicates_final_state_and_survives_restart() {
    let deployment = Deployment::new();
    let runtime = deployment.start().await;
    assert_eq!(runtime.status().await, ReplicaStatus::Uninitialized);

    fs::create_dir(deployment.native("/notes")).unwrap();
    fs::write(deployment.native("/notes/a.md"), "hello\n").unwrap();
    fs::write(
        deployment.native("/image.gif"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();

    let first = wait_for_document(&runtime, &deployment.native("/notes/a.md")).await;
    wait_for_path(&runtime, "/image.gif").await;
    let replica_id = match runtime.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let operations_before = runtime
        .list_operations(&deployment.native("/notes/a.md"), 100)
        .await
        .unwrap();
    let (binary_versions_before, lamport_before) = {
        let state = runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        (binary.versions.len(), replica.lamport_clock)
    };

    fs::create_dir_all(deployment.native("/notes")).unwrap();
    fs::write(deployment.native("/notes/a.md"), "hello\n").unwrap();
    fs::write(
        deployment.native("/image.gif"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    let duplicate = runtime
        .inspect_document(&deployment.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(duplicate.catalog_node_id, first.catalog_node_id);
    assert_eq!(duplicate.document_id, first.document_id);
    assert_eq!(duplicate.catalog_revision, first.catalog_revision);
    assert_eq!(duplicate.document_revision, first.document_revision);
    assert_eq!(
        runtime
            .list_operations(&deployment.native("/notes/a.md"), 100)
            .await
            .unwrap()
            .len(),
        operations_before.len()
    );
    {
        let state = runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        assert_eq!(binary.versions.len(), binary_versions_before);
        assert_eq!(replica.lamport_clock, lamport_before);
    }

    runtime.shutdown().await.unwrap();
    drop(runtime);
    let restarted = deployment.start().await;
    assert_eq!(
        restarted.status().await,
        ReplicaStatus::InitializedPopulated { replica_id }
    );
    let after_restart = restarted
        .inspect_document(&deployment.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(after_restart.catalog_node_id, first.catalog_node_id);
    assert_eq!(after_restart.document_id, first.document_id);
    assert_eq!(after_restart.document_revision, first.document_revision);
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_registration_closes_the_startup_scan_race() {
    let deployment = Deployment::new();
    for index in 0..300 {
        fs::write(
            deployment.native(&format!("/seed-{index:03}.md")),
            format!("seed {index}"),
        )
        .unwrap();
    }
    let logger = NodeLogger::open(&deployment.log_dir, deployment.identity.clone()).unwrap();
    let root = deployment.root.clone();
    let config = ReplicaStoreConfig::Sqlite {
        path: deployment.store_path.clone(),
    };
    let writer = deployment.identity.node_id();
    let starting =
        tokio::spawn(async move { ReplicaRuntime::start(root, &config, writer, logger).await });
    tokio::task::yield_now().await;
    fs::write(deployment.native("/arrived-during-startup.md"), "not lost").unwrap();

    let runtime = starting.await.unwrap().unwrap();
    let late = wait_for_document(&runtime, &deployment.native("/arrived-during-startup.md")).await;
    assert_eq!(late.path, "/arrived-during-startup.md");
    assert_eq!(
        fs::read_to_string(deployment.native("/arrived-during-startup.md")).unwrap(),
        "not lost"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_rename_and_editor_replacement_preserve_identity_but_offline_move_does_not() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "first").unwrap();
    let runtime = deployment.start().await;
    let original = runtime
        .inspect_document(&deployment.native("/a.md"))
        .await
        .unwrap();

    fs::rename(deployment.native("/a.md"), deployment.native("/renamed.md")).unwrap();
    let renamed = wait_for_document(&runtime, &deployment.native("/renamed.md")).await;
    assert_eq!(renamed.catalog_node_id, original.catalog_node_id);
    assert_eq!(renamed.document_id, original.document_id);

    fs::write(deployment.native("/.editor-save.tmp"), "editor replacement").unwrap();
    fs::rename(
        deployment.native("/.editor-save.tmp"),
        deployment.native("/renamed.md"),
    )
    .unwrap();
    let mut replaced = None;
    for _ in 0..50 {
        let inspection = runtime
            .inspect_document(&deployment.native("/renamed.md"))
            .await
            .unwrap();
        if inspection.document_revision != renamed.document_revision {
            replaced = Some(inspection);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let replaced = replaced.expect("editor replacement was not reconciled");
    assert_eq!(replaced.catalog_node_id, original.catalog_node_id);
    assert_eq!(replaced.document_id, original.document_id);
    assert_eq!(
        fs::read_to_string(deployment.native("/renamed.md")).unwrap(),
        "editor replacement"
    );

    runtime.shutdown().await.unwrap();
    drop(runtime);
    fs::rename(
        deployment.native("/renamed.md"),
        deployment.native("/offline.md"),
    )
    .unwrap();
    let restarted = deployment.start().await;
    let offline = restarted
        .inspect_document(&deployment.native("/offline.md"))
        .await
        .unwrap();
    assert_ne!(offline.catalog_node_id, original.catalog_node_id);
    assert_ne!(offline.document_id, original.document_id);
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commits_are_atomic_revision_guarded_idempotent_and_persistent() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "zero").unwrap();
    let runtime = deployment.start().await;
    let original = runtime
        .inspect_document(&deployment.native("/a.md"))
        .await
        .unwrap();

    let first_request = oll::CommitDocumentsRequest {
        operation_id: "replace-a-once".to_owned(),
        preconditions: vec![document_revision_precondition(&original)],
        mutations: vec![replace_mutation("/a.md", "one")],
    };
    let first_response = runtime
        .commit_documents(
            first_request.clone(),
            OperationSource::Plugin,
            "test-api-correlation",
        )
        .await
        .unwrap();
    let after_content_update = runtime
        .inspect_document(&deployment.native("/a.md"))
        .await
        .unwrap();
    assert_eq!(
        after_content_update.catalog_revision,
        original.catalog_revision
    );
    assert_ne!(
        after_content_update.document_revision,
        original.document_revision
    );
    assert_eq!(
        read_content(
            runtime
                .read_document(oll::ReadDocumentRequest {
                    path: document_path("/a.md"),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap()
        ),
        "one"
    );

    let retry = runtime
        .commit_documents(
            first_request.clone(),
            OperationSource::Plugin,
            "different-retry-correlation",
        )
        .await
        .unwrap();
    assert_eq!(retry, first_response);
    let conflicting_reuse = oll::CommitDocumentsRequest {
        mutations: vec![replace_mutation("/a.md", "not-the-original-request")],
        ..first_request.clone()
    };
    assert!(matches!(
        runtime
            .commit_documents(
                conflicting_reuse,
                OperationSource::Plugin,
                "test-api-correlation"
            )
            .await,
        Err(ReplicaError::InvalidArgument(_))
    ));

    let stale = oll::CommitDocumentsRequest {
        operation_id: "stale-all-or-nothing".to_owned(),
        preconditions: vec![document_revision_precondition(&original)],
        mutations: vec![
            replace_mutation("/a.md", "must-not-commit"),
            oll::DocumentMutation {
                mutation: Some(oll::document_mutation::Mutation::CreateDirectory(
                    oll::CreateDirectory {
                        path: document_path("/must-not-exist"),
                    },
                )),
            },
        ],
    };
    assert!(matches!(
        runtime
            .commit_documents(stale, OperationSource::Plugin, "test-api-correlation")
            .await,
        Err(ReplicaError::RevisionConflict(_))
    ));
    assert_eq!(
        read_content(
            runtime
                .read_document(oll::ReadDocumentRequest {
                    path: document_path("/a.md"),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap()
        ),
        "one"
    );
    assert!(
        runtime
            .list_directory(oll::ListDirectoryRequest {
                path: document_path("/"),
                recursive: true,
            })
            .await
            .unwrap()
            .entries
            .iter()
            .all(|entry| entry.path.as_ref().unwrap().value != "/must-not-exist")
    );

    let current = runtime
        .inspect_document(&deployment.native("/a.md"))
        .await
        .unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "move-a".to_owned(),
                preconditions: vec![catalog_revision_precondition(&current)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::MoveNode(oll::MoveNode {
                        source: document_path("/a.md"),
                        destination: document_path("/moved.md"),
                    })),
                }],
            },
            OperationSource::Plugin,
            "test-api-correlation",
        )
        .await
        .unwrap();
    assert!(!deployment.native("/a.md").exists());
    assert_eq!(
        fs::read_to_string(deployment.native("/moved.md")).unwrap(),
        "one"
    );

    let moved = runtime
        .inspect_document(&deployment.native("/moved.md"))
        .await
        .unwrap();
    assert_eq!(moved.catalog_node_id, original.catalog_node_id);
    assert_eq!(moved.document_id, original.document_id);
    assert_eq!(
        moved.document_revision,
        after_content_update.document_revision
    );

    let create_response = runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "ordered-create".to_owned(),
                preconditions: vec![
                    oll::CommitPrecondition {
                        condition: Some(oll::commit_precondition::Condition::MustNotExist(
                            oll::DocumentPath {
                                value: "/folder".to_owned(),
                            },
                        )),
                    },
                    oll::CommitPrecondition {
                        condition: Some(oll::commit_precondition::Condition::MustNotExist(
                            oll::DocumentPath {
                                value: "/folder/new.md".to_owned(),
                            },
                        )),
                    },
                ],
                mutations: vec![
                    oll::DocumentMutation {
                        mutation: Some(oll::document_mutation::Mutation::CreateDirectory(
                            oll::CreateDirectory {
                                path: document_path("/folder"),
                            },
                        )),
                    },
                    oll::DocumentMutation {
                        mutation: Some(oll::document_mutation::Mutation::CreateDocument(
                            oll::CreateDocument {
                                path: document_path("/folder/new.md"),
                                media_type: "text/markdown".to_owned(),
                                content: "created".to_owned(),
                            },
                        )),
                    },
                ],
            },
            OperationSource::Plugin,
            "test-api-correlation",
        )
        .await
        .unwrap();
    assert_eq!(create_response.updated_nodes.len(), 2);
    let created = runtime
        .inspect_document(&deployment.native("/folder/new.md"))
        .await
        .unwrap();
    let tree = runtime
        .get_directory_tree(oll::GetDirectoryTreeRequest {
            root: document_path("/"),
        })
        .await
        .unwrap()
        .root
        .unwrap();
    assert!(tree.children.iter().any(|child| {
        child
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.path.as_ref())
            .is_some_and(|path| path.value == "/folder")
    }));
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "delete-created".to_owned(),
                preconditions: vec![catalog_revision_precondition(&created)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::DeleteNode(
                        oll::DeleteNode {
                            path: document_path("/folder/new.md"),
                            recursive: false,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "test-api-correlation",
        )
        .await
        .unwrap();
    assert!(!deployment.native("/folder/new.md").exists());
    assert!(
        runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .documents
            .contains_key(&created.document_id)
    );
    runtime.shutdown().await.unwrap();
    drop(runtime);

    let restarted = deployment.start().await;
    let persisted = restarted
        .inspect_document(&deployment.native("/moved.md"))
        .await
        .unwrap();
    assert_eq!(persisted.catalog_node_id, original.catalog_node_id);
    assert_eq!(persisted.document_id, original.document_id);
    assert_eq!(
        read_content(
            restarted
                .read_document(oll::ReadDocumentRequest {
                    path: document_path("/moved.md"),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap()
        ),
        "one"
    );
    assert!(matches!(
        restarted
            .read_document(oll::ReadDocumentRequest {
                path: document_path("/folder/new.md"),
                projection: oll::DocumentProjection::Content as i32,
            })
            .await,
        Err(ReplicaError::NotFound(_))
    ));
    assert!(
        restarted
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .documents
            .contains_key(&created.document_id)
    );
    restarted.shutdown().await.unwrap();

    let log = fs::read_to_string(deployment.log_dir.join("oll.log")).unwrap();
    assert!(log.contains("\"event\":\"document_commit_started\""));
    assert!(log.contains("\"event\":\"document_commit_completed\""));
    assert!(log.contains("\"event\":\"document_commit_failed\""));
    assert!(log.contains("\"correlation_id\":\"test-api-correlation\""));
    assert!(!log.contains("must-not-commit"));
    assert!(!log.contains("not-the-original-request"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writers_with_one_revision_allow_exactly_one_commit() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/race.md"), "before").unwrap();
    let runtime = deployment.start().await;
    let inspection = runtime
        .inspect_document(&deployment.native("/race.md"))
        .await
        .unwrap();
    let first = oll::CommitDocumentsRequest {
        operation_id: "concurrent-first".to_owned(),
        preconditions: vec![document_revision_precondition(&inspection)],
        mutations: vec![replace_mutation("/race.md", "first")],
    };
    let second = oll::CommitDocumentsRequest {
        operation_id: "concurrent-second".to_owned(),
        preconditions: vec![document_revision_precondition(&inspection)],
        mutations: vec![replace_mutation("/race.md", "second")],
    };
    let (first_result, second_result) = tokio::join!(
        runtime.commit_documents(
            first,
            OperationSource::Plugin,
            "concurrent-first-correlation"
        ),
        runtime.commit_documents(
            second,
            OperationSource::Plugin,
            "concurrent-second-correlation"
        )
    );
    assert_eq!(
        usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
        1
    );
    assert_eq!(
        usize::from(matches!(
            first_result,
            Err(ReplicaError::RevisionConflict(_))
        )) + usize::from(matches!(
            second_result,
            Err(ReplicaError::RevisionConflict(_))
        )),
        1
    );
    let content = fs::read_to_string(deployment.native("/race.md")).unwrap();
    assert!(matches!(content.as_str(), "first" | "second"));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_watcher_trigger_cannot_overwrite_a_completed_host_commit() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/race.md"), "initial").unwrap();
    let runtime = deployment.start().await;

    fs::write(deployment.native("/race.md"), "filesystem-before-api").unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "api-wins-after-trigger".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/race.md", "api-authoritative")],
            },
            OperationSource::Plugin,
            "watcher-api-race-correlation",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert_eq!(
        fs::read_to_string(deployment.native("/race.md")).unwrap(),
        "api-authoritative"
    );
    assert_eq!(
        read_content(
            runtime
                .read_document(oll::ReadDocumentRequest {
                    path: document_path("/race.md"),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap()
        ),
        "api-authoritative"
    );
    runtime.shutdown().await.unwrap();
}

fn scalar_string(value: &str) -> oll::CrdtScalar {
    oll::CrdtScalar {
        kind: Some(oll::crdt_scalar::Kind::StringValue(value.to_owned())),
    }
}

fn scalar_integer(value: i64) -> oll::CrdtScalar {
    oll::CrdtScalar {
        kind: Some(oll::crdt_scalar::Kind::IntegerValue(value)),
    }
}

fn scalar_value(value: oll::CrdtScalar) -> oll::CrdtValue {
    oll::CrdtValue {
        kind: Some(oll::crdt_value::Kind::Scalar(value)),
    }
}

fn map_target(key: &str) -> SomeCrdtPath {
    Some(oll::CrdtObjectPath {
        segments: vec![oll::CrdtPathSegment {
            kind: Some(oll::crdt_path_segment::Kind::MapKey(key.to_owned())),
        }],
    })
}

type SomeCrdtPath = Option<oll::CrdtObjectPath>;

fn map_set(key: &str, value: oll::CrdtValue) -> oll::CrdtOperation {
    oll::CrdtOperation {
        operation: Some(oll::crdt_operation::Operation::MapSet(oll::MapSet {
            target: None,
            key: key.to_owned(),
            value: Some(value),
        })),
    }
}

async fn read_crdt_kind(runtime: &ReplicaRuntime, key: &str) -> oll::crdt_value::Kind {
    runtime
        .read_crdt(oll::ReadCrdtRequest {
            document: document_path("/crdt.md"),
            object: map_target(key),
        })
        .await
        .unwrap()
        .value
        .unwrap()
        .kind
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abstract_crdt_operations_round_trip_without_changing_file_content() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/crdt.md"), "body").unwrap();
    let runtime = deployment.start().await;
    let before = runtime
        .inspect_document(&deployment.native("/crdt.md"))
        .await
        .unwrap();
    let inode_before = fs::metadata(deployment.native("/crdt.md")).unwrap().ino();
    let projection_generation_before = runtime
        .state
        .read()
        .await
        .as_ref()
        .unwrap()
        .projection_generation;

    let tree = oll::CrdtValue {
        kind: Some(oll::crdt_value::Kind::Tree(oll::CrdtTree {
            nodes: vec![
                oll::CrdtTreeNode {
                    node_id: "root-node".to_owned(),
                    parent_id: None,
                    index_in_parent: Some(0),
                    metadata: HashMap::from([("title".to_owned(), scalar_string("root"))]),
                },
                oll::CrdtTreeNode {
                    node_id: "child-node".to_owned(),
                    parent_id: Some("root-node".to_owned()),
                    index_in_parent: Some(0),
                    metadata: HashMap::new(),
                },
            ],
        })),
    };
    let operations = vec![
        map_set(
            "map",
            oll::CrdtValue {
                kind: Some(oll::crdt_value::Kind::Map(oll::CrdtMap {
                    entries: HashMap::from([(
                        "answer".to_owned(),
                        scalar_value(scalar_integer(42)),
                    )]),
                })),
            },
        ),
        map_set(
            "list",
            oll::CrdtValue {
                kind: Some(oll::crdt_value::Kind::List(oll::CrdtList {
                    values: vec![
                        scalar_value(scalar_string("a")),
                        scalar_value(scalar_string("b")),
                    ],
                    movable: false,
                })),
            },
        ),
        map_set(
            "movable",
            oll::CrdtValue {
                kind: Some(oll::crdt_value::Kind::List(oll::CrdtList {
                    values: vec![
                        scalar_value(scalar_string("a")),
                        scalar_value(scalar_string("b")),
                        scalar_value(scalar_string("c")),
                    ],
                    movable: true,
                })),
            },
        ),
        map_set(
            "text",
            oll::CrdtValue {
                kind: Some(oll::crdt_value::Kind::Text(oll::CrdtText {
                    text: "ab".to_owned(),
                    marks: Vec::new(),
                })),
            },
        ),
        map_set(
            "counter",
            oll::CrdtValue {
                kind: Some(oll::crdt_value::Kind::Counter(oll::CrdtCounter {
                    value: 2.0,
                })),
            },
        ),
        map_set("tree", tree),
        oll::CrdtOperation {
            operation: Some(oll::crdt_operation::Operation::ListMove(oll::ListMove {
                target: map_target("movable"),
                index: 0,
                count: 1,
                destination: 2,
            })),
        },
        oll::CrdtOperation {
            operation: Some(oll::crdt_operation::Operation::TextInsert(
                oll::TextInsert {
                    target: map_target("text"),
                    scalar_index: 1,
                    text: "X".to_owned(),
                },
            )),
        },
        oll::CrdtOperation {
            operation: Some(oll::crdt_operation::Operation::CounterIncrement(
                oll::CounterIncrement {
                    target: map_target("counter"),
                    delta: 3.0,
                },
            )),
        },
        oll::CrdtOperation {
            operation: Some(oll::crdt_operation::Operation::TreeSetMetadata(
                oll::TreeSetMetadata {
                    target: map_target("tree"),
                    node_id: "child-node".to_owned(),
                    key: "label".to_owned(),
                    value: Some(scalar_string("child")),
                },
            )),
        },
    ];
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "populate-crdt".to_owned(),
                preconditions: vec![document_revision_precondition(&before)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::ApplyCrdtOperations(
                        oll::ApplyCrdtOperations {
                            document: document_path("/crdt.md"),
                            operations,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "test-crdt-correlation",
        )
        .await
        .unwrap();

    let oll::crdt_value::Kind::Map(map) = read_crdt_kind(&runtime, "map").await else {
        panic!("expected map")
    };
    assert!(matches!(
        map.entries["answer"].kind,
        Some(oll::crdt_value::Kind::Scalar(oll::CrdtScalar {
            kind: Some(oll::crdt_scalar::Kind::IntegerValue(42))
        }))
    ));
    let oll::crdt_value::Kind::List(list) = read_crdt_kind(&runtime, "movable").await else {
        panic!("expected movable list")
    };
    assert!(list.movable);
    let list_strings = list
        .values
        .into_iter()
        .map(|value| match value.kind.unwrap() {
            oll::crdt_value::Kind::Scalar(oll::CrdtScalar {
                kind: Some(oll::crdt_scalar::Kind::StringValue(value)),
            }) => value,
            _ => panic!("expected string scalar"),
        })
        .collect::<Vec<_>>();
    assert_eq!(list_strings, ["b", "c", "a"]);
    let oll::crdt_value::Kind::Text(text) = read_crdt_kind(&runtime, "text").await else {
        panic!("expected text")
    };
    assert_eq!(text.text, "aXb");
    let oll::crdt_value::Kind::Counter(counter) = read_crdt_kind(&runtime, "counter").await else {
        panic!("expected counter")
    };
    assert_eq!(counter.value, 5.0);
    let oll::crdt_value::Kind::Tree(tree) = read_crdt_kind(&runtime, "tree").await else {
        panic!("expected tree")
    };
    assert_eq!(tree.nodes.len(), 2);
    assert_eq!(
        tree.nodes
            .iter()
            .find(|node| node.node_id == "child-node")
            .unwrap()
            .metadata["label"],
        scalar_string("child")
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/crdt.md")).unwrap(),
        "body"
    );
    assert_eq!(
        fs::metadata(deployment.native("/crdt.md")).unwrap().ino(),
        inode_before
    );
    assert_eq!(
        runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .projection_generation,
        projection_generation_before
    );

    let revision_before_failure = runtime
        .inspect_document(&deployment.native("/crdt.md"))
        .await
        .unwrap()
        .document_revision;
    let invalid_commit = oll::CommitDocumentsRequest {
        operation_id: "invalid-ordered-crdt".to_owned(),
        preconditions: Vec::new(),
        mutations: vec![oll::DocumentMutation {
            mutation: Some(oll::document_mutation::Mutation::ApplyCrdtOperations(
                oll::ApplyCrdtOperations {
                    document: document_path("/crdt.md"),
                    operations: vec![
                        map_set("rolled_back", scalar_value(scalar_string("yes"))),
                        oll::CrdtOperation {
                            operation: Some(oll::crdt_operation::Operation::ListDelete(
                                oll::ListDelete {
                                    target: map_target("list"),
                                    index: 99,
                                    count: 1,
                                },
                            )),
                        },
                    ],
                },
            )),
        }],
    };
    assert!(matches!(
        runtime
            .commit_documents(
                invalid_commit,
                OperationSource::Plugin,
                "test-crdt-correlation"
            )
            .await,
        Err(ReplicaError::InvalidArgument(_))
    ));
    let root = runtime
        .read_crdt(oll::ReadCrdtRequest {
            document: document_path("/crdt.md"),
            object: None,
        })
        .await
        .unwrap();
    assert_eq!(
        root.revision.unwrap().token,
        revision_before_failure.to_vec()
    );
    let Some(oll::crdt_value::Kind::Map(root)) = root.value.unwrap().kind else {
        panic!("expected root map")
    };
    assert!(!root.entries.contains_key("rolled_back"));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrepresentable_legacy_text_is_durably_promoted_to_utf8() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/legacy.txt"), b"caf\xe9").unwrap();
    let runtime = deployment.start().await;
    let before = runtime
        .inspect_document(&deployment.native("/legacy.txt"))
        .await
        .unwrap();
    assert_ne!(before.encoding, "UTF-8");

    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "promote-legacy-encoding".to_owned(),
                preconditions: vec![document_revision_precondition(&before)],
                mutations: vec![replace_mutation("/legacy.txt", "café \u{1f343}")],
            },
            OperationSource::Plugin,
            "encoding-promotion-correlation",
        )
        .await
        .unwrap();
    let after = runtime
        .inspect_document(&deployment.native("/legacy.txt"))
        .await
        .unwrap();
    assert_eq!(after.encoding, "UTF-8");
    assert!(!after.has_byte_order_mark);
    assert_ne!(after.catalog_revision, before.catalog_revision);
    assert_eq!(
        fs::read(deployment.native("/legacy.txt")).unwrap(),
        "café \u{1f343}".as_bytes()
    );
    runtime.shutdown().await.unwrap();
    let log = fs::read_to_string(deployment.log_dir.join("oll.log")).unwrap();
    assert!(log.contains("\"event\":\"document_encoding_promoted\""));
    assert!(log.contains("\"correlation_id\":\"encoding-promotion-correlation\""));
    assert!(!log.contains("café"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_markers_win_over_stale_working_tree_after_restart() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "store-wins").unwrap();
    let runtime = deployment.start().await;
    runtime.shutdown().await.unwrap();

    let active = runtime.state.read().await.clone().unwrap();
    fs::write(deployment.native("/a.md"), "stale-disk").unwrap();
    runtime
        .store
        .save_active(&active, &[], &[], &["/a.md".to_owned()])
        .await
        .unwrap();
    drop(runtime);

    let restarted = deployment.start().await;
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "store-wins"
    );
    assert!(
        restarted
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_commit_retry_completes_its_pending_projection() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "before").unwrap();
    let runtime = deployment.start().await;
    let request = oll::CommitDocumentsRequest {
        operation_id: "retained-projection-result".to_owned(),
        preconditions: Vec::new(),
        mutations: vec![replace_mutation("/a.md", "committed")],
    };
    let response = runtime
        .commit_documents(
            request.clone(),
            OperationSource::Plugin,
            "original-correlation",
        )
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    let active = runtime.state.read().await.clone().unwrap();
    fs::write(deployment.native("/a.md"), "stale").unwrap();
    runtime
        .store
        .save_active(&active, &[], &[], &["/a.md".to_owned()])
        .await
        .unwrap();

    let retry = runtime
        .commit_documents(request, OperationSource::Plugin, "retry-correlation")
        .await
        .unwrap();
    assert_eq!(retry, response);
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "committed"
    );
    assert!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targeted_projection_never_follows_a_parent_symlink_outside_replica_root() {
    let deployment = Deployment::new();
    fs::create_dir(deployment.native("/dir")).unwrap();
    fs::write(deployment.native("/dir/a.md"), "before").unwrap();
    let outside = deployment._directory.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let runtime = deployment.start().await;
    runtime.shutdown().await.unwrap();

    fs::remove_dir_all(deployment.native("/dir")).unwrap();
    symlink(&outside, deployment.native("/dir")).unwrap();
    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "safe-parent-projection".to_owned(),
                preconditions: Vec::new(),
                mutations: vec![replace_mutation("/dir/a.md", "inside-only")],
            },
            OperationSource::Plugin,
            "safe-projection-correlation",
        )
        .await
        .unwrap();

    assert!(
        fs::symlink_metadata(deployment.native("/dir"))
            .unwrap()
            .is_dir()
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/dir/a.md")).unwrap(),
        "inside-only"
    );
    assert!(!outside.join("a.md").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_sql_transaction_and_generation_switch_boundaries_preserve_authority() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "authoritative").unwrap();
    let runtime = deployment.start().await;
    runtime.shutdown().await.unwrap();
    let active = runtime.state.read().await.clone().unwrap();

    let mut uncommitted = active.clone();
    uncommitted.projection_generation += 1;
    let wrong_blob = NewBlob {
        sha256: "0".repeat(64),
        source: NewBlobSource::Bytes(b"not-zero-hash".to_vec()),
    };
    assert!(
        runtime
            .store
            .save_active(
                &uncommitted,
                &[wrong_blob],
                &[],
                &["/must-not-persist".to_owned()]
            )
            .await
            .is_err()
    );
    let loaded = runtime.store.load_active().await.unwrap().unwrap();
    assert_eq!(loaded.projection_generation, active.projection_generation);
    assert!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );

    let mut inactive = active.clone();
    inactive.generation_id = Uuid::new_v4();
    inactive.replica_id = Uuid::new_v4();
    runtime
        .store
        .build_inactive_generation(&inactive, &[], &[])
        .await
        .unwrap();
    drop(runtime);

    let before_switch_restart = deployment.start().await;
    assert_eq!(
        before_switch_restart.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: active.replica_id
        }
    );
    before_switch_restart.shutdown().await.unwrap();
    before_switch_restart
        .store
        .activate_generation(Some(active.generation_id), inactive.generation_id)
        .await
        .unwrap();
    fs::write(deployment.native("/a.md"), "old-working-tree").unwrap();
    fs::write(deployment.native("/stale.md"), "must disappear").unwrap();
    drop(before_switch_restart);

    let after_switch_restart = deployment.start().await;
    assert_eq!(
        after_switch_restart.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: inactive.replica_id
        }
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "authoritative"
    );
    assert!(!deployment.native("/stale.md").exists());
    assert!(
        !after_switch_restart
            .store
            .projection_pending()
            .await
            .unwrap()
    );
    after_switch_restart.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_round_trip_preserves_documents_blobs_and_replaces_one_replica() {
    let source = Deployment::new();
    fs::create_dir(source.native("/notes")).unwrap();
    fs::write(source.native("/notes/a.md"), "snapshot text").unwrap();
    fs::write(source.native("/removed.md"), "retained tombstone").unwrap();
    let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
    fs::write(source.native("/image.gif"), &binary).unwrap();
    let source_runtime = source.start().await;
    let source_document = source_runtime
        .inspect_document(&source.native("/notes/a.md"))
        .await
        .unwrap();
    let removed_document = source_runtime
        .inspect_document(&source.native("/removed.md"))
        .await
        .unwrap();
    source_runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "delete-before-snapshot".to_owned(),
                preconditions: vec![catalog_revision_precondition(&removed_document)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::DeleteNode(
                        oll::DeleteNode {
                            path: document_path("/removed.md"),
                            recursive: false,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "snapshot-test-correlation",
        )
        .await
        .unwrap();
    let mut second_binary = binary.clone();
    second_binary[6] = 2;
    second_binary.resize(1024 * 1024 + 17, 0x5a);
    fs::write(source.native("/image.gif"), &second_binary).unwrap();
    for _ in 0..50 {
        let state = source_runtime.state.read().await;
        let versions = state
            .as_ref()
            .and_then(|replica| replica.entry_at_path("/image.gif").ok().flatten())
            .and_then(|entry| entry.binary())
            .map_or(0, |binary| binary.versions.len());
        if versions == 2 {
            break;
        }
        drop(state);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        source_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap()
            .versions
            .len(),
        2
    );
    fs::write(source.native("/image-copy.gif"), &second_binary).unwrap();
    wait_for_path(&source_runtime, "/image-copy.gif").await;
    let (source_replica_id, source_peer) = {
        let state = source_runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        (replica.replica_id, replica.loro_peer_id)
    };
    let snapshot = source._directory.path().join("backup.ollsnap");
    let (_, exported_replica_id) = source_runtime.export_snapshot(&snapshot).await.unwrap();
    assert_eq!(exported_replica_id, source_replica_id);
    let inspection = super::verify_snapshot(&snapshot).unwrap();
    assert_eq!(inspection.live_documents, 1);
    assert_eq!(inspection.tombstoned_documents, 1);
    assert_eq!(inspection.blobs, 2);
    assert!(matches!(
        source_runtime.export_snapshot(&snapshot).await,
        Err(ReplicaError::AlreadyExists(_))
    ));
    let racing_destination = source._directory.path().join("racing-backup.ollsnap");
    let (left, right) = tokio::join!(
        source_runtime.export_snapshot(&racing_destination),
        source_runtime.export_snapshot(&racing_destination)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(
        usize::from(matches!(left, Err(ReplicaError::AlreadyExists(_))))
            + usize::from(matches!(right, Err(ReplicaError::AlreadyExists(_)))),
        1
    );

    let target = Deployment::new();
    fs::write(target.native("/old.md"), "old replica").unwrap();
    let target_runtime = target.start().await;
    let old_replica_id = match target_runtime.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    assert_ne!(old_replica_id, source_replica_id);
    let (_, imported_replica_id) = target_runtime.import_snapshot(&snapshot).await.unwrap();
    assert_eq!(imported_replica_id, source_replica_id);
    assert!(!target.native("/old.md").exists());
    assert_eq!(
        fs::read_to_string(target.native("/notes/a.md")).unwrap(),
        "snapshot text"
    );
    assert_eq!(
        fs::read(target.native("/image.gif")).unwrap(),
        second_binary
    );
    assert_eq!(
        fs::read(target.native("/image-copy.gif")).unwrap(),
        second_binary
    );
    let imported_document = target_runtime
        .inspect_document(&target.native("/notes/a.md"))
        .await
        .unwrap();
    assert_eq!(imported_document.document_id, source_document.document_id);
    let imported_peer = target_runtime
        .state
        .read()
        .await
        .as_ref()
        .unwrap()
        .loro_peer_id;
    assert_ne!(imported_peer, source_peer);
    {
        let state = target_runtime.state.read().await;
        let replica = state.as_ref().unwrap();
        assert_eq!(replica.documents.len(), 2);
        assert!(
            replica
                .documents
                .contains_key(&removed_document.document_id)
        );
        let binary = replica
            .entry_at_path("/image.gif")
            .unwrap()
            .unwrap()
            .binary()
            .unwrap();
        assert_eq!(binary.versions.len(), 2);
        for version in binary.versions.values() {
            assert_eq!(
                target_runtime
                    .store
                    .read_blob(&version.sha256)
                    .await
                    .unwrap()
                    .len() as u64,
                version.size_bytes
            );
        }
    }
    assert!(!target.native("/removed.md").exists());

    target_runtime.shutdown().await.unwrap();
    drop(target_runtime);
    let restarted = target.start().await;
    assert_eq!(
        restarted.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_eq!(
        restarted
            .inspect_document(&target.native("/notes/a.md"))
            .await
            .unwrap()
            .document_id,
        source_document.document_id
    );
    restarted.shutdown().await.unwrap();

    let (_, same_replica_id) = source_runtime.import_snapshot(&snapshot).await.unwrap();
    assert_eq!(same_replica_id, source_replica_id);
    assert_eq!(
        source_runtime.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: source_replica_id
        }
    );
    assert_ne!(
        source_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .loro_peer_id,
        source_peer
    );
    source_runtime.shutdown().await.unwrap();
    let source_log = fs::read_to_string(source.log_dir.join("oll.log")).unwrap();
    assert!(source_log.contains("\"event\":\"snapshot_export_started\""));
    assert!(source_log.contains("\"event\":\"snapshot_export_completed\""));
    assert!(source_log.contains("\"event\":\"snapshot_export_failed\""));
    let target_log = fs::read_to_string(target.log_dir.join("oll.log")).unwrap();
    assert!(target_log.contains("\"event\":\"snapshot_import_started\""));
    assert!(target_log.contains("\"event\":\"snapshot_import_completed\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_empty_snapshot_round_trips_into_an_uninitialized_slot() {
    let source = Deployment::new();
    fs::write(source.native("/temporary.md"), "retained history").unwrap();
    let source_runtime = source.start().await;
    let document = source_runtime
        .inspect_document(&source.native("/temporary.md"))
        .await
        .unwrap();
    source_runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "empty-snapshot-delete".to_owned(),
                preconditions: vec![catalog_revision_precondition(&document)],
                mutations: vec![oll::DocumentMutation {
                    mutation: Some(oll::document_mutation::Mutation::DeleteNode(
                        oll::DeleteNode {
                            path: document_path("/temporary.md"),
                            recursive: false,
                        },
                    )),
                }],
            },
            OperationSource::Plugin,
            "empty-snapshot-correlation",
        )
        .await
        .unwrap();
    let replica_id = match source_runtime.status().await {
        ReplicaStatus::InitializedEmpty { replica_id } => replica_id,
        state => panic!("unexpected state: {state:?}"),
    };
    let snapshot = source._directory.path().join("empty.ollsnap");
    source_runtime.export_snapshot(&snapshot).await.unwrap();
    let inspection = super::verify_snapshot(&snapshot).unwrap();
    assert_eq!(inspection.live_documents, 0);
    assert_eq!(inspection.tombstoned_documents, 1);

    let target = Deployment::new();
    let target_runtime = target.start().await;
    assert_eq!(target_runtime.status().await, ReplicaStatus::Uninitialized);
    let (_, imported_replica_id) = target_runtime.import_snapshot(&snapshot).await.unwrap();
    assert_eq!(imported_replica_id, replica_id);
    assert_eq!(
        target_runtime.status().await,
        ReplicaStatus::InitializedEmpty { replica_id }
    );
    assert!(fs::read_dir(&target.root).unwrap().next().is_none());
    assert!(
        target_runtime
            .state
            .read()
            .await
            .as_ref()
            .unwrap()
            .documents
            .contains_key(&document.document_id)
    );

    target_runtime.shutdown().await.unwrap();
    source_runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filesystem_kind_replacement_allocates_new_stable_identity() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/changing"), "text").unwrap();
    let runtime = deployment.start().await;
    let text = runtime
        .inspect_document(&deployment.native("/changing"))
        .await
        .unwrap();

    fs::write(
        deployment.native("/changing"),
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff",
    )
    .unwrap();
    for _ in 0..50 {
        let state = runtime.state.read().await;
        if state
            .as_ref()
            .and_then(|replica| replica.entry_at_path("/changing").ok().flatten())
            .is_some_and(|entry| matches!(entry.data, EntryData::Binary(_)))
        {
            break;
        }
        drop(state);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let binary_identity = {
        let state = runtime.state.read().await;
        let entry = state
            .as_ref()
            .unwrap()
            .entry_at_path("/changing")
            .unwrap()
            .unwrap();
        assert_ne!(entry.catalog_node_id, text.catalog_node_id);
        entry.binary().unwrap().binary_id
    };

    fs::write(deployment.native("/changing"), "text again").unwrap();
    let replacement = wait_for_document(&runtime, &deployment.native("/changing")).await;
    assert_ne!(replacement.catalog_node_id, text.catalog_node_id);
    assert_ne!(replacement.document_id, text.document_id);
    let state = runtime.state.read().await;
    assert!(state.as_ref().unwrap().entries.values().any(|entry| {
        entry
            .binary()
            .is_some_and(|binary| binary.binary_id == binary_identity)
    }));
    drop(state);
    runtime.shutdown().await.unwrap();
}
