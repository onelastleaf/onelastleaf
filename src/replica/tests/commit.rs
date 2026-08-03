use super::*;

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
    shutdown_runtime(&runtime).await;
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
    shutdown_runtime(&restarted).await;

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
    shutdown_runtime(&runtime).await;
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
    shutdown_runtime(&runtime).await;
}
