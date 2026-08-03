use super::*;

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
    shutdown_runtime(&runtime).await;
}
