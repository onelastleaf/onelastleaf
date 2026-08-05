use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_catalog_payload_metadata_cannot_replace_the_active_replica() {
    let directory = TempDir::new().unwrap();
    let mut fixture = test_snapshot();
    fixture.mutate_catalog(|entries| {
        let document = entries
            .values_mut()
            .find_map(|entry| match &mut entry.data {
                EntryData::Document(document) => Some(document),
                _ => None,
            })
            .unwrap();
        document.size_bytes += 1;
    });
    let snapshot = directory.path().join("invalid-metadata.ollsnap");
    write_test_archive(
        &snapshot,
        manifest_source(&fixture.manifest),
        &fixture.entries(),
    );

    let root = directory.path().join("working");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("original.md"), "authoritative").unwrap();
    let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
    let identities = crate::node::identity::IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&directory.path().join("log"), identity.clone(), None).unwrap();
    let runtime = ReplicaRuntime::start(
        directory.path().to_owned(),
        root.clone(),
        &ReplicaStoreConfig::Sqlite {
            path: directory.path().join("store/replica.sqlite3"),
        },
        identities,
        logger,
    )
    .await
    .unwrap();
    let before = runtime.status().await;

    assert!(matches!(
        runtime
            .import_snapshot(&snapshot, "invalid-import-correlation")
            .await,
        Err(ReplicaError::InvalidSnapshot(_))
    ));
    assert_eq!(runtime.status().await, before);
    assert_eq!(
        fs::read_to_string(root.join("original.md")).unwrap(),
        "authoritative"
    );
    runtime
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_export_cleans_its_owned_temporary_archive() {
    let directory = TempDir::new().unwrap();
    let root = directory.path().join("working");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("document.md"), "snapshot content").unwrap();
    let identity = NodeIdentity::generate("snapshot-test".parse().unwrap());
    let identities = crate::node::identity::IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&directory.path().join("log"), identity.clone(), None).unwrap();
    let runtime = ReplicaRuntime::start(
        directory.path().to_owned(),
        root,
        &ReplicaStoreConfig::Sqlite {
            path: directory.path().join("store/replica.sqlite3"),
        },
        identities,
        logger,
    )
    .await
    .unwrap();
    let destination = directory.path().join("cancelled.ollsnap");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *EXPORT_ARCHIVE_TEST_HOOK.lock().unwrap() = Some(ExportArchiveTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });

    let export_runtime = Arc::clone(&runtime);
    let export_destination = destination.clone();
    let export = tokio::spawn(async move {
        export_runtime
            .export_snapshot(&export_destination, "cancelled-export-correlation")
            .await
    });
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();
    assert!(directory.path().read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".oll-snapshot-")
    }));

    export.abort();
    assert!(export.await.unwrap_err().is_cancelled());
    assert!(!destination.exists());
    release_tx.send(()).unwrap();
    for _ in 0..100 {
        let has_temporary = directory.path().read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".oll-snapshot-")
        });
        if !has_temporary {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!directory.path().read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".oll-snapshot-")
    }));

    runtime
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}
