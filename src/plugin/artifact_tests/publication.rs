use std::{
    fs,
    os::unix::fs::PermissionsExt,
    sync::{Arc, mpsc},
    time::Duration as StdDuration,
};

use sha2::{Digest, Sha256};

use crate::plugin::{
    ArtifactPublisher, JobState, MAX_ARTIFACT_CHUNK_BYTES, PluginError, RemovalIntent,
    RemovalPhase,
    artifact::{PublishTestHook, install_publish_test_hook},
};

use super::support::*;

#[tokio::test]
async fn a_durable_removal_intent_rejects_later_artifact_publication() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-after-removal").await;
    fixture
        .store
        .prepare_removal(&RemovalIntent {
            plugin_id: fixture.plugin_id.clone(),
            operation_id: "remove-artifact-plugin".to_owned(),
            plugins_lua_sha256: Sha256::digest(b"return {}").into(),
            prepared_plugins_lua: b"return {}\n".to_vec(),
            trash_path: fixture._directory.path().join("plugin-trash"),
            phase: RemovalPhase::Prepared,
            correlation_id: "correlation-remove-artifact-plugin".to_owned(),
        })
        .await
        .unwrap();

    let artifact_id = fixed_artifact_id(15);
    let bytes = b"must-not-publish";
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "blocked.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    assert!(matches!(
        session
            .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    assert!(!fixture.download_dir.join("blocked.bin").exists());
    assert!(staging_files(&fixture.download_dir).is_empty());
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn verifies_hash_and_projects_a_deterministic_collision_name() {
    let fixture = Fixture::new().await;
    fs::write(fixture.download_dir.join("report.pdf"), b"user-owned").unwrap();
    let job = fixture.job("artifact-collision-projection").await;
    let artifact_id = fixed_artifact_id(2);
    let bytes = b"verified-pdf";
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "report.pdf", bytes, 2),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, &bytes[..4]), &job.correlation_id)
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 1, &bytes[4..]), &job.correlation_id)
        .await
        .unwrap();
    let (_, stored) = session
        .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
        .await
        .unwrap();

    assert_eq!(
        fs::read(fixture.download_dir.join("report.pdf")).unwrap(),
        b"user-owned"
    );
    let expected = fixture
        .download_dir
        .join(format!("report.artifact-{artifact_id}.pdf"));
    assert_eq!(stored.destination, expected);
    assert_eq!(fs::read(expected).unwrap(), bytes);
    assert!(staging_files(&fixture.download_dir).is_empty());

    let hash_job = fixture.job("artifact-hash-mismatch").await;
    let hash_id = fixed_artifact_id(3);
    let mut bad = start(&hash_job, hash_id, "bad.bin", b"expected", 1);
    bad.artifact.as_mut().unwrap().sha256 = vec![7; 32];
    session
        .start_transfer(&bad, &hash_job.correlation_id)
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(hash_id, 0, b"expected"), &hash_job.correlation_id)
        .await
        .unwrap();
    assert!(matches!(
        session
            .complete_transfer(&complete(hash_id), &hash_job.correlation_id, fixture.now)
            .await,
        Err(PluginError::InvalidArgument(_))
    ));
    assert_eq!(
        fixture.store.get_job(hash_job.job_id).await.unwrap().state,
        JobState::Failed
    );
    assert!(!fixture.download_dir.join("bad.bin").exists());

    let size_job = fixture.job("artifact-size-mismatch").await;
    let size_id = fixed_artifact_id(8);
    let mut declared = start(&size_job, size_id, "short.bin", b"12345", 1);
    declared.artifact.as_mut().unwrap().size_bytes = 6;
    session
        .start_transfer(&declared, &size_job.correlation_id)
        .await
        .unwrap();
    assert!(matches!(
        session
            .receive_chunk(&chunk(size_id, 0, b"12345"), &size_job.correlation_id,)
            .await,
        Err(PluginError::InvalidArgument(_))
    ));
    assert_eq!(
        fixture.store.get_job(size_job.job_id).await.unwrap().state,
        JobState::Failed
    );
    assert!(!fixture.download_dir.join("short.bin").exists());
    fixture.pool.close().await;
}

#[tokio::test]
async fn publishes_multiple_mebibytes_across_bounded_chunks_with_exact_metadata() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-multi-chunk-publication").await;
    let artifact_id = fixed_artifact_id(16);
    let bytes = (0..(3 * MAX_ARTIFACT_CHUNK_BYTES + 12_345))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    let expected_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    let bounded_chunks = bytes.chunks(MAX_ARTIFACT_CHUNK_BYTES).collect::<Vec<_>>();
    assert!(bounded_chunks.len() > 3);
    assert!(
        bounded_chunks
            .iter()
            .all(|bytes| bytes.len() <= MAX_ARTIFACT_CHUNK_BYTES)
    );

    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(
                &job,
                artifact_id,
                "multi-mebibyte.bin",
                &bytes,
                u32::try_from(bounded_chunks.len()).unwrap(),
            ),
            &job.correlation_id,
        )
        .await
        .unwrap();
    for (index, bytes) in bounded_chunks.into_iter().enumerate() {
        session
            .receive_chunk(
                &chunk(artifact_id, u32::try_from(index).unwrap(), bytes),
                &job.correlation_id,
            )
            .await
            .unwrap();
    }
    let (response, stored) = session
        .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
        .await
        .unwrap();

    assert_eq!(response.artifact_id.unwrap().value, artifact_id.to_string());
    assert_eq!(stored.artifact_id, artifact_id);
    assert_eq!(stored.job_id, job.job_id);
    assert_eq!(stored.plugin_id, fixture.plugin_id);
    assert_eq!(stored.file_name, "multi-mebibyte.bin");
    assert_eq!(stored.media_type, "application/octet-stream");
    assert_eq!(stored.size_bytes, u64::try_from(bytes.len()).unwrap());
    assert_eq!(stored.sha256, expected_sha256);
    assert_eq!(
        stored.destination,
        fixture.download_dir.join("multi-mebibyte.bin")
    );
    assert_eq!(stored.stored_at, fixture.now);

    let projected = fs::read(&stored.destination).unwrap();
    assert_eq!(projected, bytes);
    let projected_sha256: [u8; 32] = Sha256::digest(&projected).into();
    assert_eq!(projected_sha256, expected_sha256);
    assert_eq!(
        fixture.store.get_artifact(artifact_id).await.unwrap(),
        stored
    );
    assert_eq!(
        fixture.store.artifacts_for_job(job.job_id).await.unwrap(),
        vec![stored]
    );
    assert!(staging_files(&fixture.download_dir).is_empty());
    fixture.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_no_replace_race_preserves_the_user_file_and_fails_publication() {
    let _hook_guard = lock_publish_hook().await;
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-no-replace-race").await;
    let artifact_id = fixed_artifact_id(4);
    let bytes = b"plugin-output";
    let destination = fixture.download_dir.join("raced.bin");
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "raced.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    install_publish_test_hook(PublishTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });
    let now = fixture.now;
    let correlation_id = job.correlation_id.clone();
    let task = tokio::spawn(async move {
        session
            .complete_transfer(&complete(artifact_id), &correlation_id, now)
            .await
    });
    tokio::task::spawn_blocking(move || {
        started_rx.recv_timeout(StdDuration::from_secs(5)).unwrap();
    })
    .await
    .unwrap();
    fs::write(&destination, b"racing-user-file").unwrap();
    release_tx.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(PluginError::AlreadyExists(_))
    ));
    assert_eq!(fs::read(&destination).unwrap(), b"racing-user-file");
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
    );
    fixture.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_content_no_replace_race_does_not_claim_the_user_file() {
    let _hook_guard = lock_publish_hook().await;
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-same-content-race").await;
    let artifact_id = fixed_artifact_id(13);
    let bytes = b"independently-created-content";
    let destination = fixture.download_dir.join("same.bin");
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "same.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    install_publish_test_hook(PublishTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });
    let now = fixture.now;
    let correlation_id = job.correlation_id.clone();
    let task = tokio::spawn(async move {
        session
            .complete_transfer(&complete(artifact_id), &correlation_id, now)
            .await
    });
    tokio::task::spawn_blocking(move || {
        started_rx.recv_timeout(StdDuration::from_secs(5)).unwrap();
    })
    .await
    .unwrap();
    fs::write(&destination, bytes).unwrap();
    release_tx.send(()).unwrap();

    assert!(matches!(
        task.await.unwrap(),
        Err(PluginError::AlreadyExists(_))
    ));
    assert_eq!(fs::read(&destination).unwrap(), bytes);
    assert!(matches!(
        fixture.store.get_artifact(artifact_id).await,
        Err(PluginError::NotFound(_))
    ));
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
    );
    fixture.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_publication_outlives_a_cancelled_session_observer() {
    let _hook_guard = lock_publish_hook().await;
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-cancelled-observer").await;
    let artifact_id = fixed_artifact_id(16);
    let bytes = b"publication-must-finish";
    let destination = fixture.download_dir.join("durable.bin");
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "durable.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    install_publish_test_hook(PublishTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });
    let now = fixture.now;
    let correlation_id = job.correlation_id.clone();
    let observer = tokio::spawn(async move {
        session
            .complete_transfer(&complete(artifact_id), &correlation_id, now)
            .await
    });
    tokio::task::spawn_blocking(move || {
        started_rx.recv_timeout(StdDuration::from_secs(5)).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .len(),
        1
    );

    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    fixture
        .publisher
        .settle_plugin_publications(&fixture.plugin_id)
        .await
        .unwrap();

    assert_eq!(fs::read(destination).unwrap(), bytes);
    assert_eq!(
        fixture
            .store
            .get_artifact(artifact_id)
            .await
            .unwrap()
            .job_id,
        job.job_id
    );
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Running
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn removal_settles_a_transiently_failed_durable_publication() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-transient-publication-failure").await;
    let artifact_id = fixed_artifact_id(18);
    let bytes = b"recover-before-removal";
    let destination = fixture.download_dir.join("recover-before-remove.bin");
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "recover-before-remove.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    fs::set_permissions(&fixture.download_dir, fs::Permissions::from_mode(0o500)).unwrap();
    let publication = session
        .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
        .await;
    fs::set_permissions(&fixture.download_dir, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(publication, Err(PluginError::Io { .. })));
    assert_eq!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(!destination.exists());

    fixture
        .store
        .prepare_removal(&RemovalIntent {
            plugin_id: fixture.plugin_id.clone(),
            operation_id: "remove-after-artifact-recovery".to_owned(),
            plugins_lua_sha256: Sha256::digest(b"return {}").into(),
            prepared_plugins_lua: b"return {}\n".to_vec(),
            trash_path: fixture
                ._directory
                .path()
                .join("plugin-trash-after-recovery"),
            phase: RemovalPhase::Prepared,
            correlation_id: "correlation-remove-after-artifact-recovery".to_owned(),
        })
        .await
        .unwrap();
    fixture
        .publisher
        .settle_plugin_publications(&fixture.plugin_id)
        .await
        .unwrap();

    assert_eq!(fs::read(destination).unwrap(), bytes);
    assert_eq!(
        fixture
            .store
            .get_artifact(artifact_id)
            .await
            .unwrap()
            .job_id,
        job.job_id
    );
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    fixture.pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_aborts_and_reaps_the_continuation_but_retains_its_intent() {
    let _hook_guard = lock_publish_hook().await;
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-shutdown-deadline").await;
    let artifact_id = fixed_artifact_id(19);
    let bytes = b"recover-after-shutdown";
    let destination = fixture.download_dir.join("shutdown-recovery.bin");
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "shutdown-recovery.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    install_publish_test_hook(PublishTestHook {
        destination: destination.clone(),
        started: started_tx,
        release: release_rx,
    });
    let now = fixture.now;
    let correlation_id = job.correlation_id.clone();
    let observer = tokio::spawn(async move {
        session
            .complete_transfer(&complete(artifact_id), &correlation_id, now)
            .await
    });
    tokio::task::spawn_blocking(move || {
        started_rx.recv_timeout(StdDuration::from_secs(5)).unwrap();
    })
    .await
    .unwrap();

    assert!(
        fixture
            .publisher
            .shutdown_publications(
                tokio::time::Instant::now() + StdDuration::from_millis(50),
                "artifact-shutdown-correlation",
            )
            .await
            .is_err()
    );
    assert!(matches!(
        observer.await.unwrap(),
        Err(PluginError::Store(_))
    ));
    assert_eq!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        fixture.store.get_artifact(artifact_id).await,
        Err(PluginError::NotFound(_))
    ));

    release_tx.send(()).unwrap();
    for _ in 0..100 {
        if destination.exists() {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    assert_eq!(fs::read(&destination).unwrap(), bytes);
    let (_, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &fixture.download_dir,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-restart",
    )
    .await
    .unwrap();
    assert_eq!(report.recovered, 1);
    assert!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .store
            .get_artifact(artifact_id)
            .await
            .unwrap()
            .destination,
        destination
    );
    fixture.pool.close().await;
}
