use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

use uuid::Uuid;

use crate::plugin::{
    ArtifactPublisher, ArtifactSession, JobState, MAX_ARTIFACT_CHUNK_BYTES, PluginError,
};

use super::support::*;

#[tokio::test]
async fn startup_creates_private_directory_and_caches_its_resolved_path() {
    fn assert_send<T: Send>() {}
    assert_send::<ArtifactSession>();

    let fixture = Fixture::new().await;
    assert!(fixture.publisher.download_dir().is_absolute());
    assert_eq!(
        fixture.publisher.maximum_chunk_bytes(),
        MAX_ARTIFACT_CHUNK_BYTES
    );
    assert_eq!(
        fs::metadata(&fixture.download_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fixture.store.artifact_download_dir().await.unwrap(),
        Some(fs::canonicalize(&fixture.download_dir).unwrap())
    );
    fixture.pool.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn startup_does_not_follow_a_replaced_cached_directory_into_user_files() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    let moved_old_root = fixture._directory.path().join("moved-old-downloads");
    let unrelated_root = fixture._directory.path().join("unrelated");
    let new_root = fixture._directory.path().join("new-downloads");
    fs::rename(&fixture.download_dir, &moved_old_root).unwrap();
    fs::create_dir(&unrelated_root).unwrap();
    let user_file = unrelated_root.join(format!(
        ".oll-artifact-{}-{}.part",
        fixed_artifact_id(12),
        Uuid::new_v4()
    ));
    fs::write(&user_file, b"not-owned-by-oll").unwrap();
    symlink(&unrelated_root, &fixture.download_dir).unwrap();

    let _ = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &new_root,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-new-root",
    )
    .await
    .unwrap();

    assert_eq!(fs::read(user_file).unwrap(), b"not-owned-by-oll");
    fixture.pool.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_does_not_follow_a_replaced_cached_root_for_a_durable_intent() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-replaced-recovery-root").await;
    let artifact_id = fixed_artifact_id(14);
    let bytes = b"user-file-with-matching-bytes";
    let staging_name = format!(".oll-artifact-{artifact_id}-{}.part", Uuid::new_v4());
    let staging = fixture.download_dir.join(&staging_name);
    let destination = fixture.download_dir.join("published.bin");
    fs::write(&staging, bytes).unwrap();
    let publish_intent = intent(
        &fixture,
        &job,
        artifact_id,
        "published.bin",
        bytes,
        staging,
        destination,
    );
    fixture
        .store
        .prepare_artifact_publish(&publish_intent)
        .await
        .unwrap();

    let moved_old_root = fixture._directory.path().join("moved-old-downloads");
    let unrelated_root = fixture._directory.path().join("unrelated");
    let new_root = fixture._directory.path().join("new-downloads");
    fs::rename(&fixture.download_dir, &moved_old_root).unwrap();
    fs::create_dir(&unrelated_root).unwrap();
    let unrelated_staging = unrelated_root.join(&staging_name);
    let unrelated_destination = unrelated_root.join("published.bin");
    fs::write(&unrelated_staging, bytes).unwrap();
    fs::write(&unrelated_destination, bytes).unwrap();
    symlink(&unrelated_root, &fixture.download_dir).unwrap();

    let (_, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &new_root,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-replaced-root",
    )
    .await
    .unwrap();

    assert_eq!(report.recovered, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(fs::read(&unrelated_staging).unwrap(), bytes);
    assert_eq!(fs::read(&unrelated_destination).unwrap(), bytes);
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

#[cfg(unix)]
#[tokio::test]
async fn an_active_transfer_does_not_follow_a_replaced_download_root() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-replaced-live-root").await;
    let artifact_id = fixed_artifact_id(20);
    let bytes = b"must-stay-in-original-root";
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "must-not-publish.bin", bytes, 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();
    let staging_name = staging_files(&fixture.download_dir)[0]
        .file_name()
        .unwrap()
        .to_owned();

    let moved_root = fixture._directory.path().join("moved-live-downloads");
    let unrelated_root = fixture._directory.path().join("unrelated-live-root");
    fs::rename(&fixture.download_dir, &moved_root).unwrap();
    fs::create_dir(&unrelated_root).unwrap();
    let unrelated_staging = unrelated_root.join(staging_name);
    fs::write(&unrelated_staging, bytes).unwrap();
    symlink(&unrelated_root, &fixture.download_dir).unwrap();

    assert!(matches!(
        session
            .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    assert_eq!(fs::read(&unrelated_staging).unwrap(), bytes);
    assert!(!unrelated_root.join("must-not-publish.bin").exists());
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
