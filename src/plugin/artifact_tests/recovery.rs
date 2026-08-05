use std::{fs, sync::Arc};

use uuid::Uuid;

use crate::plugin::{ArtifactPublisher, JobState, MAX_ARTIFACT_CHUNK_BYTES};

use super::support::*;

#[tokio::test]
async fn startup_recovers_matching_staging_before_general_job_failure() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-recovery-stage").await;
    let artifact_id = fixed_artifact_id(5);
    let bytes = b"recoverable-output";
    let staging = fixture.download_dir.join(format!(
        ".oll-artifact-{artifact_id}-{}.part",
        Uuid::new_v4()
    ));
    fs::write(&staging, bytes).unwrap();
    let destination = fixture.download_dir.join("recovered.bin");
    let recovery_intent = intent(
        &fixture,
        &job,
        artifact_id,
        "recovered.bin",
        bytes,
        staging.clone(),
        destination.clone(),
    );
    fixture
        .store
        .prepare_artifact_publish(&recovery_intent)
        .await
        .unwrap();
    let published_job = fixture.job("artifact-recovery-published").await;
    let published_id = fixed_artifact_id(9);
    let published_bytes = b"already-published-output";
    let published_destination = fixture.download_dir.join("already-published.bin");
    fs::write(&published_destination, published_bytes).unwrap();
    let missing_staging = fixture.download_dir.join(format!(
        ".oll-artifact-{published_id}-{}.part",
        Uuid::new_v4()
    ));
    let published_intent = intent(
        &fixture,
        &published_job,
        published_id,
        "already-published.bin",
        published_bytes,
        missing_staging,
        published_destination.clone(),
    );
    fixture
        .store
        .prepare_artifact_publish(&published_intent)
        .await
        .unwrap();
    let orphan = fixture.download_dir.join(format!(
        ".oll-artifact-{}-{}.part",
        fixed_artifact_id(10),
        Uuid::new_v4()
    ));
    fs::write(&orphan, b"interrupted-before-intent").unwrap();

    let (restarted, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &fixture.download_dir,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-recovery",
    )
    .await
    .unwrap();
    assert_eq!(report.recovered, 2);
    assert_eq!(report.failed, 0);
    assert_eq!(fs::read(&destination).unwrap(), bytes);
    assert!(!staging.exists());
    assert!(!orphan.exists());
    assert_eq!(
        fixture
            .store
            .get_artifact(artifact_id)
            .await
            .unwrap()
            .destination,
        destination
    );
    assert_eq!(
        fixture
            .store
            .get_artifact(published_id)
            .await
            .unwrap()
            .destination,
        published_destination
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Running
    );
    fixture
        .store
        .fail_nonterminal_jobs_on_startup(fixture.now)
        .await
        .unwrap();
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
    );
    drop(restarted);
    fixture.pool.close().await;
}

#[tokio::test]
async fn contradictory_recovery_never_overwrites_and_fails_the_owning_job() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-recovery-conflict").await;
    let artifact_id = fixed_artifact_id(6);
    let bytes = b"staged-output";
    let staging = fixture.download_dir.join(format!(
        ".oll-artifact-{artifact_id}-{}.part",
        Uuid::new_v4()
    ));
    fs::write(&staging, bytes).unwrap();
    let destination = fixture.download_dir.join("conflict.bin");
    fs::write(&destination, b"user-file").unwrap();
    let intent = intent(
        &fixture,
        &job,
        artifact_id,
        "conflict.bin",
        bytes,
        staging.clone(),
        destination.clone(),
    );
    fixture
        .store
        .prepare_artifact_publish(&intent)
        .await
        .unwrap();

    let (_, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &fixture.download_dir,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-contradictory-recovery",
    )
    .await
    .unwrap();
    assert_eq!(report.recovered, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(fs::read(destination).unwrap(), b"user-file");
    assert!(!staging.exists());
    let failed = fixture.store.get_job(job.job_id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(
        failed.error_code.as_deref(),
        Some("artifact_recovery_failed")
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

#[tokio::test]
async fn recovery_never_deletes_a_staging_path_it_does_not_own() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-recovery-invalid-staging").await;
    let artifact_id = fixed_artifact_id(11);
    let bytes = b"user-owned-file";
    let unowned_staging = fixture.download_dir.join("ordinary-user-file.bin");
    fs::write(&unowned_staging, bytes).unwrap();
    let intent = intent(
        &fixture,
        &job,
        artifact_id,
        "published.bin",
        bytes,
        unowned_staging.clone(),
        fixture.download_dir.join("published.bin"),
    );
    fixture
        .store
        .prepare_artifact_publish(&intent)
        .await
        .unwrap();

    let (_, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &fixture.download_dir,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-unowned-recovery",
    )
    .await
    .unwrap();

    assert_eq!(report.recovered, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(fs::read(&unowned_staging).unwrap(), bytes);
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
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

#[cfg(unix)]
#[tokio::test]
async fn recovery_keeps_its_durable_intent_when_root_revalidation_errors() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    let unstable_parent = fixture._directory.path().join("unstable");
    let unstable_downloads = unstable_parent.join("downloads");
    fs::create_dir_all(&unstable_downloads).unwrap();
    let (publisher, report) = ArtifactPublisher::initialize(
        fixture.store.clone(),
        &unstable_downloads,
        MAX_ARTIFACT_CHUNK_BYTES,
        Arc::clone(&fixture.logger),
        fixture.now,
        "artifact-test-unstable-root",
    )
    .await
    .unwrap();
    assert_eq!(report.recovered, 0);
    assert_eq!(report.failed, 0);

    let job = fixture.job("artifact-recovery-root-error").await;
    let artifact_id = fixed_artifact_id(21);
    let bytes = b"recover-after-root-error";
    let staging = unstable_downloads.join(format!(
        ".oll-artifact-{artifact_id}-{}.part",
        Uuid::new_v4()
    ));
    fs::write(&staging, bytes).unwrap();
    let publish_intent = intent(
        &fixture,
        &job,
        artifact_id,
        "root-error.bin",
        bytes,
        staging,
        unstable_downloads.join("root-error.bin"),
    );
    fixture
        .store
        .prepare_artifact_publish(&publish_intent)
        .await
        .unwrap();

    let moved_parent = fixture._directory.path().join("unstable-moved");
    fs::rename(&unstable_parent, &moved_parent).unwrap();
    symlink(&unstable_parent, &unstable_parent).unwrap();

    assert!(
        publisher
            .settle_plugin_publications(&fixture.plugin_id)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Running
    );
    assert_eq!(
        fixture
            .store
            .artifact_publish_intents()
            .await
            .unwrap()
            .len(),
        1
    );

    fs::remove_file(&unstable_parent).unwrap();
    fixture.pool.close().await;
}
