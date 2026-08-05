use std::{fs, io, time::Duration};

use uuid::Uuid;

use crate::plugin::{JobState, PluginError};

use super::support::*;

#[tokio::test]
async fn rejects_traversal_and_fails_only_the_job_with_an_invalid_chunk_stream() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-invalid-stream").await;
    let artifact_id = fixed_artifact_id(1);
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    for file_name in ["", ".", "..", "../secret", "/tmp/secret", "nul\0name"] {
        let request = start(&job, artifact_id, file_name, b"abc", 1);
        assert!(matches!(
            session.start_transfer(&request, &job.correlation_id).await,
            Err(PluginError::InvalidArgument(_))
        ));
    }

    session
        .start_transfer(
            &start(&job, artifact_id, "safe.bin", b"abc", 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    let error = session
        .receive_chunk(&chunk(artifact_id, 1, b"abc"), &job.correlation_id)
        .await
        .unwrap_err();
    assert!(matches!(error, PluginError::InvalidArgument(_)));
    let failed = fixture.store.get_job(job.job_id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.error_code.as_deref(), Some("artifact_chunk_invalid"));
    assert_eq!(
        failed.error_message.as_deref(),
        Some("artifact chunks must have contiguous zero-based indexes")
    );
    assert!(staging_files(&fixture.download_dir).is_empty());
    fixture.pool.close().await;
}

#[tokio::test]
async fn internal_artifact_failure_is_redacted_before_job_persistence_and_logging() {
    let fixture = Fixture::new().await;
    let secret = "postgresql://alice:super-secret@example.invalid/db /home/alice/private-token";
    let failures = [
        (
            22,
            "artifact-internal-io-redaction",
            "artifact_staging_write_failed",
            PluginError::io("write artifact staging file", io::Error::other(secret)),
            "artifact staging write failed",
        ),
        (
            23,
            "artifact-internal-store-redaction",
            "artifact_publish_intent_failed",
            PluginError::Store(format!("SQL backend exposed {secret}")),
            "artifact publication intent could not be persisted",
        ),
        (
            24,
            "artifact-untrusted-validation-redaction",
            "artifact_validation_failed",
            PluginError::InvalidArgument(format!("unrecognized validation detail: {secret}")),
            "artifact staging validation failed",
        ),
    ];
    for (number, operation, code, internal, expected_message) in failures {
        let job = fixture.job(operation).await;
        let artifact_id = fixed_artifact_id(number);
        let mut session = fixture
            .publisher
            .session(fixture.plugin_id.clone(), fixture.instance_id);
        session
            .start_transfer(
                &start(&job, artifact_id, "redacted.bin", b"content", 1),
                &job.correlation_id,
            )
            .await
            .unwrap();
        session
            .fail_transfer_for_test(artifact_id, code, &internal)
            .await
            .unwrap();

        let failed = fixture.store.get_job(job.job_id).await.unwrap();
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.error_code.as_deref(), Some(code));
        assert_eq!(failed.error_message.as_deref(), Some(expected_message));
        // GetPluginJob derives its visible error from this durable row. Keep
        // the boundary safe even if an encoder forwards it verbatim.
        assert!(!failed.error_message.as_deref().unwrap().contains(secret));
    }
    assert!(staging_files(&fixture.download_dir).is_empty());

    fixture
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    let log = fs::read_to_string(fixture._directory.path().join("logs/oll.log")).unwrap();
    assert!(log.contains("plugin_artifact_transfer_failed"));
    assert!(log.contains("artifact_staging_write_failed"));
    assert!(log.contains("artifact_publish_intent_failed"));
    assert!(!log.contains(secret));
    fixture.pool.close().await;
}

#[tokio::test]
async fn every_artifact_message_must_retain_the_owning_job_correlation() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-correlation").await;
    let store_artifact_id = fixed_artifact_id(21);
    let store_staging = fixture.download_dir.join(format!(
        ".oll-artifact-{store_artifact_id}-{}.part",
        Uuid::new_v4()
    ));
    fs::write(&store_staging, b"store-correlation").unwrap();
    let mut mismatched_intent = intent(
        &fixture,
        &job,
        store_artifact_id,
        "store-correlation.bin",
        b"store-correlation",
        store_staging.clone(),
        fixture.download_dir.join("store-correlation.bin"),
    );
    mismatched_intent.correlation_id = "wrong-correlation".to_owned();
    assert!(matches!(
        fixture
            .store
            .prepare_artifact_publish(&mismatched_intent)
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    fs::remove_file(store_staging).unwrap();

    let artifact_id = fixed_artifact_id(17);
    let bytes = b"correlated-output";
    let request = start(&job, artifact_id, "correlated.bin", bytes, 1);
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);

    assert!(matches!(
        session.start_transfer(&request, "wrong-correlation").await,
        Err(PluginError::FailedPrecondition(_))
    ));
    assert!(staging_files(&fixture.download_dir).is_empty());
    session
        .start_transfer(&request, &job.correlation_id)
        .await
        .unwrap();
    assert!(matches!(
        session
            .receive_chunk(&chunk(artifact_id, 0, bytes), "wrong-correlation")
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    session
        .receive_chunk(&chunk(artifact_id, 0, bytes), &job.correlation_id)
        .await
        .unwrap();
    assert!(matches!(
        session
            .complete_transfer(&complete(artifact_id), "wrong-correlation", fixture.now,)
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    session
        .complete_transfer(&complete(artifact_id), &job.correlation_id, fixture.now)
        .await
        .unwrap();

    assert_eq!(
        fixture
            .store
            .get_artifact(artifact_id)
            .await
            .unwrap()
            .job_id,
        job.job_id
    );
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Running
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn artifact_ids_are_claimed_across_all_live_sessions() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-live-id-claim").await;
    let artifact_id = fixed_artifact_id(9);
    let request = start(&job, artifact_id, "claimed.bin", b"abc", 1);
    let mut first = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    let mut second = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);

    first
        .start_transfer(&request, &job.correlation_id)
        .await
        .unwrap();
    assert!(matches!(
        second.start_transfer(&request, &job.correlation_id).await,
        Err(PluginError::AlreadyExists(_))
    ));
    first
        .abort_all("artifact_session_ended", fixture.now)
        .await
        .unwrap();
    assert_eq!(
        fixture.store.get_job(job.job_id).await.unwrap().state,
        JobState::Failed
    );
    assert!(staging_files(&fixture.download_dir).is_empty());
    fixture.pool.close().await;
}

#[tokio::test]
async fn session_abort_removes_interrupted_staging_and_fails_its_job() {
    let fixture = Fixture::new().await;
    let job = fixture.job("artifact-session-abort").await;
    let artifact_id = fixed_artifact_id(7);
    let mut session = fixture
        .publisher
        .session(fixture.plugin_id.clone(), fixture.instance_id);
    session
        .start_transfer(
            &start(&job, artifact_id, "partial.bin", b"partial", 1),
            &job.correlation_id,
        )
        .await
        .unwrap();
    session
        .abort_all("plugin_session_ended", fixture.now)
        .await
        .unwrap();
    assert!(staging_files(&fixture.download_dir).is_empty());
    let failed = fixture.store.get_job(job.job_id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.error_code.as_deref(), Some("plugin_session_ended"));
    fixture.pool.close().await;
}
