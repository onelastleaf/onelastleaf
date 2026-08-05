use std::{fs, path::Path};

use sha2::{Digest, Sha256};
use sqlx::{AnyPool, any::AnyPoolOptions};
use tempfile::TempDir;
use time::OffsetDateTime;
use url::Url;
use uuid::Uuid;

use super::PluginStore;
use crate::plugin::{
    ArtifactPublishIntent, DesiredPluginState, InstallMode, JobAdmission, JobCancellationReason,
    JobState, NormalizedJobPayload, PackagePublishIntent, PluginArtifactId, PluginError, PluginId,
    PluginInstanceId, PluginOperationId, PluginSelector, RemovalIntent, RemovalPhase,
};

async fn sqlite_pool(path: &Path) -> AnyPool {
    sqlite_pool_with_connections(path, 1).await
}

async fn sqlite_pool_with_connections(path: &Path, max_connections: u32) -> AnyPool {
    sqlx::any::install_default_drivers();
    fs::File::create(path).unwrap();
    let url = Url::from_file_path(path)
        .unwrap()
        .as_str()
        .replacen("file:", "sqlite:", 1);
    AnyPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .unwrap()
}

fn new_package(id: &str, name: &str, generation: Uuid) -> PackagePublishIntent {
    PackagePublishIntent {
        plugin_id: id.parse().unwrap(),
        plugin_name: name.parse().unwrap(),
        operation_id: format!("install-{id}-{generation}"),
        expected_current_generation: None,
        candidate_generation: generation,
        normalized_declaration: format!("declaration:{id}").into_bytes(),
        declaration_sha256: Sha256::digest(format!("declaration:{id}")).into(),
        effective_manifest: format!("manifest:{id}").into_bytes(),
        selected_commit: Some("0123456789abcdef".to_owned()),
        install_mode: InstallMode::Source,
        release_id: None,
        correlation_id: format!("correlation-{generation}"),
    }
}

async fn install(
    store: &PluginStore,
    package: &PackagePublishIntent,
) -> crate::plugin::InstalledPlugin {
    store.prepare_package_publish(package).await.unwrap();
    store
        .finalize_package_publish(&package.plugin_id, package.candidate_generation)
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_schema_is_idempotent_and_desired_state_survives_reopen() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("plugin.sqlite3");
    let pool = sqlite_pool(&path).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.alpha", "alpha", Uuid::new_v4());
    let installed = install(&store, &plugin).await;
    assert_eq!(installed.desired_state, DesiredPluginState::Stopped);
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    let restarted = store.request_restart(&plugin.plugin_id).await.unwrap();
    assert_eq!(restarted.restart_sequence, 1);
    drop(store);
    pool.close().await;

    let pool = sqlite_pool_existing(&path).await;
    let reopened = PluginStore::initialize(pool.clone()).await.unwrap();
    let loaded = reopened
        .get_plugin(&PluginSelector::Id(plugin.plugin_id))
        .await
        .unwrap();
    assert_eq!(loaded.desired_state, DesiredPluginState::Running);
    assert_eq!(loaded.restart_sequence, 1);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_artifact_download_cache_rejects_a_missing_metadata_singleton() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    sqlx::query("DELETE FROM plugin_meta WHERE singleton = 1")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store
            .cache_artifact_download_dir(&directory.path().join("downloads"))
            .await,
        Err(PluginError::CorruptStore(_))
    ));
}

#[tokio::test]
async fn sqlite_retains_the_prior_failure_until_the_exact_instance_is_ready() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.ready-state", "ready-state", Uuid::new_v4());
    install(&store, &plugin).await;
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE plugins SET restart_attempt = 3,
             restart_not_before_seconds = 1800000000,
             restart_not_before_nanos = 0,
             last_lifecycle_failure = 'prior_start_failed'
         WHERE plugin_id = $1",
    )
    .bind(plugin.plugin_id.as_str())
    .execute(&pool)
    .await
    .unwrap();

    let instance = PluginInstanceId::new();
    store
        .record_running_instance(&plugin.plugin_id, plugin.candidate_generation, instance)
        .await
        .unwrap();
    let starting = store
        .get_plugin(&PluginSelector::Id(plugin.plugin_id.clone()))
        .await
        .unwrap();
    assert_eq!(
        starting.last_lifecycle_failure.as_deref(),
        Some("prior_start_failed")
    );
    assert_eq!(starting.restart_attempt, 3);

    assert!(matches!(
        store
            .record_instance_ready(&plugin.plugin_id, PluginInstanceId::new())
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    store
        .record_instance_ready(&plugin.plugin_id, instance)
        .await
        .unwrap();
    let ready = store
        .get_plugin(&PluginSelector::Id(plugin.plugin_id))
        .await
        .unwrap();
    assert_eq!(ready.restart_attempt, 0);
    assert!(ready.restart_not_before.is_none());
    assert!(ready.last_lifecycle_failure.is_none());
}

#[tokio::test]
async fn sqlite_response_parsing_failure_rolls_back_plugin_mutations() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    let generation = Uuid::new_v4();
    let plugin = new_package("oll.rollback", "rollback", generation);
    install(&store, &plugin).await;
    sqlx::query("UPDATE plugins SET restart_attempt = -1 WHERE plugin_id = $1")
        .bind(plugin.plugin_id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store
            .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT desired_state FROM plugins WHERE plugin_id = $1")
            .bind(plugin.plugin_id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "stopped",
    );

    assert!(matches!(
        store.request_restart(&plugin.plugin_id).await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT restart_sequence FROM plugins WHERE plugin_id = $1")
            .bind(plugin.plugin_id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
    );

    let candidate = Uuid::new_v4();
    let mut replacement = plugin.clone();
    replacement.operation_id = "rollback-publication".to_owned();
    replacement.expected_current_generation = Some(generation);
    replacement.candidate_generation = candidate;
    replacement.effective_manifest = b"replacement".to_vec();
    store.prepare_package_publish(&replacement).await.unwrap();
    assert!(matches!(
        store
            .finalize_package_publish(&plugin.plugin_id, candidate)
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT current_generation FROM plugins WHERE plugin_id = $1"
        )
        .bind(plugin.plugin_id.as_str())
        .fetch_one(&pool)
        .await
        .unwrap(),
        generation.to_string(),
    );
    assert!(
        store
            .package_publish_intent(&plugin.plugin_id)
            .await
            .unwrap()
            .is_some(),
        "failed response parsing must retain the publication intent"
    );
}

#[tokio::test]
async fn sqlite_response_parsing_failure_rolls_back_job_and_artifact_mutations() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.job-rollback", "job-rollback", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    store
        .record_running_instance(&plugin.plugin_id, plugin.candidate_generation, instance)
        .await
        .unwrap();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let payload = NormalizedJobPayload::new(
        plugin.plugin_id.clone(),
        "render".to_owned(),
        Vec::new(),
        None,
    )
    .unwrap();
    let corrupted_operation: PluginOperationId = "corrupt-insert-response".parse().unwrap();
    sqlx::query(
        "CREATE TRIGGER corrupt_inserted_job AFTER INSERT ON plugin_jobs
         BEGIN
           UPDATE plugin_jobs SET action = '' WHERE job_id = NEW.job_id;
         END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store
            .admit_job(
                &corrupted_operation,
                &payload,
                instance,
                admitted_at,
                "corrupt-insert-response",
            )
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM plugin_jobs WHERE operation_id = $1")
            .bind(corrupted_operation.as_str())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
    );
    sqlx::query("DROP TRIGGER corrupt_inserted_job")
        .execute(&pool)
        .await
        .unwrap();

    let operation: PluginOperationId = "corrupt-transition-response".parse().unwrap();
    let JobAdmission::Created(job) = store
        .admit_job(
            &operation,
            &payload,
            instance,
            admitted_at,
            "corrupt-transition-response",
        )
        .await
        .unwrap()
    else {
        panic!("job admission must create a row")
    };
    sqlx::query("UPDATE plugin_jobs SET action = '' WHERE job_id = $1")
        .bind(job.job_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .mark_job_accepted(job.job_id, instance, admitted_at)
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(job_state(&pool, job.job_id).await, "dispatching");

    sqlx::query("UPDATE plugin_jobs SET action = 'render' WHERE job_id = $1")
        .bind(job.job_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    store
        .mark_job_accepted(job.job_id, instance, admitted_at)
        .await
        .unwrap();
    sqlx::query("UPDATE plugin_jobs SET action = '' WHERE job_id = $1")
        .bind(job.job_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .finish_job(
                job.job_id,
                instance,
                JobState::Succeeded,
                None,
                None,
                None,
                admitted_at,
            )
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(job_state(&pool, job.job_id).await, "running");

    let artifact_id = PluginArtifactId::new();
    let intent = ArtifactPublishIntent {
        artifact_id,
        job_id: job.job_id,
        plugin_id: plugin.plugin_id,
        file_name: "result.bin".to_owned(),
        media_type: "application/octet-stream".to_owned(),
        size_bytes: 4,
        sha256: [7; 32],
        staging_path: directory.path().join("staging.bin"),
        destination: directory.path().join("result.bin"),
        correlation_id: job.correlation_id,
    };
    store.prepare_artifact_publish(&intent).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER corrupt_inserted_artifact AFTER INSERT ON plugin_artifacts
         BEGIN
           UPDATE plugin_artifacts SET size_bytes = 'invalid' WHERE artifact_id = NEW.artifact_id;
         END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store
            .finalize_artifact_publish(artifact_id, admitted_at)
            .await,
        Err(PluginError::CorruptStore(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM plugin_artifacts WHERE artifact_id = $1"
        )
        .bind(artifact_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
    );
    assert!(
        store
            .artifact_publish_intent(artifact_id)
            .await
            .unwrap()
            .is_some(),
        "failed response parsing must retain the artifact intent"
    );
}

async fn job_state(pool: &AnyPool, job_id: crate::plugin::PluginJobId) -> String {
    sqlx::query_scalar("SELECT state FROM plugin_jobs WHERE job_id = $1")
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_migrates_existing_job_tables_without_inventing_timestamps() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("plugin.sqlite3");
    let pool = sqlite_pool(&path).await;
    for statement in super::schema::SCHEMA {
        let legacy = statement
            .replace("        accepted_at_seconds BIGINT,\n", "")
            .replace("        accepted_at_nanos BIGINT,\n", "")
            .replace("        terminal_at_seconds BIGINT,\n", "")
            .replace("        terminal_at_nanos BIGINT,\n", "");
        sqlx::query(sqlx::AssertSqlSafe(legacy))
            .execute(&pool)
            .await
            .unwrap();
    }

    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.migrated", "migrated", Uuid::new_v4());
    install(&store, &plugin).await;
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let job = admit(
        &store,
        &plugin.plugin_id,
        "migrated-job",
        PluginInstanceId::new(),
        admitted_at,
    )
    .await;
    assert_eq!(job.accepted_at, None);
    assert_eq!(job.terminal_at, None);
    pool.close().await;
}

async fn sqlite_pool_existing(path: &Path) -> AnyPool {
    sqlx::any::install_default_drivers();
    let url = Url::from_file_path(path)
        .unwrap()
        .as_str()
        .replacen("file:", "sqlite:", 1);
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap()
}

#[tokio::test]
async fn sqlite_enforces_name_uniqueness_and_preserves_running_old_generation() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let old_generation = Uuid::new_v4();
    let plugin = new_package("oll.alpha", "alpha", old_generation);
    install(&store, &plugin).await;
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    let conflicting = new_package("oll.beta", "alpha", Uuid::new_v4());
    assert!(matches!(
        store.prepare_package_publish(&conflicting).await,
        Err(PluginError::AlreadyExists(_))
    ));

    let instance = PluginInstanceId::new();
    store
        .record_running_instance(&plugin.plugin_id, old_generation, instance)
        .await
        .unwrap();
    let candidate = Uuid::new_v4();
    let mut intent = plugin.clone();
    intent.operation_id = "publish-1".to_owned();
    intent.expected_current_generation = Some(old_generation);
    intent.candidate_generation = candidate;
    intent.effective_manifest = b"new manifest".to_vec();
    intent.correlation_id = "corr-publish".to_owned();
    store.prepare_package_publish(&intent).await.unwrap();
    let published = store
        .finalize_package_publish(&plugin.plugin_id, candidate)
        .await
        .unwrap();
    assert_eq!(published.current_generation, candidate);
    assert_eq!(published.running_generation, Some(old_generation));
    assert_eq!(published.running_instance_id, Some(instance));
    assert!(store.package_publish_intents().await.unwrap().is_empty());
}

#[tokio::test]
async fn sqlite_job_admission_is_deployment_global_and_payload_idempotent() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.alpha", "alpha", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    store
        .record_running_instance(&plugin.plugin_id, plugin.candidate_generation, instance)
        .await
        .unwrap();
    let operation: PluginOperationId = "operation-1".parse().unwrap();
    let payload = NormalizedJobPayload::new(
        plugin.plugin_id.clone(),
        "render".to_owned(),
        vec!["".to_owned(), "-x".to_owned(), "-x".to_owned()],
        None,
    )
    .unwrap();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let created = store
        .admit_job(&operation, &payload, instance, admitted_at, "corr-job")
        .await
        .unwrap();
    let JobAdmission::Created(created) = created else {
        panic!("first admission did not create a job")
    };
    store
        .settle_ended_instance(
            &plugin.plugin_id,
            instance,
            None,
            admitted_at + time::Duration::seconds(1),
            "test_instance_ended",
        )
        .await
        .unwrap();
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Stopped)
        .await
        .unwrap();
    let retried = store
        .admit_job(
            &operation,
            &payload,
            PluginInstanceId::new(),
            admitted_at + time::Duration::seconds(10),
            "another-correlation",
        )
        .await
        .unwrap();
    let JobAdmission::Existing(retried) = retried else {
        panic!("retry did not return the retained job")
    };
    assert_eq!(created.job_id, retried.job_id);
    assert_eq!(created.absolute_deadline, retried.absolute_deadline);

    let changed = NormalizedJobPayload::new(
        plugin.plugin_id,
        "render".to_owned(),
        vec!["different".to_owned()],
        None,
    )
    .unwrap();
    assert!(matches!(
        store
            .admit_job(&operation, &changed, instance, admitted_at, "corr-job")
            .await,
        Err(PluginError::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn sqlite_job_admission_requires_the_expected_running_instance() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.stopped", "stopped", Uuid::new_v4());
    install(&store, &plugin).await;
    let operation: PluginOperationId = "stopped-operation".parse().unwrap();
    let payload =
        NormalizedJobPayload::new(plugin.plugin_id, "render".to_owned(), Vec::new(), None).unwrap();

    assert!(matches!(
        store
            .admit_job(
                &operation,
                &payload,
                PluginInstanceId::new(),
                OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap(),
                "stopped-operation",
            )
            .await,
        Err(PluginError::FailedPrecondition(_))
    ));
    assert!(matches!(
        store.job_by_operation_id(&operation).await,
        Err(PluginError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_admits_distinct_jobs_concurrently_without_lock_upgrade_failure() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool_with_connections(&directory.path().join("plugin.sqlite3"), 4).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.concurrent", "concurrent", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    store
        .set_desired_state(&plugin.plugin_id, DesiredPluginState::Running)
        .await
        .unwrap();
    store
        .record_running_instance(&plugin.plugin_id, plugin.candidate_generation, instance)
        .await
        .unwrap();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));

    let admit = |operation: &'static str, argument: &'static str| {
        let store = store.clone();
        let plugin_id = plugin.plugin_id.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let operation: PluginOperationId = operation.parse().unwrap();
            let payload = NormalizedJobPayload::new(
                plugin_id,
                "render".to_owned(),
                vec![argument.to_owned()],
                None,
            )
            .unwrap();
            barrier.wait().await;
            store
                .admit_job(
                    &operation,
                    &payload,
                    instance,
                    admitted_at,
                    operation.as_str(),
                )
                .await
        })
    };
    let first = admit("concurrent-job-1", "first");
    let second = admit("concurrent-job-2", "second");
    barrier.wait().await;
    let (first, second) = tokio::join!(first, second);

    assert!(matches!(first.unwrap().unwrap(), JobAdmission::Created(_)));
    assert!(matches!(second.unwrap().unwrap(), JobAdmission::Created(_)));
    assert_eq!(store.list_jobs(None, 1).await.unwrap().len(), 1);
    assert_eq!(
        store
            .list_jobs(Some(&plugin.plugin_id), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    let counts = store.job_counts(&plugin.plugin_id).await.unwrap();
    assert_eq!(counts.dispatching, 2);
    assert_eq!(
        counts.running
            + counts.cancelling
            + counts.succeeded
            + counts.failed
            + counts.cancelled
            + counts.timed_out,
        0
    );
}

#[tokio::test]
async fn sqlite_job_lifecycle_timestamps_are_first_transition_times_and_survive_reopen() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("plugin.sqlite3");
    let pool = sqlite_pool(&path).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.times", "times", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let accepted_at = admitted_at + time::Duration::seconds(1);
    let terminal_at = admitted_at + time::Duration::seconds(2);
    let job = admit(
        &store,
        &plugin.plugin_id,
        "timestamped-job",
        instance,
        admitted_at,
    )
    .await;
    assert_eq!(job.accepted_at, None);
    assert_eq!(job.terminal_at, None);

    let accepted = store
        .mark_job_accepted(job.job_id, instance, accepted_at)
        .await
        .unwrap();
    assert_eq!(accepted.accepted_at, Some(accepted_at));
    let accepted_retry = store
        .mark_job_accepted(job.job_id, instance, accepted_at + time::Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(accepted_retry.accepted_at, Some(accepted_at));

    let finished = store
        .finish_job(
            job.job_id,
            instance,
            JobState::Succeeded,
            Some(b"first"),
            None,
            None,
            terminal_at,
        )
        .await
        .unwrap();
    assert_eq!(finished.terminal_at, Some(terminal_at));
    let finish_retry = store
        .finish_job(
            job.job_id,
            instance,
            JobState::Failed,
            None,
            Some("late"),
            None,
            terminal_at + time::Duration::hours(1),
        )
        .await
        .unwrap();
    assert_eq!(finish_retry.state, JobState::Succeeded);
    assert_eq!(finish_retry.result.as_deref(), Some(b"first".as_slice()));
    assert_eq!(finish_retry.terminal_at, Some(terminal_at));

    drop(store);
    pool.close().await;
    let pool = sqlite_pool_existing(&path).await;
    let reopened = PluginStore::initialize(pool.clone()).await.unwrap();
    let retained = reopened.get_job(job.job_id).await.unwrap();
    assert_eq!(retained.accepted_at, Some(accepted_at));
    assert_eq!(retained.terminal_at, Some(terminal_at));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_bulk_failure_paths_record_terminal_times() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.failures", "failures", Uuid::new_v4());
    install(&store, &plugin).await;
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let failed_at = admitted_at + time::Duration::seconds(1);
    let instance = PluginInstanceId::new();
    let dispatching = admit(
        &store,
        &plugin.plugin_id,
        "instance-dispatching",
        instance,
        admitted_at,
    )
    .await;
    let running = admit(
        &store,
        &plugin.plugin_id,
        "instance-running",
        instance,
        admitted_at,
    )
    .await;
    store
        .mark_job_accepted(running.job_id, instance, admitted_at)
        .await
        .unwrap();
    store
        .settle_ended_instance(
            &plugin.plugin_id,
            instance,
            Some("instance_exited"),
            failed_at,
            "instance_exited",
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_job(dispatching.job_id).await.unwrap().terminal_at,
        Some(failed_at)
    );
    assert_eq!(
        store.get_job(running.job_id).await.unwrap().terminal_at,
        Some(failed_at)
    );

    let startup_job = admit(
        &store,
        &plugin.plugin_id,
        "startup-dispatching",
        PluginInstanceId::new(),
        admitted_at,
    )
    .await;
    let restarted_at = failed_at + time::Duration::seconds(1);
    assert_eq!(
        store
            .fail_nonterminal_jobs_on_startup(restarted_at)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store.get_job(startup_job.job_id).await.unwrap().terminal_at,
        Some(restarted_at)
    );
}

#[tokio::test]
async fn sqlite_instance_settlement_is_atomic_with_job_failure() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool.clone()).await.unwrap();
    let plugin = new_package("oll.settlement", "settlement", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    let ended_at = OffsetDateTime::from_unix_timestamp(1_800_000_100).unwrap();
    let job = admit(
        &store,
        &plugin.plugin_id,
        "settlement-job",
        instance,
        ended_at - time::Duration::seconds(1),
    )
    .await;
    sqlx::query(
        "CREATE TRIGGER reject_instance_settlement BEFORE UPDATE OF state ON plugin_jobs
         WHEN NEW.state = 'failed'
         BEGIN
           SELECT RAISE(ABORT, 'injected settlement failure');
         END",
    )
    .execute(&pool)
    .await
    .unwrap();

    assert!(matches!(
        store
            .settle_ended_instance(
                &plugin.plugin_id,
                instance,
                Some("plugin_process_failed"),
                ended_at,
                "plugin_process_failed",
            )
            .await,
        Err(PluginError::Store(_))
    ));
    let unchanged = store
        .get_plugin(&PluginSelector::Id(plugin.plugin_id.clone()))
        .await
        .unwrap();
    assert_eq!(unchanged.running_instance_id, Some(instance));
    assert!(unchanged.last_lifecycle_failure.is_none());
    assert_eq!(
        store.get_job(job.job_id).await.unwrap().state,
        JobState::Dispatching
    );

    sqlx::query("DROP TRIGGER reject_instance_settlement")
        .execute(&pool)
        .await
        .unwrap();
    store
        .settle_ended_instance(
            &plugin.plugin_id,
            instance,
            Some("plugin_process_failed"),
            ended_at,
            "plugin_process_failed",
        )
        .await
        .unwrap();
    let settled = store
        .get_plugin(&PluginSelector::Id(plugin.plugin_id))
        .await
        .unwrap();
    assert!(settled.running_instance_id.is_none());
    assert_eq!(
        settled.last_lifecycle_failure.as_deref(),
        Some("plugin_process_failed")
    );
    let failed = store.get_job(job.job_id).await.unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.error_code.as_deref(), Some("plugin_process_failed"));
    assert_eq!(failed.terminal_at, Some(ended_at));
}

#[tokio::test]
async fn sqlite_job_cancellation_and_artifact_intent_are_job_scoped() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.alpha", "alpha", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let first = admit(&store, &plugin.plugin_id, "op-first", instance, admitted_at).await;
    let second = admit(
        &store,
        &plugin.plugin_id,
        "op-second",
        instance,
        admitted_at,
    )
    .await;
    store
        .mark_job_accepted(first.job_id, instance, admitted_at)
        .await
        .unwrap();
    store
        .mark_job_accepted(second.job_id, instance, admitted_at)
        .await
        .unwrap();
    let cancellation = store
        .begin_job_cancellation(
            first.job_id,
            JobCancellationReason::UserRequest,
            admitted_at,
        )
        .await
        .unwrap();
    assert!(cancellation.send_request);
    let repeated = store
        .begin_job_cancellation(
            first.job_id,
            JobCancellationReason::UserRequest,
            admitted_at,
        )
        .await
        .unwrap();
    assert_eq!(repeated.job.state, JobState::Cancelling);
    assert!(!repeated.send_request);
    assert!(!repeated.needs_request_dispatch());
    let cancelled = store
        .complete_job_cancellation(first.job_id, instance, admitted_at)
        .await
        .unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(cancelled.accepted_at, Some(admitted_at));
    assert_eq!(cancelled.terminal_at, Some(admitted_at));
    assert_eq!(
        store.get_job(second.job_id).await.unwrap().state,
        JobState::Running
    );

    let artifact_id = PluginArtifactId::new();
    let intent = ArtifactPublishIntent {
        artifact_id,
        job_id: second.job_id,
        plugin_id: plugin.plugin_id,
        file_name: "render.pdf".to_owned(),
        media_type: "application/pdf".to_owned(),
        size_bytes: 8 * 1024 * 1024,
        sha256: [7; 32],
        staging_path: directory.path().join(".render.staging"),
        destination: directory.path().join("render.pdf"),
        correlation_id: second.correlation_id.clone(),
    };
    store.prepare_artifact_publish(&intent).await.unwrap();
    assert_eq!(
        store.artifact_publish_intent(artifact_id).await.unwrap(),
        Some(intent.clone())
    );
    let stored = store
        .finalize_artifact_publish(artifact_id, admitted_at)
        .await
        .unwrap();
    assert_eq!(stored.size_bytes, intent.size_bytes);
    assert!(store.artifact_publish_intents().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_cancellation_snapshot_cannot_combine_terminal_state_with_dispatch_claim() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool_with_connections(&directory.path().join("plugin.sqlite3"), 4).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.cancel-race", "cancel-race", Uuid::new_v4());
    install(&store, &plugin).await;
    let instance = PluginInstanceId::new();
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let job = admit(
        &store,
        &plugin.plugin_id,
        "cancel-race",
        instance,
        admitted_at,
    )
    .await;
    store
        .mark_job_accepted(job.job_id, instance, admitted_at)
        .await
        .unwrap();
    let job_id = job.job_id;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let cancellation_store = store.clone();
    let cancellation_barrier = barrier.clone();
    let cancellation = tokio::spawn(async move {
        cancellation_barrier.wait().await;
        cancellation_store
            .begin_job_cancellation(
                job_id,
                JobCancellationReason::UserRequest,
                admitted_at + time::Duration::seconds(1),
            )
            .await
    });
    let completion_store = store.clone();
    let completion_barrier = barrier.clone();
    let completion = tokio::spawn(async move {
        completion_barrier.wait().await;
        completion_store
            .finish_job(
                job_id,
                instance,
                JobState::Succeeded,
                None,
                None,
                None,
                admitted_at + time::Duration::seconds(1),
            )
            .await
    });
    barrier.wait().await;
    let (cancellation, completion) = tokio::join!(cancellation, completion);
    let cancellation = cancellation.unwrap().unwrap();
    completion.unwrap().unwrap();

    assert!(
        !cancellation.send_request || !cancellation.job.state.is_terminal(),
        "a terminal snapshot must never retain a cancellation dispatch claim"
    );
}

async fn admit(
    store: &PluginStore,
    plugin_id: &PluginId,
    operation: &str,
    instance: PluginInstanceId,
    admitted_at: OffsetDateTime,
) -> crate::plugin::PluginJob {
    let installed = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    if installed.running_instance_id != Some(instance) {
        if let Some(running) = installed.running_instance_id {
            store
                .settle_ended_instance(plugin_id, running, None, admitted_at, "test_instance_ended")
                .await
                .unwrap();
        }
        store
            .set_desired_state(plugin_id, DesiredPluginState::Running)
            .await
            .unwrap();
        store
            .record_running_instance(plugin_id, installed.current_generation, instance)
            .await
            .unwrap();
    }
    let payload = NormalizedJobPayload::new(
        plugin_id.clone(),
        "run".to_owned(),
        vec![operation.to_owned()],
        None,
    )
    .unwrap();
    let operation = operation.parse().unwrap();
    match store
        .admit_job(&operation, &payload, instance, admitted_at, "corr-job")
        .await
        .unwrap()
    {
        JobAdmission::Created(job) => job,
        JobAdmission::Existing(_) => panic!("unexpected retained job"),
    }
}

#[tokio::test]
async fn sqlite_removal_intent_requires_each_recovery_phase() {
    let directory = TempDir::new().unwrap();
    let pool = sqlite_pool(&directory.path().join("plugin.sqlite3")).await;
    let store = PluginStore::initialize(pool).await.unwrap();
    let plugin = new_package("oll.alpha", "alpha", Uuid::new_v4());
    install(&store, &plugin).await;
    let intent = RemovalIntent {
        plugin_id: plugin.plugin_id.clone(),
        operation_id: "remove-1".to_owned(),
        plugins_lua_sha256: [9; 32],
        prepared_plugins_lua: b"return {}\n".to_vec(),
        trash_path: directory.path().join("trash/remove-1"),
        phase: RemovalPhase::Prepared,
        correlation_id: "corr-remove".to_owned(),
    };
    store.prepare_removal(&intent).await.unwrap();
    assert!(
        store
            .discard_prepared_removal(&plugin.plugin_id)
            .await
            .unwrap()
    );
    assert!(
        store
            .removal_intent(&plugin.plugin_id)
            .await
            .unwrap()
            .is_none()
    );
    store.prepare_removal(&intent).await.unwrap();
    assert!(matches!(
        store.finalize_removal(&plugin.plugin_id).await,
        Err(PluginError::FailedPrecondition(_))
    ));
    store
        .advance_removal(
            &plugin.plugin_id,
            RemovalPhase::Prepared,
            RemovalPhase::DeclarationPublished,
        )
        .await
        .unwrap();
    store
        .advance_removal(
            &plugin.plugin_id,
            RemovalPhase::DeclarationPublished,
            RemovalPhase::PackageTrashed,
        )
        .await
        .unwrap();
    store.finalize_removal(&plugin.plugin_id).await.unwrap();
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin.plugin_id))
            .await,
        Err(PluginError::NotFound(_))
    ));
}
