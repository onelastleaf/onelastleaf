use std::{env, fs};

use sha2::{Digest, Sha256};
use sqlx::{AnyPool, any::AnyPoolOptions};
use tempfile::TempDir;
use time::OffsetDateTime;
use url::Url;
use uuid::Uuid;

use super::PluginStore;
use crate::plugin::{
    ArtifactPublishIntent, DesiredPluginState, InstallMode, JobAdmission, JobState,
    NormalizedJobPayload, PackagePublishIntent, PluginArtifactId, PluginError, PluginId,
    PluginInstanceId, PluginOperationId, PluginSelector, RemovalIntent, RemovalPhase,
};

#[tokio::test]
#[ignore = "requires OLL_TEST_POSTGRES_URL and an externally managed PostgreSQL database"]
async fn postgres_implements_plugin_store_contract_when_configured() {
    let base_url = env::var("OLL_TEST_POSTGRES_URL")
        .expect("explicit PostgreSQL plugin-store test requires UTF-8 OLL_TEST_POSTGRES_URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .expect("connect to OLL_TEST_POSTGRES_URL");
    let schema = format!("oll_plugin_test_{}", Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated PostgreSQL plugin-store schema");

    let scoped_url = postgres_schema_url(&base_url, &schema);
    sqlx::any::install_default_drivers();
    let scratch = TempDir::new().expect("create PostgreSQL plugin-store scratch directory");
    let exercise = exercise_contract(&scoped_url, &scratch).await;

    let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.expect("drop isolated PostgreSQL plugin-store schema");
    if let Err(error) = exercise {
        panic!("{error}");
    }
}

fn postgres_schema_url(base_url: &str, schema: &str) -> String {
    let mut url = Url::parse(base_url).expect("parse OLL_TEST_POSTGRES_URL");
    let mut retained = Vec::new();
    let mut options = None;
    for (key, value) in url.query_pairs() {
        if key == "options" {
            options = Some(value.into_owned());
        } else {
            retained.push((key.into_owned(), value.into_owned()));
        }
    }
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in retained {
            query.append_pair(&key, &value);
        }
        let search_path = format!("-csearch_path={schema}");
        let options = options
            .map(|existing| format!("{existing} {search_path}"))
            .unwrap_or(search_path);
        query.append_pair("options", &options);
    }
    url.into()
}

async fn exercise_contract(scoped_url: &str, scratch: &TempDir) -> Result<(), String> {
    let (mut pool, mut store) = open_store(scoped_url).await?;
    PluginStore::initialize(pool.clone())
        .await
        .map_err(|error| error.to_string())?;

    let download_dir = scratch.path().join("downloads");
    fs::create_dir(&download_dir).map_err(|error| error.to_string())?;
    store
        .cache_artifact_download_dir(&download_dir)
        .await
        .map_err(|error| error.to_string())?;

    let old_generation = Uuid::new_v4();
    let primary = new_package("oll.postgres", "postgres", old_generation);
    store
        .prepare_package_publish(&primary)
        .await
        .map_err(|error| error.to_string())?;
    require(
        store
            .package_publish_intent(&primary.plugin_id)
            .await
            .map_err(|error| error.to_string())?
            == Some(primary.clone()),
        "PostgreSQL did not persist the package publish intent",
    )?;
    let installed = store
        .finalize_package_publish(&primary.plugin_id, old_generation)
        .await
        .map_err(|error| error.to_string())?;
    require(
        installed.desired_state == DesiredPluginState::Stopped,
        "new PostgreSQL installations must start stopped",
    )?;
    sqlx::query("UPDATE plugins SET restart_attempt = -1 WHERE plugin_id = $1")
        .bind(primary.plugin_id.as_str())
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
    require_error(
        store
            .set_desired_state(&primary.plugin_id, DesiredPluginState::Running)
            .await,
        |error| matches!(error, PluginError::CorruptStore(_)),
        "PostgreSQL committed desired state before parsing its response row",
    )?;
    require(
        sqlx::query_scalar::<_, String>("SELECT desired_state FROM plugins WHERE plugin_id = $1")
            .bind(primary.plugin_id.as_str())
            .fetch_one(&pool)
            .await
            .map_err(|error| error.to_string())?
            == "stopped",
        "PostgreSQL did not roll back desired state after response parsing failed",
    )?;
    require_error(
        store.request_restart(&primary.plugin_id).await,
        |error| matches!(error, PluginError::CorruptStore(_)),
        "PostgreSQL committed restart before parsing its response row",
    )?;
    require(
        sqlx::query_scalar::<_, i64>("SELECT restart_sequence FROM plugins WHERE plugin_id = $1")
            .bind(primary.plugin_id.as_str())
            .fetch_one(&pool)
            .await
            .map_err(|error| error.to_string())?
            == 0,
        "PostgreSQL did not roll back restart after response parsing failed",
    )?;
    sqlx::query("UPDATE plugins SET restart_attempt = 0 WHERE plugin_id = $1")
        .bind(primary.plugin_id.as_str())
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
    store
        .set_desired_state(&primary.plugin_id, DesiredPluginState::Running)
        .await
        .map_err(|error| error.to_string())?;
    store
        .request_restart(&primary.plugin_id)
        .await
        .map_err(|error| error.to_string())?;
    let instance = PluginInstanceId::new();
    store
        .record_running_instance(&primary.plugin_id, old_generation, instance)
        .await
        .map_err(|error| error.to_string())?;
    store
        .record_instance_ready(&primary.plugin_id, instance)
        .await
        .map_err(|error| error.to_string())?;

    reopen(scoped_url, &mut pool, &mut store).await?;
    let persisted = store
        .get_plugin(&PluginSelector::Id(primary.plugin_id.clone()))
        .await
        .map_err(|error| error.to_string())?;
    require(
        persisted.desired_state == DesiredPluginState::Running
            && persisted.restart_sequence == 1
            && persisted.running_generation == Some(old_generation)
            && persisted.running_instance_id == Some(instance),
        "PostgreSQL lost desired state or running-instance state across reconnect",
    )?;
    require(
        store
            .artifact_download_dir()
            .await
            .map_err(|error| error.to_string())?
            .as_deref()
            == Some(download_dir.as_path()),
        "PostgreSQL lost the cached artifact directory across reconnect",
    )?;

    let new_generation = Uuid::new_v4();
    let mut replacement = primary.clone();
    replacement.plugin_name = "postgres-new".parse().unwrap();
    replacement.operation_id = "update-oll.postgres".to_owned();
    replacement.expected_current_generation = Some(old_generation);
    replacement.candidate_generation = new_generation;
    replacement.effective_manifest = b"replacement manifest".to_vec();
    replacement.correlation_id = "correlation-update".to_owned();
    store
        .prepare_package_publish(&replacement)
        .await
        .map_err(|error| error.to_string())?;
    reopen(scoped_url, &mut pool, &mut store).await?;
    require(
        store
            .package_publish_intent(&primary.plugin_id)
            .await
            .map_err(|error| error.to_string())?
            == Some(replacement.clone()),
        "PostgreSQL lost an unfinished replacement intent across reconnect",
    )?;
    let updated = store
        .finalize_package_publish(&primary.plugin_id, new_generation)
        .await
        .map_err(|error| error.to_string())?;
    require(
        updated.current_generation == new_generation
            && updated.running_generation == Some(old_generation)
            && updated.running_instance_id == Some(instance)
            && updated.desired_state == DesiredPluginState::Running,
        "PostgreSQL did not preserve the running old generation across publication",
    )?;
    require(
        store
            .get_plugin(&PluginSelector::Name("postgres-new".parse().unwrap()))
            .await
            .map_err(|error| error.to_string())?
            .plugin_id
            == primary.plugin_id,
        "PostgreSQL did not atomically publish the updated name binding",
    )?;

    let collision = new_package("oll.collision", "postgres-new", Uuid::new_v4());
    require_error(
        store.prepare_package_publish(&collision).await,
        |error| matches!(error, PluginError::AlreadyExists(_)),
        "PostgreSQL accepted a duplicate effective plugin name",
    )?;

    let secondary = new_package("oll.secondary", "secondary", Uuid::new_v4());
    install(&store, &secondary).await?;
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let operation: PluginOperationId = "postgres-global-operation".parse().unwrap();
    let payload = normalized_payload(primary.plugin_id.clone(), "same");
    let created = store
        .admit_job(&operation, &payload, instance, now, "correlation-job")
        .await
        .map_err(|error| error.to_string())?;
    let JobAdmission::Created(job) = created else {
        return Err("first PostgreSQL job admission was not created".to_owned());
    };
    let retry = store
        .admit_job(
            &operation,
            &payload,
            PluginInstanceId::new(),
            now + time::Duration::seconds(30),
            "correlation-retry",
        )
        .await
        .map_err(|error| error.to_string())?;
    let JobAdmission::Existing(retry) = retry else {
        return Err("PostgreSQL job retry created a second job".to_owned());
    };
    require(
        retry.job_id == job.job_id && retry.absolute_deadline == job.absolute_deadline,
        "PostgreSQL did not retain the original idempotent job admission",
    )?;
    let conflicting_payload = normalized_payload(secondary.plugin_id.clone(), "different");
    require_error(
        store
            .admit_job(
                &operation,
                &conflicting_payload,
                instance,
                now,
                "correlation-conflict",
            )
            .await,
        |error| matches!(error, PluginError::AlreadyExists(_)),
        "PostgreSQL allowed deployment-global operation-ID reuse for another payload",
    )?;
    sqlx::query("UPDATE plugin_jobs SET action = '' WHERE job_id = $1")
        .bind(job.job_id.to_string())
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
    require_error(
        store.mark_job_accepted(job.job_id, instance, now).await,
        |error| matches!(error, PluginError::CorruptStore(_)),
        "PostgreSQL committed job acceptance before parsing its response row",
    )?;
    require(
        sqlx::query_scalar::<_, String>("SELECT state FROM plugin_jobs WHERE job_id = $1")
            .bind(job.job_id.to_string())
            .fetch_one(&pool)
            .await
            .map_err(|error| error.to_string())?
            == "dispatching",
        "PostgreSQL did not roll back job acceptance after response parsing failed",
    )?;
    sqlx::query("UPDATE plugin_jobs SET action = $1 WHERE job_id = $2")
        .bind(&payload.action)
        .bind(job.job_id.to_string())
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
    store
        .mark_job_accepted(job.job_id, instance, now)
        .await
        .map_err(|error| error.to_string())?;
    let counts = store
        .job_counts(&primary.plugin_id)
        .await
        .map_err(|error| error.to_string())?;
    require(
        counts.running == 1
            && counts.dispatching == 0
            && counts.cancelling == 0
            && counts.succeeded == 0
            && counts.failed == 0
            && counts.cancelled == 0
            && counts.timed_out == 0,
        "PostgreSQL job-state aggregation returned inconsistent counts",
    )?;

    let artifact_id = PluginArtifactId::new();
    let artifact_path = download_dir.join("postgres.pdf");
    fs::write(&artifact_path, b"published bytes").map_err(|error| error.to_string())?;
    let artifact_intent = ArtifactPublishIntent {
        artifact_id,
        job_id: job.job_id,
        plugin_id: primary.plugin_id.clone(),
        file_name: "postgres.pdf".to_owned(),
        media_type: "application/pdf".to_owned(),
        size_bytes: 15,
        sha256: Sha256::digest(b"published bytes").into(),
        staging_path: download_dir.join(".postgres.staging"),
        destination: artifact_path.clone(),
        correlation_id: job.correlation_id.clone(),
    };
    store
        .prepare_artifact_publish(&artifact_intent)
        .await
        .map_err(|error| error.to_string())?;
    reopen(scoped_url, &mut pool, &mut store).await?;
    require(
        store
            .artifact_publish_intent(artifact_id)
            .await
            .map_err(|error| error.to_string())?
            == Some(artifact_intent.clone()),
        "PostgreSQL lost an artifact publication intent across reconnect",
    )?;
    let stored_artifact = store
        .finalize_artifact_publish(artifact_id, now)
        .await
        .map_err(|error| error.to_string())?;
    let idempotent_artifact = store
        .finalize_artifact_publish(artifact_id, now + time::Duration::seconds(1))
        .await
        .map_err(|error| error.to_string())?;
    require(
        stored_artifact == idempotent_artifact
            && store
                .artifacts_for_job(job.job_id)
                .await
                .map_err(|error| error.to_string())?
                == vec![stored_artifact],
        "PostgreSQL artifact finalization is not idempotent",
    )?;
    store
        .finish_job(
            job.job_id,
            instance,
            JobState::Succeeded,
            Some(b"done"),
            None,
            None,
            now + time::Duration::seconds(2),
        )
        .await
        .map_err(|error| error.to_string())?;
    store
        .settle_ended_instance(
            &primary.plugin_id,
            instance,
            None,
            now + time::Duration::seconds(3),
            "plugin_process_stopped",
        )
        .await
        .map_err(|error| error.to_string())?;
    require(
        store
            .get_plugin(&PluginSelector::Id(primary.plugin_id.clone()))
            .await
            .map_err(|error| error.to_string())?
            .running_instance_id
            .is_none(),
        "PostgreSQL did not atomically settle the exact running instance",
    )?;

    let removal = RemovalIntent {
        plugin_id: primary.plugin_id.clone(),
        operation_id: "remove-oll.postgres".to_owned(),
        plugins_lua_sha256: [9; 32],
        prepared_plugins_lua: b"return {}\n".to_vec(),
        trash_path: scratch.path().join("trash/oll.postgres"),
        phase: RemovalPhase::Prepared,
        correlation_id: "correlation-remove".to_owned(),
    };
    store
        .prepare_removal(&removal)
        .await
        .map_err(|error| error.to_string())?;
    store
        .prepare_removal(&removal)
        .await
        .map_err(|error| error.to_string())?;
    reopen(scoped_url, &mut pool, &mut store).await?;
    require(
        store
            .removal_intent(&primary.plugin_id)
            .await
            .map_err(|error| error.to_string())?
            == Some(removal),
        "PostgreSQL lost a removal intent across reconnect",
    )?;
    require_error(
        store.finalize_removal(&primary.plugin_id).await,
        |error| matches!(error, PluginError::FailedPrecondition(_)),
        "PostgreSQL finalized removal before the package was trashed",
    )?;
    store
        .advance_removal(
            &primary.plugin_id,
            RemovalPhase::Prepared,
            RemovalPhase::DeclarationPublished,
        )
        .await
        .map_err(|error| error.to_string())?;
    store
        .advance_removal(
            &primary.plugin_id,
            RemovalPhase::DeclarationPublished,
            RemovalPhase::PackageTrashed,
        )
        .await
        .map_err(|error| error.to_string())?;
    store
        .finalize_removal(&primary.plugin_id)
        .await
        .map_err(|error| error.to_string())?;
    require_error(
        store
            .get_plugin(&PluginSelector::Id(primary.plugin_id.clone()))
            .await,
        |error| matches!(error, PluginError::NotFound(_)),
        "PostgreSQL retained the plugin after removal finalization",
    )?;
    require_error(
        store.get_job(job.job_id).await,
        |error| matches!(error, PluginError::NotFound(_)),
        "PostgreSQL retained a removed plugin job",
    )?;
    require_error(
        store.get_artifact(artifact_id).await,
        |error| matches!(error, PluginError::NotFound(_)),
        "PostgreSQL retained removed plugin artifact metadata",
    )?;
    require(
        fs::read(&artifact_path).map_err(|error| error.to_string())? == b"published bytes",
        "plugin removal deleted a published artifact file",
    )?;
    require(
        store
            .get_plugin(&PluginSelector::Id(secondary.plugin_id.clone()))
            .await
            .map_err(|error| error.to_string())?
            .plugin_id
            == secondary.plugin_id,
        "PostgreSQL removal affected another installed plugin",
    )?;

    let rebound = new_package("oll.rebound", "postgres-new", Uuid::new_v4());
    install(&store, &rebound).await?;
    require(
        store
            .get_plugin(&PluginSelector::Name("postgres-new".parse().unwrap()))
            .await
            .map_err(|error| error.to_string())?
            .plugin_id
            == rebound.plugin_id,
        "PostgreSQL did not release the removed plugin name binding",
    )?;

    sqlx::query("DELETE FROM plugin_meta WHERE singleton = 1")
        .execute(&pool)
        .await
        .map_err(|error| error.to_string())?;
    require_error(
        store.cache_artifact_download_dir(&download_dir).await,
        |error| matches!(error, PluginError::CorruptStore(_)),
        "PostgreSQL silently lost the artifact directory when plugin metadata was missing",
    )?;

    drop(store);
    pool.close().await;
    Ok(())
}

async fn open_store(scoped_url: &str) -> Result<(AnyPool, PluginStore), String> {
    let pool = AnyPoolOptions::new()
        .max_connections(4)
        .connect(scoped_url)
        .await
        .map_err(|error| error.to_string())?;
    let store = PluginStore::initialize(pool.clone())
        .await
        .map_err(|error| error.to_string())?;
    Ok((pool, store))
}

async fn reopen(
    scoped_url: &str,
    pool: &mut AnyPool,
    store: &mut PluginStore,
) -> Result<(), String> {
    let replacement = AnyPoolOptions::new()
        .max_connections(4)
        .connect(scoped_url)
        .await
        .map_err(|error| error.to_string())?;
    let replacement_store = PluginStore::initialize(replacement.clone())
        .await
        .map_err(|error| error.to_string())?;
    let old_pool = std::mem::replace(pool, replacement);
    *store = replacement_store;
    old_pool.close().await;
    Ok(())
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

async fn install(store: &PluginStore, intent: &PackagePublishIntent) -> Result<(), String> {
    store
        .prepare_package_publish(intent)
        .await
        .map_err(|error| error.to_string())?;
    store
        .finalize_package_publish(&intent.plugin_id, intent.candidate_generation)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn normalized_payload(plugin_id: PluginId, argument: &str) -> NormalizedJobPayload {
    NormalizedJobPayload::new(
        plugin_id,
        "render".to_owned(),
        vec![argument.to_owned()],
        None,
    )
    .unwrap()
}

fn require(condition: bool, message: &str) -> Result<(), String> {
    condition.then_some(()).ok_or_else(|| message.to_owned())
}

fn require_error<T>(
    result: Result<T, PluginError>,
    expected: impl FnOnce(&PluginError) -> bool,
    message: &str,
) -> Result<(), String> {
    match result {
        Err(error) if expected(&error) => Ok(()),
        Err(error) => Err(format!("{message}: received {error}")),
        Ok(_) => Err(message.to_owned()),
    }
}
