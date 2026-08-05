use std::fs;

use sha2::{Digest, Sha256};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

use super::{IdentityTransitionKind, ReplicaStore, RetainedCommit};
use crate::{
    configuration::ReplicaStoreConfig,
    plugin::{
        DesiredPluginState, InstallMode, PackagePublishIntent, PluginId, PluginSelector,
        PluginStore,
    },
    replica::{
        identity,
        model::{initialize_from_disk, scan_working_tree},
    },
};

#[tokio::test]
#[ignore = "requires OLL_TEST_POSTGRES_URL and an externally managed PostgreSQL database"]
async fn postgres_implements_the_logical_store_contract_when_configured() {
    let base_url = std::env::var("OLL_TEST_POSTGRES_URL")
        .expect("explicit PostgreSQL contract test requires UTF-8 OLL_TEST_POSTGRES_URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .expect("connect to OLL_TEST_POSTGRES_URL");
    let schema = format!("oll_test_{}", Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated PostgreSQL test schema");

    let mut scoped = Url::parse(&base_url).expect("parse OLL_TEST_POSTGRES_URL");
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let exercise = async {
        let config = ReplicaStoreConfig::Postgres {
            url: scoped.as_str().parse().map_err(|error: String| error)?,
        };
        let directory = TempDir::new().map_err(|error| error.to_string())?;
        let working = directory.path().join("working");
        let config_root = directory.path().join("config");
        fs::create_dir(&working).map_err(|error| error.to_string())?;
        fs::create_dir(&config_root).map_err(|error| error.to_string())?;
        fs::write(working.join("a.md"), "postgres").map_err(|error| error.to_string())?;
        let binary = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff".to_vec();
        fs::write(working.join("image.gif"), &binary).map_err(|error| error.to_string())?;
        let disk = scan_working_tree(&working).map_err(|error| error.to_string())?;
        let change = initialize_from_disk(&disk, Uuid::new_v4(), "postgres-test-correlation")
            .map_err(|error| error.to_string())?;
        let store = ReplicaStore::open(&config)
            .await
            .map_err(|error| error.to_string())?;
        let plugin_store = PluginStore::initialize(store.pool.clone())
            .await
            .map_err(|error| error.to_string())?;
        let plugin_id: PluginId = "oll.postgres-isolation".parse().unwrap();
        let plugin_generation = Uuid::new_v4();
        let declaration = b"postgres plugin isolation declaration".to_vec();
        let package = PackagePublishIntent {
            plugin_id: plugin_id.clone(),
            plugin_name: "postgres-isolation".parse().unwrap(),
            operation_id: "postgres-plugin-isolation-install".to_owned(),
            expected_current_generation: None,
            candidate_generation: plugin_generation,
            normalized_declaration: declaration.clone(),
            declaration_sha256: Sha256::digest(&declaration).into(),
            effective_manifest: b"postgres-plugin-state-must-remain-local".to_vec(),
            selected_commit: Some("0123456789abcdef".to_owned()),
            install_mode: InstallMode::Source,
            release_id: None,
            correlation_id: "postgres-plugin-isolation-correlation".to_owned(),
        };
        plugin_store
            .prepare_package_publish(&package)
            .await
            .map_err(|error| error.to_string())?;
        plugin_store
            .finalize_package_publish(&plugin_id, plugin_generation)
            .await
            .map_err(|error| error.to_string())?;
        plugin_store
            .set_desired_state(&plugin_id, DesiredPluginState::Running)
            .await
            .map_err(|error| error.to_string())?;
        let expected_plugin = plugin_store
            .request_restart(&plugin_id)
            .await
            .map_err(|error| error.to_string())?;
        store
            .build_inactive_generation(
                &change.replica,
                &change.blobs,
                &change.operations,
                &["/initial-projection-marker".to_owned()],
            )
            .await
            .map_err(|error| error.to_string())?;
        identity::activate_candidate(
            &store,
            &config_root,
            None,
            &change.replica,
            IdentityTransitionKind::Initialize,
            false,
        )
        .await
        .map_err(|error| error.to_string())?;
        let loaded = store
            .load_active()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "PostgreSQL active generation is missing".to_owned())?;
        if loaded.replica_id != change.replica.replica_id
            || loaded.documents.len() != change.replica.documents.len()
            || loaded.entries.len() != change.replica.entries.len()
        {
            return Err("PostgreSQL logical round trip changed replica state".to_owned());
        }
        if plugin_store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .map_err(|error| error.to_string())?
            != expected_plugin
        {
            return Err("PostgreSQL initial activation changed plugin state".to_owned());
        }

        let document_id = *loaded
            .documents
            .keys()
            .next()
            .ok_or_else(|| "PostgreSQL document object is missing".to_owned())?;
        let operations = store
            .list_operations(loaded.generation_id, document_id, 10)
            .await
            .map_err(|error| error.to_string())?;
        if operations.len() != 1 {
            return Err("PostgreSQL operation history did not round trip".to_owned());
        }

        let blob_hash = loaded
            .entries
            .values()
            .find_map(|entry| {
                entry
                    .binary()
                    .and_then(|binary| binary.winning_version())
                    .map(|(_, version)| version.sha256.clone())
            })
            .ok_or_else(|| "PostgreSQL binary version is missing".to_owned())?;
        if store
            .read_blob(&blob_hash)
            .await
            .map_err(|error| error.to_string())?
            != binary
        {
            return Err("PostgreSQL blob chunks did not round trip".to_owned());
        }
        let projected_blob = directory.path().join("projected.gif");
        store
            .write_blob_to_path(&blob_hash, &projected_blob)
            .await
            .map_err(|error| error.to_string())?;
        if fs::read(projected_blob).map_err(|error| error.to_string())? != binary {
            return Err("PostgreSQL streamed blob projection changed bytes".to_owned());
        }

        let retained = RetainedCommit {
            operation_id: "postgres-retained-commit".to_owned(),
            request: vec![1, 2, 3],
            response: vec![4, 5, 6],
        };
        store
            .save_active_commit(
                &loaded,
                &[],
                &[],
                &["/saved-projection-marker".to_owned()],
                &retained,
            )
            .await
            .map_err(|error| error.to_string())?;
        let restored = store
            .retained_commit(loaded.generation_id, &retained.operation_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "PostgreSQL retained commit is missing".to_owned())?;
        if restored.request != retained.request || restored.response != retained.response {
            return Err("PostgreSQL retained commit changed bytes".to_owned());
        }
        store
            .clear_projection_path(loaded.generation_id, "/saved-projection-marker")
            .await
            .map_err(|error| error.to_string())?;
        if store
            .projection_paths(loaded.generation_id)
            .await
            .map_err(|error| error.to_string())?
            != ["/initial-projection-marker"]
        {
            return Err("PostgreSQL path acknowledgement cleared an unrelated marker".to_owned());
        }

        let mut candidate = loaded.clone();
        candidate.generation_id = Uuid::new_v4();
        store
            .build_inactive_generation(&candidate, &[], &[], &[])
            .await
            .map_err(|error| error.to_string())?;
        identity::activate_candidate(
            &store,
            &config_root,
            Some((loaded.generation_id, loaded.replica_id)),
            &candidate,
            IdentityTransitionKind::SnapshotImport,
            true,
        )
        .await
        .map_err(|error| error.to_string())?;
        if !store
            .projection_pending()
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("PostgreSQL generation switch omitted projection_pending".to_owned());
        }
        let switched = store
            .load_active()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "PostgreSQL switched generation is missing".to_owned())?;
        if switched.generation_id != candidate.generation_id {
            return Err("PostgreSQL generation switch selected the wrong state".to_owned());
        }
        store
            .clear_projection_pending(candidate.generation_id)
            .await
            .map_err(|error| error.to_string())?;
        if store
            .projection_pending()
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("PostgreSQL projection_pending did not clear".to_owned());
        }
        if !store
            .projection_paths(candidate.generation_id)
            .await
            .map_err(|error| error.to_string())?
            .is_empty()
        {
            return Err("PostgreSQL projection paths did not clear".to_owned());
        }
        if plugin_store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .map_err(|error| error.to_string())?
            != expected_plugin
        {
            return Err("PostgreSQL snapshot replacement changed plugin state".to_owned());
        }

        let expected_state_token = store
            .active_state_token(candidate.generation_id)
            .await
            .map_err(|error| error.to_string())?;
        let mut sync_candidate = candidate.clone();
        sync_candidate.generation_id = Uuid::new_v4();
        store
            .build_sync_generation(candidate.generation_id, &sync_candidate, &[], &[])
            .await
            .map_err(|error| error.to_string())?;
        store
            .activate_sync_generation(
                candidate.generation_id,
                expected_state_token,
                &sync_candidate,
            )
            .await
            .map_err(|error| error.to_string())?;
        if plugin_store
            .get_plugin(&PluginSelector::Id(plugin_id))
            .await
            .map_err(|error| error.to_string())?
            != expected_plugin
        {
            return Err("PostgreSQL sync activation changed plugin state".to_owned());
        }
        drop(store);
        Ok::<(), String>(())
    }
    .await;

    let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.expect("drop isolated PostgreSQL test schema");
    if let Err(error) = exercise {
        panic!("{error}");
    }
}
