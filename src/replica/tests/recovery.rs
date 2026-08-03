use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_sql_transaction_and_generation_switch_boundaries_preserve_authority() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/a.md"), "authoritative").unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;
    let active = runtime.state.read().await.clone().unwrap();

    let mut uncommitted = active.clone();
    uncommitted.projection_generation += 1;
    let wrong_blob = NewBlob {
        sha256: "0".repeat(64),
        source: NewBlobSource::Bytes(b"not-zero-hash".to_vec()),
    };
    assert!(
        runtime
            .store
            .save_active(
                &uncommitted,
                &[wrong_blob],
                &[],
                &["/must-not-persist".to_owned()]
            )
            .await
            .is_err()
    );
    let loaded = runtime.store.load_active().await.unwrap().unwrap();
    assert_eq!(loaded.projection_generation, active.projection_generation);
    assert!(
        runtime
            .store
            .projection_paths(active.generation_id)
            .await
            .unwrap()
            .is_empty()
    );

    let mut inactive = active.clone();
    inactive.generation_id = Uuid::new_v4();
    inactive.replica_id = Uuid::new_v4();
    runtime
        .store
        .build_inactive_generation(&inactive, &[], &[], &[])
        .await
        .unwrap();
    drop(runtime);

    let before_switch_restart = deployment.start().await;
    assert_eq!(
        before_switch_restart.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: active.replica_id
        }
    );
    assert!(
        !before_switch_restart
            .store
            .generation_exists(inactive.generation_id)
            .await
            .unwrap()
    );
    before_switch_restart
        .store
        .build_inactive_generation(&inactive, &[], &[], &[])
        .await
        .unwrap();
    shutdown_runtime(&before_switch_restart).await;
    identity::activate_candidate(
        &before_switch_restart.store,
        &deployment.config_root,
        Some((active.generation_id, active.replica_id)),
        &inactive,
        IdentityTransitionKind::SnapshotImport,
        true,
    )
    .await
    .unwrap();
    fs::write(deployment.native("/a.md"), "old-working-tree").unwrap();
    fs::write(deployment.native("/stale.md"), "must disappear").unwrap();
    drop(before_switch_restart);

    let after_switch_restart = deployment.start().await;
    assert_eq!(
        after_switch_restart.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: inactive.replica_id
        }
    );
    assert_eq!(
        fs::read_to_string(deployment.native("/a.md")).unwrap(),
        "authoritative"
    );
    assert!(!deployment.native("/stale.md").exists());
    assert!(
        !after_switch_restart
            .store
            .projection_pending()
            .await
            .unwrap()
    );
    shutdown_runtime(&after_switch_restart).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_identity_transition_recovers_on_both_sides_of_sql_activation() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/identity.md"), "identity recovery").unwrap();
    let runtime = deployment.start().await;
    shutdown_runtime(&runtime).await;
    let active = runtime.state.read().await.clone().unwrap();
    let old_identity_file = identity::read_identity_bytes(&deployment.config_root)
        .unwrap()
        .unwrap();

    let mut before_activation = active.clone();
    before_activation.generation_id = Uuid::new_v4();
    before_activation.replica_id = Uuid::new_v4();
    runtime
        .store
        .build_inactive_generation(&before_activation, &[], &[], &[])
        .await
        .unwrap();
    let prepared_new_file = identity::ReplicaIdentity::new(before_activation.replica_id)
        .encode()
        .unwrap();
    runtime
        .store
        .prepare_identity_transition(&IdentityTransition {
            kind: IdentityTransitionKind::SnapshotImport,
            expected_active_generation: Some(active.generation_id),
            candidate_generation: before_activation.generation_id,
            old_replica_id: Some(active.replica_id),
            new_replica_id: before_activation.replica_id,
            old_identity_file: Some(old_identity_file.clone()),
            new_identity_file: prepared_new_file.clone(),
            projection_pending: true,
            committed: false,
        })
        .await
        .unwrap();
    fs::write(
        identity::identity_path(&deployment.config_root),
        &prepared_new_file,
    )
    .unwrap();
    drop(runtime);

    let rolled_back = deployment.start().await;
    assert_eq!(
        rolled_back.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: active.replica_id,
        }
    );
    assert_eq!(
        identity::read_identity_bytes(&deployment.config_root)
            .unwrap()
            .unwrap(),
        old_identity_file
    );
    assert!(
        rolled_back
            .store
            .identity_transition()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        rolled_back
            .store
            .load_generation(&before_activation.generation_id.to_string())
            .await
            .is_err()
    );
    shutdown_runtime(&rolled_back).await;

    let active = rolled_back.state.read().await.clone().unwrap();
    let mut after_activation = active.clone();
    after_activation.generation_id = Uuid::new_v4();
    after_activation.replica_id = Uuid::new_v4();
    rolled_back
        .store
        .build_inactive_generation(&after_activation, &[], &[], &[])
        .await
        .unwrap();
    let committed_new_file = identity::ReplicaIdentity::new(after_activation.replica_id)
        .encode()
        .unwrap();
    rolled_back
        .store
        .prepare_identity_transition(&IdentityTransition {
            kind: IdentityTransitionKind::SnapshotImport,
            expected_active_generation: Some(active.generation_id),
            candidate_generation: after_activation.generation_id,
            old_replica_id: Some(active.replica_id),
            new_replica_id: after_activation.replica_id,
            old_identity_file: Some(
                identity::read_identity_bytes(&deployment.config_root)
                    .unwrap()
                    .unwrap(),
            ),
            new_identity_file: committed_new_file.clone(),
            projection_pending: true,
            committed: false,
        })
        .await
        .unwrap();
    fs::write(
        identity::identity_path(&deployment.config_root),
        &committed_new_file,
    )
    .unwrap();
    rolled_back
        .store
        .activate_identity_transition(after_activation.generation_id)
        .await
        .unwrap();
    drop(rolled_back);

    let recovered = deployment.start().await;
    assert_eq!(
        recovered.status().await,
        ReplicaStatus::InitializedPopulated {
            replica_id: after_activation.replica_id,
        }
    );
    assert_eq!(
        identity::read_identity_bytes(&deployment.config_root)
            .unwrap()
            .unwrap(),
        committed_new_file
    );
    assert!(
        recovered
            .store
            .identity_transition()
            .await
            .unwrap()
            .is_none()
    );
    shutdown_runtime(&recovered).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_replica_identity_edit_updates_sql_before_publication_and_invalid_edit_is_retained() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/identity.md"), "hot identity").unwrap();
    let runtime = deployment.start().await;
    let previous = runtime.state.read().await.as_ref().unwrap().replica_id;
    let replacement = Uuid::new_v4();
    identity::ReplicaIdentity::new(replacement)
        .write(&deployment.config_root)
        .unwrap();
    assert!(
        runtime
            .reload_replica_identity("replica-identity-hot-edit")
            .await
            .unwrap()
    );
    assert_ne!(previous, replacement);
    assert_eq!(
        runtime
            .store
            .load_active()
            .await
            .unwrap()
            .unwrap()
            .replica_id,
        replacement
    );
    assert_eq!(runtime.identities.epoch(), 1);

    fs::write(identity::identity_path(&deployment.config_root), b"{").unwrap();
    assert!(matches!(
        runtime
            .reload_replica_identity("replica-identity-invalid-edit")
            .await,
        Err(ReplicaError::Configuration(_))
    ));
    assert_eq!(
        runtime.state.read().await.as_ref().unwrap().replica_id,
        replacement
    );
    assert_eq!(
        runtime
            .store
            .load_active()
            .await
            .unwrap()
            .unwrap()
            .replica_id,
        replacement
    );
    shutdown_runtime(&runtime).await;
}
