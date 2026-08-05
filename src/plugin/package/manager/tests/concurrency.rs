use super::*;

#[tokio::test]
async fn package_gates_serialize_one_id_but_not_distinct_ids() {
    let gates = Arc::new(PluginPackageGates::default());
    let first: PluginId = "oll.first".parse().unwrap();
    let second: PluginId = "oll.second".parse().unwrap();
    let held = gates.lock(&first).await;
    assert!(gates.entries.lock().await.contains_key(&first));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), gates.lock(&second))
            .await
            .is_ok()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), gates.lock(&first))
            .await
            .is_err()
    );
    drop(held);
}

#[tokio::test]
async fn shutdown_rejects_an_install_already_waiting_for_its_package_gate() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let plugin_id: PluginId = "oll.shutdown-admission".parse().unwrap();
    let declaration = test_declaration("shutdown-admission");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration);
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, shutdown) = test_manager(directory.path(), &config_root).await;
    let held = manager.gates().lock(&plugin_id).await;
    let mut operation = tokio::spawn({
        let manager = manager.clone();
        async move { manager.install_declared("shutdown-admission-race").await }
    });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut operation)
            .await
            .is_err(),
        "install should be admitted and waiting for the held per-plugin gate"
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    shutdown.send_replace(Some(deadline));
    manager.shutdown(deadline).await.unwrap();
    drop(held);

    let results = operation.await.unwrap().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].plugin_id.as_ref(), Some(&plugin_id));
    assert_eq!(results[0].outcome, PackageOperationOutcome::Failed);
    assert!(
        results[0]
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.message == "plugin package manager is shutting down" })
    );
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
    assert!(matches!(
        manager.install_declared("after-shutdown").await,
        Err(PluginError::FailedPrecondition(_))
    ));
}

#[tokio::test]
async fn blocked_recipe_does_not_delay_another_plugin_publication() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let blocked_id: PluginId = "oll.concurrent-blocked".parse().unwrap();
    let fast_id: PluginId = "oll.concurrent-fast".parse().unwrap();
    let blocked_name: PluginName = "concurrent-blocked".parse().unwrap();
    let fast_name: PluginName = "concurrent-fast".parse().unwrap();
    let blocked_declaration = test_declaration(blocked_name.as_str());
    let fast_declaration = test_declaration(fast_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(blocked_id.clone(), blocked_declaration.clone());
    declarations.insert(fast_id.clone(), fast_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (_store, _layout, manager, shutdown) = test_manager(directory.path(), &config_root).await;
    let blocked = resolve_local_candidate(
        directory.path(),
        &manager,
        blocked_id.clone(),
        blocked_name,
        blocked_declaration,
        Some(&["/bin/sleep", "60"]),
    )
    .await;
    let fast = resolve_local_candidate(
        directory.path(),
        &manager,
        fast_id.clone(),
        fast_name,
        fast_declaration,
        None,
    )
    .await;
    let store = manager.store.clone();
    let operation = tokio::spawn({
        let manager = manager.clone();
        async move { manager.publish_resolved_set(vec![blocked, fast]).await }
    });

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if store
                .get_plugin(&PluginSelector::Id(fast_id.clone()))
                .await
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fast plugin did not publish while another recipe remained blocked");
    assert!(!operation.is_finished());
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(blocked_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));

    shutdown.send_replace(Some(
        tokio::time::Instant::now() + std::time::Duration::from_secs(2),
    ));
    let results = tokio::time::timeout(std::time::Duration::from_secs(3), operation)
        .await
        .expect("blocked recipe did not observe package shutdown")
        .unwrap()
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].plugin_id.as_ref(), Some(&blocked_id));
    assert_eq!(results[0].outcome, PackageOperationOutcome::Failed);
    assert_eq!(results[1].plugin_id.as_ref(), Some(&fast_id));
    assert_eq!(results[1].outcome, PackageOperationOutcome::Installed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_package_client_logs_and_cleans_the_active_build_phase() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    let plugin_id: PluginId = "oll.cancelled-client-build".parse().unwrap();
    let plugin_name: PluginName = "cancelled-client-build".parse().unwrap();
    let declaration = test_declaration("cancelled-client-build");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    fs::write(
        checkout_root.join("oll.toml"),
        test_publisher_with_step(&plugin_id, &plugin_name, Some(&["/bin/sleep", "60"])),
    )
    .unwrap();
    let (_store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let operation_id = Uuid::new_v4().to_string();
    let candidate = match manager
        .resolve_candidate(
            plugin_id.clone(),
            declaration,
            false,
            Some((
                operation_id.clone(),
                GitCheckout {
                    source_root: checkout_root,
                    commit: "1".repeat(40),
                },
            )),
            InstalledResolution::Lookup,
            "client-disconnect-build",
        )
        .await
    {
        Resolved::Candidate(candidate) => *candidate,
        Resolved::Result(result) => panic!("candidate resolution failed: {result:#?}"),
    };
    let build_log = layout.build_log(&plugin_id, &operation_id).unwrap();
    let client = tokio::spawn({
        let manager = manager.clone();
        async move { manager.publish_resolved_set(vec![candidate]).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if build_log.exists() {
                break;
            }
            assert!(
                !client.is_finished(),
                "source build ended before cancellation"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source recipe did not start");

    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    manager
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
        .await
        .unwrap();
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();

    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    let cancelled = lifecycle_log
        .lines()
        .filter(|line| {
            line.contains("plugin_package_candidate_build_failed")
                && line.contains("client-disconnect-build")
        })
        .collect::<Vec<_>>();
    assert_eq!(cancelled.len(), 1);
    assert!(cancelled[0].contains("operation_cancelled"));
    assert!(cancelled[0].contains(&operation_id));
    assert!(
        fs::read_dir(layout.plugin_root(&plugin_id))
            .unwrap()
            .all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".operation-")
            })
    );
    assert_eq!(
        fs::read_dir(layout.plugin_root(&plugin_id).join("candidates"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn effective_name_conflicts_fail_every_conflicting_plugin_before_build() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let first_id: PluginId = "oll.conflict-first".parse().unwrap();
    let second_id: PluginId = "oll.conflict-second".parse().unwrap();
    let shared_name: PluginName = "shared-name".parse().unwrap();
    let first_declaration = test_declaration("conflict-first");
    let second_declaration = test_declaration("conflict-second");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(first_id.clone(), first_declaration.clone());
    declarations.insert(second_id.clone(), second_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let first = resolve_local_candidate(
        directory.path(),
        &manager,
        first_id.clone(),
        shared_name.clone(),
        first_declaration,
        Some(&["/bin/false"]),
    )
    .await;
    let second = resolve_local_candidate(
        directory.path(),
        &manager,
        second_id.clone(),
        shared_name,
        second_declaration,
        None,
    )
    .await;

    let results = manager
        .publish_resolved_set(vec![first, second])
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| {
        result.outcome == PackageOperationOutcome::Failed
            && result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "plugin_name_conflict")
    }));
    for plugin_id in [&first_id, &second_id] {
        assert!(matches!(
            store
                .get_plugin(&PluginSelector::Id(plugin_id.clone()))
                .await,
            Err(PluginError::NotFound(_))
        ));
        assert_eq!(layout.current_generation(plugin_id).unwrap(), None);
    }
}

#[tokio::test]
async fn one_recipe_failure_does_not_discard_another_plugins_success() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let failed_id: PluginId = "oll.partial-failed".parse().unwrap();
    let succeeded_id: PluginId = "oll.partial-succeeded".parse().unwrap();
    let failed_name: PluginName = "partial-failed".parse().unwrap();
    let succeeded_name: PluginName = "partial-succeeded".parse().unwrap();
    let failed_declaration = test_declaration(failed_name.as_str());
    let succeeded_declaration = test_declaration(succeeded_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(failed_id.clone(), failed_declaration.clone());
    declarations.insert(succeeded_id.clone(), succeeded_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, _layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let failed = resolve_local_candidate(
        directory.path(),
        &manager,
        failed_id.clone(),
        failed_name,
        failed_declaration,
        Some(&["/bin/false"]),
    )
    .await;
    let succeeded = resolve_local_candidate(
        directory.path(),
        &manager,
        succeeded_id.clone(),
        succeeded_name,
        succeeded_declaration,
        None,
    )
    .await;

    let results = manager
        .publish_resolved_set(vec![failed, succeeded])
        .await
        .unwrap();

    assert_eq!(results[0].plugin_id.as_ref(), Some(&failed_id));
    assert_eq!(results[0].outcome, PackageOperationOutcome::Failed);
    assert_eq!(results[1].plugin_id.as_ref(), Some(&succeeded_id));
    assert_eq!(results[1].outcome, PackageOperationOutcome::Installed);
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(failed_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));
    assert!(
        store
            .get_plugin(&PluginSelector::Id(succeeded_id))
            .await
            .is_ok()
    );
}
