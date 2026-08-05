use super::*;

#[test]
fn store_diagnostics_do_not_expose_database_error_details() {
    let secret = "postgresql://user:secret@example.invalid/plugins";
    let diagnostic = PackageDiagnostic::store(PluginError::Store(secret.to_owned()));

    assert_eq!(diagnostic.code, "install_publish_failed");
    assert_eq!(diagnostic.phase, "store");
    assert_eq!(
        diagnostic.message,
        "plugin package state could not be committed"
    );
    assert!(!diagnostic.message.contains(secret));
}

#[tokio::test]
async fn exact_reconcile_removes_an_undeclared_name_owner_before_handoff() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let old_id: PluginId = "oll.handoff-old".parse().unwrap();
    let new_id: PluginId = "oll.handoff-new".parse().unwrap();
    let shared_name: PluginName = "handoff-name".parse().unwrap();
    let old_declaration = test_declaration("handoff-old");
    let new_declaration = test_declaration("handoff-new");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(new_id.clone(), new_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let old = seed_installed_plugin(
        &store,
        &layout,
        old_id.clone(),
        shared_name.clone(),
        &old_declaration,
    )
    .await;
    let candidate = resolve_local_candidate(
        directory.path(),
        &manager,
        new_id.clone(),
        shared_name.clone(),
        new_declaration,
        None,
    )
    .await;
    let plan = PackageManager::partition_exact_reconcile(
        &declarations,
        vec![old],
        vec![candidate],
        Vec::new(),
    );
    assert!(plan.independent.0.is_empty());
    assert_eq!(plan.removals.len(), 1);
    assert_eq!(plan.removals[0].dependents.0.len(), 1);

    let removal_manager = manager.clone();
    let results = manager
        .execute_exact_reconcile_plan(plan, move |plugin_id| {
            let manager = removal_manager.clone();
            async move {
                let preparation = manager.begin_removal(&plugin_id, "name-handoff").await?;
                manager.finish_removal(preparation).await
            }
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        result_for(&results, &old_id).outcome,
        PackageOperationOutcome::Removed
    );
    assert_eq!(
        result_for(&results, &new_id).outcome,
        PackageOperationOutcome::Installed
    );
    assert!(matches!(
        store.get_plugin(&PluginSelector::Id(old_id.clone())).await,
        Err(PluginError::NotFound(_))
    ));
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(new_id))
            .await
            .unwrap()
            .plugin_name,
        shared_name
    );
}

#[tokio::test]
async fn exact_reconcile_removal_failure_blocks_only_its_name_dependents() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let old_id: PluginId = "oll.failed-owner".parse().unwrap();
    let dependent_id: PluginId = "oll.failed-dependent".parse().unwrap();
    let independent_id: PluginId = "oll.failure-independent".parse().unwrap();
    let shared_name: PluginName = "failed-handoff".parse().unwrap();
    let independent_name: PluginName = "failure-independent".parse().unwrap();
    let old_declaration = test_declaration("failed-owner");
    let dependent_declaration = test_declaration("failed-dependent");
    let independent_declaration = test_declaration("failure-independent");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(dependent_id.clone(), dependent_declaration.clone());
    declarations.insert(independent_id.clone(), independent_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let old = seed_installed_plugin(
        &store,
        &layout,
        old_id.clone(),
        shared_name.clone(),
        &old_declaration,
    )
    .await;
    let dependent = resolve_local_candidate(
        directory.path(),
        &manager,
        dependent_id.clone(),
        shared_name,
        dependent_declaration,
        None,
    )
    .await;
    let independent = resolve_local_candidate(
        directory.path(),
        &manager,
        independent_id.clone(),
        independent_name,
        independent_declaration,
        None,
    )
    .await;
    let plan = PackageManager::partition_exact_reconcile(
        &declarations,
        vec![old],
        vec![dependent, independent],
        Vec::new(),
    );

    let results = manager
        .execute_exact_reconcile_plan(plan, |_| async {
            Err(PluginError::Store(
                "postgresql://user:do-not-log@example.invalid/private".to_owned(),
            ))
        })
        .await
        .unwrap();

    assert_eq!(
        result_for(&results, &old_id).outcome,
        PackageOperationOutcome::Failed
    );
    assert_eq!(
        result_for(&results, &old_id).diagnostics[0].code,
        "install_publish_failed"
    );
    assert_eq!(
        result_for(&results, &dependent_id).outcome,
        PackageOperationOutcome::Failed
    );
    assert_eq!(
        result_for(&results, &dependent_id).diagnostics[0].code,
        "plugin_name_conflict"
    );
    assert_eq!(
        result_for(&results, &independent_id).outcome,
        PackageOperationOutcome::Installed
    );
    assert!(
        results
            .iter()
            .flat_map(|result| &result.diagnostics)
            .all(|diagnostic| !diagnostic.message.contains("do-not-log")
                && !diagnostic.message.contains("private"))
    );
    assert!(
        store
            .get_plugin(&PluginSelector::Id(independent_id))
            .await
            .is_ok()
    );
    assert!(matches!(
        store.get_plugin(&PluginSelector::Id(dependent_id)).await,
        Err(PluginError::NotFound(_))
    ));
}

#[tokio::test]
async fn exact_reconcile_slow_unrelated_removal_does_not_delay_publication() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let old_id: PluginId = "oll.slow-removal".parse().unwrap();
    let new_id: PluginId = "oll.fast-publication".parse().unwrap();
    let old_name: PluginName = "slow-removal".parse().unwrap();
    let new_name: PluginName = "fast-publication".parse().unwrap();
    let old_declaration = test_declaration("slow-removal");
    let new_declaration = test_declaration("fast-publication");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(new_id.clone(), new_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let old =
        seed_installed_plugin(&store, &layout, old_id.clone(), old_name, &old_declaration).await;
    let candidate = resolve_local_candidate(
        directory.path(),
        &manager,
        new_id.clone(),
        new_name,
        new_declaration,
        None,
    )
    .await;
    let plan = PackageManager::partition_exact_reconcile(
        &declarations,
        vec![old],
        vec![candidate],
        Vec::new(),
    );
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let operation = tokio::spawn({
        let manager = manager.clone();
        let removal_manager = manager.clone();
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        async move {
            manager
                .execute_exact_reconcile_plan(plan, move |plugin_id| {
                    let manager = removal_manager.clone();
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        let preparation = manager
                            .begin_removal(&plugin_id, "slow-unrelated-removal")
                            .await?;
                        manager.finish_removal(preparation).await
                    }
                })
                .await
        }
    });
    entered.acquire().await.unwrap().forget();

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if store
                .get_plugin(&PluginSelector::Id(new_id.clone()))
                .await
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("independent publication waited for an unrelated removal");
    assert!(!operation.is_finished());
    release.add_permits(1);
    let results = operation.await.unwrap().unwrap();
    assert_eq!(
        result_for(&results, &old_id).outcome,
        PackageOperationOutcome::Removed
    );
    assert_eq!(
        result_for(&results, &new_id).outcome,
        PackageOperationOutcome::Installed
    );
}

#[tokio::test]
async fn exact_reconcile_declared_name_swap_fails_stably_before_build() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let first_id: PluginId = "oll.swap-first".parse().unwrap();
    let second_id: PluginId = "oll.swap-second".parse().unwrap();
    let first_name: PluginName = "swap-first".parse().unwrap();
    let second_name: PluginName = "swap-second".parse().unwrap();
    let first_declaration = test_declaration("swap-first");
    let second_declaration = test_declaration("swap-second");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(first_id.clone(), first_declaration.clone());
    declarations.insert(second_id.clone(), second_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let first = seed_installed_plugin(
        &store,
        &layout,
        first_id.clone(),
        first_name.clone(),
        &first_declaration,
    )
    .await;
    let second = seed_installed_plugin(
        &store,
        &layout,
        second_id.clone(),
        second_name.clone(),
        &second_declaration,
    )
    .await;
    let first_candidate = resolve_local_candidate(
        directory.path(),
        &manager,
        first_id.clone(),
        second_name,
        first_declaration,
        Some(&["/bin/false"]),
    )
    .await;
    let second_candidate = resolve_local_candidate(
        directory.path(),
        &manager,
        second_id.clone(),
        first_name,
        second_declaration,
        Some(&["/bin/false"]),
    )
    .await;
    let plan = PackageManager::partition_exact_reconcile(
        &declarations,
        vec![first, second],
        vec![first_candidate, second_candidate],
        Vec::new(),
    );
    assert!(plan.independent.0.is_empty());
    assert!(plan.removals.is_empty());

    let removal_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let called = Arc::clone(&removal_called);
    let results = manager
        .execute_exact_reconcile_plan(plan, move |_| {
            called.store(true, std::sync::atomic::Ordering::Release);
            async {
                Err(PluginError::FailedPrecondition(
                    "unexpected removal".to_owned(),
                ))
            }
        })
        .await
        .unwrap();

    assert!(!removal_called.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| {
        result.outcome == PackageOperationOutcome::Failed
            && result.diagnostics[0].code == "plugin_name_conflict"
    }));
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(first_id))
            .await
            .unwrap()
            .plugin_name
            .as_str(),
        "swap-first"
    );
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(second_id))
            .await
            .unwrap()
            .plugin_name
            .as_str(),
        "swap-second"
    );
}

#[test]
fn mask_paths_remain_keyed_by_plugin_id() {
    let root = Path::new("/config");
    let plugin_id: PluginId = "oll.test".parse().unwrap();
    assert_eq!(
        crate::plugin::package::mask_path(root, &plugin_id),
        root.join("plugin-masks/oll.test.toml")
    );
}
