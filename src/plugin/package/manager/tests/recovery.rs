use super::*;

#[tokio::test]
async fn recovery_publishes_a_direct_generation_and_prunes_later_orphans() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let plugin_id: PluginId = "oll.direct-recovery".parse().unwrap();
    let plugin_name: PluginName = "direct-recovery".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    let publisher = format!(
        r#"format_version = 1
[plugin]
id = "{plugin_id}"
name = "{plugin_name}"
protocol_fingerprint = "{fingerprint}"
[source]
checkout = "generation"
[runtime]
argv = ["{{generation}}/plugin"]
"#
    );
    let generation = Uuid::new_v4();
    let direct = layout.direct_generation(&plugin_id, generation).unwrap();
    fs::write(direct.join("oll.toml"), &publisher).unwrap();
    fs::write(direct.join("plugin"), b"ready").unwrap();
    let intent = package_intent(
        plugin_id.clone(),
        plugin_name,
        generation,
        None,
        &declaration,
        &publisher,
    );
    store.prepare_package_publish(&intent).await.unwrap();

    manager.recover("direct-generation-recovery").await.unwrap();
    assert_eq!(
        layout.current_generation(&plugin_id).unwrap(),
        Some(generation)
    );
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .unwrap()
            .current_generation,
        generation
    );

    let orphan = Uuid::new_v4();
    fs::write(
        layout
            .direct_generation(&plugin_id, orphan)
            .unwrap()
            .join("partial"),
        b"partial",
    )
    .unwrap();
    manager.recover("direct-generation-prune").await.unwrap();
    assert!(!layout.generation(&plugin_id, orphan).exists());
    assert!(layout.generation(&plugin_id, generation).exists());
}

#[tokio::test]
async fn recovery_completes_publication_on_both_sides_of_the_symlink_switch() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let before_id: PluginId = "oll.before".parse().unwrap();
    let after_id: PluginId = "oll.after".parse().unwrap();
    let before_declaration = test_declaration("oll-before");
    let after_declaration = test_declaration("oll-after");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(before_id.clone(), before_declaration.clone());
    declarations.insert(after_id.clone(), after_declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    for switched_before_recovery in [false, true] {
        let (plugin_id, plugin_name, declaration) = if switched_before_recovery {
            (
                after_id.clone(),
                "after".parse().unwrap(),
                &after_declaration,
            )
        } else {
            (
                before_id.clone(),
                "before".parse().unwrap(),
                &before_declaration,
            )
        };
        let generation = Uuid::new_v4();
        let publisher = test_publisher(&plugin_id, &plugin_name);
        fs::write(
            layout
                .candidate(&plugin_id, generation)
                .unwrap()
                .join("oll.toml"),
            &publisher,
        )
        .unwrap();
        let intent = package_intent(
            plugin_id.clone(),
            plugin_name,
            generation,
            None,
            declaration,
            &publisher,
        );
        store.prepare_package_publish(&intent).await.unwrap();
        if switched_before_recovery {
            layout
                .publish_candidate(&plugin_id, generation, None)
                .unwrap();
        }
        manager.recover("package-test-startup").await.unwrap();
        assert_eq!(
            store
                .get_plugin(&PluginSelector::Id(plugin_id.clone()))
                .await
                .unwrap()
                .current_generation,
            generation
        );
        assert_eq!(
            layout.current_generation(&plugin_id).unwrap(),
            Some(generation)
        );
    }
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    assert!(lifecycle_log.contains("plugin_package_publication_recovery_started"));
    assert!(lifecycle_log.contains("plugin_package_publication_recovery_succeeded"));
    assert!(
        lifecycle_log
            .lines()
            .filter(|line| line.contains("plugin_package_publication_recovery_"))
            .all(|line| line.contains("\"correlation_id\":\"test-correlation\""))
    );
}

#[tokio::test]
async fn empty_sql_recovery_removes_orphan_current_before_clean_reinstall() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    let plugin_id: PluginId = "oll.reinitialized-store".parse().unwrap();
    let plugin_name: PluginName = "reinitialized-store".parse().unwrap();
    let declaration = test_declaration("reinitialized-store");
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let orphan_generation = Uuid::new_v4();
    fs::write(
        layout
            .candidate(&plugin_id, orphan_generation)
            .unwrap()
            .join("oll.toml"),
        test_publisher(&plugin_id, &plugin_name),
    )
    .unwrap();
    layout
        .publish_candidate(&plugin_id, orphan_generation, None)
        .unwrap();
    assert_eq!(
        layout.current_generation(&plugin_id).unwrap(),
        Some(orphan_generation)
    );
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));

    manager.recover("package-test-startup").await.unwrap();
    assert!(!layout.plugin_root(&plugin_id).exists());

    fs::write(
        checkout_root.join("oll.toml"),
        test_publisher(&plugin_id, &plugin_name),
    )
    .unwrap();
    let prepared = manager
        .prepare_candidate(
            plugin_id.clone(),
            declaration,
            false,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root,
                    commit: "1".repeat(40),
                },
            )),
            "reinstall-after-empty-sql",
        )
        .await;
    let result = manager.finish_single(prepared).await;

    assert_eq!(result.outcome, PackageOperationOutcome::Installed);
    assert!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .is_ok()
    );
    assert!(layout.current_generation(&plugin_id).unwrap().is_some());
}

#[tokio::test]
async fn recovery_discards_stale_declaration_before_current_switch() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let plugin_id: PluginId = "oll.recover-stale".parse().unwrap();
    let plugin_name: PluginName = "recover-stale".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let generation = Uuid::new_v4();
    let publisher = test_publisher(&plugin_id, &plugin_name);
    fs::write(
        layout
            .candidate(&plugin_id, generation)
            .unwrap()
            .join("oll.toml"),
        &publisher,
    )
    .unwrap();
    let intent = package_intent(
        plugin_id.clone(),
        plugin_name,
        generation,
        None,
        &declaration,
        &publisher,
    );
    store.prepare_package_publish(&intent).await.unwrap();
    declarations.insert(
        plugin_id.clone(),
        PluginDeclaration {
            selection: GitSelection::Revision("2".repeat(40)),
            ..declaration
        },
    );
    write_plugin_declarations(&config_root, &declarations).unwrap();

    manager.recover("package-test-startup").await.unwrap();

    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));
    assert!(store.package_publish_intents().await.unwrap().is_empty());
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
    assert!(layout.pending_generation(&plugin_id, generation).is_none());
}

#[tokio::test]
async fn recovery_restores_previous_current_when_mask_changed_after_switch() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let plugin_id: PluginId = "oll.recover-mask".parse().unwrap();
    let plugin_name: PluginName = "recover-mask".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let publisher = test_publisher(&plugin_id, &plugin_name);

    let previous = Uuid::new_v4();
    fs::write(
        layout
            .candidate(&plugin_id, previous)
            .unwrap()
            .join("oll.toml"),
        &publisher,
    )
    .unwrap();
    let previous_intent = package_intent(
        plugin_id.clone(),
        plugin_name.clone(),
        previous,
        None,
        &declaration,
        &publisher,
    );
    store
        .prepare_package_publish(&previous_intent)
        .await
        .unwrap();
    layout
        .publish_candidate(&plugin_id, previous, None)
        .unwrap();
    store
        .finalize_package_publish(&plugin_id, previous)
        .await
        .unwrap();

    let stale = Uuid::new_v4();
    fs::write(
        layout
            .candidate(&plugin_id, stale)
            .unwrap()
            .join("oll.toml"),
        &publisher,
    )
    .unwrap();
    let stale_intent = package_intent(
        plugin_id.clone(),
        plugin_name,
        stale,
        Some(previous),
        &declaration,
        &publisher,
    );
    store.prepare_package_publish(&stale_intent).await.unwrap();
    layout
        .publish_candidate(&plugin_id, stale, Some(previous))
        .unwrap();
    fs::create_dir(config_root.join("plugin-masks")).unwrap();
    fs::write(
        crate::plugin::package::mask_path(&config_root, &plugin_id),
        b"format_version = 1\n[plugin]\nname = \"changed-after-switch\"\n",
    )
    .unwrap();

    manager.recover("package-test-startup").await.unwrap();

    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .unwrap()
            .current_generation,
        previous
    );
    assert_eq!(
        layout.current_generation(&plugin_id).unwrap(),
        Some(previous)
    );
    assert!(store.package_publish_intents().await.unwrap().is_empty());
    assert!(layout.pending_generation(&plugin_id, stale).is_none());
    assert!(layout.generation(&plugin_id, previous).is_dir());
}
