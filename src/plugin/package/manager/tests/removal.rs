use super::*;

#[test]
fn removal_dependency_diagnostics_do_not_expose_internal_error_details() {
    let plugin_id: PluginId = "oll.secret-owner".parse().unwrap();
    let plugin_name: PluginName = "secret-name".parse().unwrap();
    let declaration = test_declaration("secret-dependent");
    let error = PluginError::Store(
        "postgresql://user:do-not-log@example.invalid/private-database".to_owned(),
    );

    let diagnostic = PackageDiagnostic::blocked_by_removal(&plugin_name, &plugin_id, &declaration);

    assert_eq!(diagnostic.code, "plugin_name_conflict");
    assert_eq!(diagnostic.phase, "removal");
    assert!(!diagnostic.message.contains(error.code()));
    assert!(!diagnostic.message.contains("do-not-log"));
    assert!(!diagnostic.message.contains("private-database"));
}

#[tokio::test]
async fn removal_recovery_recognizes_an_already_published_declaration_file() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let plugin_id: PluginId = "oll.remove".parse().unwrap();
    let declaration = PluginDeclaration {
        remote: "https://example.com/oll-remove.git".to_owned(),
        mode: super::super::DeclarationMode::Source,
        selection: GitSelection::Default,
        release: None,
    };
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration);
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let generation = Uuid::new_v4();
    let plugin_name: PluginName = "remove".parse().unwrap();
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
        declarations.get(&plugin_id).unwrap(),
        &publisher,
    );
    store.prepare_package_publish(&intent).await.unwrap();
    layout
        .publish_candidate(&plugin_id, generation, None)
        .unwrap();
    store
        .finalize_package_publish(&plugin_id, generation)
        .await
        .unwrap();

    let preparation = manager
        .begin_removal(&plugin_id, "removal-correlation")
        .await
        .unwrap();
    crate::node::identity::atomic_write(
        &config_root.join("plugins.lua"),
        &preparation.intent.prepared_plugins_lua,
    )
    .unwrap();
    drop(preparation);

    manager.recover("package-test-startup").await.unwrap();
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));
    assert!(!layout.plugin_root(&plugin_id).exists());
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    for event in [
        "plugin_package_removal_prepared",
        "plugin_package_removal_started",
        "plugin_package_removal_succeeded",
        "plugin_package_removal_recovery_succeeded",
    ] {
        assert!(
            lifecycle_log.contains(event),
            "missing removal event {event}"
        );
    }
    assert!(
        lifecycle_log
            .lines()
            .filter(|line| line.contains("plugin_package_removal_"))
            .all(|line| line.contains("\"correlation_id\":\"removal-correlation\""))
    );
}
