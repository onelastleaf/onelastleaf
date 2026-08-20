use super::*;

#[tokio::test]
async fn source_checkout_can_build_in_install_or_final_generation() {
    use std::os::unix::fs::PermissionsExt;

    for (suffix, checkout, root_placeholder, direct) in [
        ("install", "install", "{install}", false),
        ("generation", "generation", "{generation}", true),
    ] {
        let directory = tempfile::TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let checkout_root = directory.path().join("checkout");
        fs::create_dir(&config_root).unwrap();
        fs::create_dir(&checkout_root).unwrap();
        let plugin_id: PluginId = format!("oll.layout-{suffix}").parse().unwrap();
        let plugin_name: PluginName = format!("layout-{suffix}").parse().unwrap();
        let declaration = test_declaration(plugin_name.as_str());
        let mut declarations = PluginDeclarations::default();
        declarations.insert(plugin_id.clone(), declaration.clone());
        write_plugin_declarations(&config_root, &declarations).unwrap();
        let publisher = format!(
            r#"format_version = 1
[plugin]
id = "{plugin_id}"
name = "{plugin_name}"
[source]
checkout = "{checkout}"
steps = [["{root_placeholder}/entry", "{root_placeholder}/built-at"]]
[runtime]
argv = ["{root_placeholder}/entry"]
"#
        );
        fs::write(checkout_root.join("oll.toml"), publisher).unwrap();
        fs::write(checkout_root.join("entry"), b"#!/bin/sh\npwd > \"$1\"\n").unwrap();
        fs::set_permissions(
            checkout_root.join("entry"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(checkout_root.join("source-marker"), b"retained").unwrap();
        let (_store, layout, manager, _shutdown) =
            test_manager(directory.path(), &config_root).await;

        let result = manager
            .finish_single(
                manager
                    .prepare_candidate(
                        plugin_id.clone(),
                        declaration,
                        false,
                        Some((
                            Uuid::new_v4().to_string(),
                            GitCheckout {
                                source_root: checkout_root.clone(),
                                commit: "1".repeat(40),
                            },
                        )),
                        "checkout-layout-test",
                    )
                    .await,
            )
            .await;
        assert_eq!(result.outcome, PackageOperationOutcome::Installed);
        let generation = layout.current_generation(&plugin_id).unwrap().unwrap();
        let published = layout.generation(&plugin_id, generation);
        assert_eq!(
            fs::read(published.join("source-marker")).unwrap(),
            b"retained"
        );
        let built_at = fs::read_to_string(published.join("built-at")).unwrap();
        if direct {
            assert_eq!(built_at.trim(), published.to_str().unwrap());
        } else {
            assert!(built_at.contains("/candidates/"));
        }
        assert!(!checkout_root.exists());
        assert!(
            fs::read_dir(layout.plugin_root(&plugin_id).join("candidates"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[tokio::test]
async fn failed_direct_generation_build_is_removed_immediately() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    let plugin_id: PluginId = "oll.failed-generation".parse().unwrap();
    let plugin_name: PluginName = "failed-generation".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    fs::write(
        checkout_root.join("oll.toml"),
        format!(
            r#"format_version = 1
[plugin]
id = "{plugin_id}"
name = "{plugin_name}"
[source]
checkout = "generation"
steps = [["/bin/false"]]
[runtime]
argv = ["{{generation}}/entry"]
"#
        ),
    )
    .unwrap();
    fs::write(checkout_root.join("entry"), b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(
        checkout_root.join("entry"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let (_store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    let result = manager
        .finish_single(
            manager
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
                    "failed-generation-test",
                )
                .await,
        )
        .await;
    assert_eq!(result.outcome, PackageOperationOutcome::Failed);
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
    assert!(
        fs::read_dir(layout.plugin_root(&plugin_id).join("generations"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn publication_rejects_declaration_and_mask_changes_after_build() {
    for change_mask in [false, true] {
        let directory = tempfile::TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let checkout_root = directory.path().join("checkout");
        fs::create_dir(&config_root).unwrap();
        fs::create_dir(&checkout_root).unwrap();
        let plugin_id: PluginId = if change_mask {
            "oll.stale-mask".parse().unwrap()
        } else {
            "oll.stale-declaration".parse().unwrap()
        };
        let plugin_name: PluginName = if change_mask {
            "stale-mask".parse().unwrap()
        } else {
            "stale-declaration".parse().unwrap()
        };
        let declaration = test_declaration(plugin_name.as_str());
        let mut declarations = PluginDeclarations::default();
        declarations.insert(plugin_id.clone(), declaration.clone());
        write_plugin_declarations(&config_root, &declarations).unwrap();
        fs::write(
            checkout_root.join("oll.toml"),
            test_publisher(&plugin_id, &plugin_name),
        )
        .unwrap();
        let (store, layout, manager, _shutdown) =
            test_manager(directory.path(), &config_root).await;

        let prepared = manager
            .prepare_candidate(
                plugin_id.clone(),
                declaration.clone(),
                false,
                Some((
                    Uuid::new_v4().to_string(),
                    GitCheckout {
                        source_root: checkout_root,
                        commit: "1".repeat(40),
                    },
                )),
                "stale-input-race",
            )
            .await;
        let generation = match &prepared {
            Prepared::Candidate(candidate) => candidate.built.generation,
            Prepared::Result(result) => panic!("candidate build failed: {result:#?}"),
        };
        if change_mask {
            fs::create_dir(config_root.join("plugin-masks")).unwrap();
            fs::write(
                crate::plugin::package::mask_path(&config_root, &plugin_id),
                b"format_version = 1\n[plugin]\nname = \"changed-mask\"\n",
            )
            .unwrap();
        } else {
            declarations.insert(
                plugin_id.clone(),
                PluginDeclaration {
                    selection: GitSelection::Branch("changed".to_owned()),
                    ..declaration
                },
            );
            write_plugin_declarations(&config_root, &declarations).unwrap();
        }

        let result = manager.finish_single(prepared).await;
        assert_eq!(result.outcome, PackageOperationOutcome::Failed);
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
}

#[tokio::test]
async fn candidate_sync_failure_never_creates_a_publish_intent() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    let plugin_id: PluginId = "oll.sync-failure".parse().unwrap();
    let plugin_name: PluginName = "sync-failure".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    fs::write(
        checkout_root.join("oll.toml"),
        test_publisher(&plugin_id, &plugin_name),
    )
    .unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
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
            "candidate-sync-failure",
        )
        .await;
    let generation = match &prepared {
        Prepared::Candidate(candidate) => candidate.built.generation,
        Prepared::Result(result) => panic!("candidate build failed: {result:#?}"),
    };
    layout.remove_candidate(&plugin_id, generation);

    let result = manager.finish_single(prepared).await;

    assert_eq!(result.outcome, PackageOperationOutcome::Failed);
    assert!(store.package_publish_intents().await.unwrap().is_empty());
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
}

#[tokio::test]
async fn aborted_client_future_does_not_abandon_a_durable_publication() {
    for pause in [PublishPause::AfterIntent, PublishPause::AfterCurrentSwitch] {
        let directory = tempfile::TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let checkout_root = directory.path().join("checkout");
        fs::create_dir(&config_root).unwrap();
        fs::create_dir(&checkout_root).unwrap();
        let plugin_id: PluginId = match pause {
            PublishPause::AfterIntent => "oll.abort-before-switch".parse().unwrap(),
            PublishPause::AfterCurrentSwitch => "oll.abort-after-switch".parse().unwrap(),
            PublishPause::PanicAfterIntent => unreachable!(),
        };
        let plugin_name: PluginName = match pause {
            PublishPause::AfterIntent => "abort-before-switch".parse().unwrap(),
            PublishPause::AfterCurrentSwitch => "abort-after-switch".parse().unwrap(),
            PublishPause::PanicAfterIntent => unreachable!(),
        };
        let declaration = test_declaration(plugin_name.as_str());
        let mut declarations = PluginDeclarations::default();
        declarations.insert(plugin_id.clone(), declaration.clone());
        write_plugin_declarations(&config_root, &declarations).unwrap();
        fs::write(
            checkout_root.join("oll.toml"),
            test_publisher(&plugin_id, &plugin_name),
        )
        .unwrap();
        let (store, layout, manager, _shutdown) =
            test_manager(directory.path(), &config_root).await;
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
                "aborted-package-client",
            )
            .await;
        let generation = match &prepared {
            Prepared::Candidate(candidate) => candidate.built.generation,
            Prepared::Result(result) => panic!("candidate build failed: {result:#?}"),
        };
        let hook = PublishTestHook::new(pause);
        manager.set_publish_test_hook(Arc::clone(&hook)).await;
        let client = tokio::spawn({
            let manager = manager.clone();
            async move { manager.finish_single(prepared).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), hook.wait_until_reached())
            .await
            .expect("durable publication did not reach the requested boundary");
        assert!(
            store
                .package_publish_intent(&plugin_id)
                .await
                .unwrap()
                .is_some()
        );
        match pause {
            PublishPause::AfterIntent => {
                assert_eq!(layout.current_generation(&plugin_id).unwrap(), None)
            }
            PublishPause::AfterCurrentSwitch => assert_eq!(
                layout.current_generation(&plugin_id).unwrap(),
                Some(generation)
            ),
            PublishPause::PanicAfterIntent => unreachable!(),
        }

        client.abort();
        assert!(client.await.unwrap_err().is_cancelled());
        hook.resume();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(installed) = store
                    .get_plugin(&PluginSelector::Id(plugin_id.clone()))
                    .await
                    && installed.current_generation == generation
                    && layout.current_generation(&plugin_id).unwrap() == Some(generation)
                    && store
                        .package_publish_intent(&plugin_id)
                        .await
                        .unwrap()
                        .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable publication was abandoned after client cancellation");
        manager
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn panicked_durable_publication_is_reaped_logged_and_left_for_recovery() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    let plugin_id: PluginId = "oll.publish-panic".parse().unwrap();
    let plugin_name: PluginName = "publish-panic".parse().unwrap();
    let declaration = test_declaration(plugin_name.as_str());
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    fs::write(
        checkout_root.join("oll.toml"),
        test_publisher(&plugin_id, &plugin_name),
    )
    .unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
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
            "durable-publication-panic",
        )
        .await;
    let generation = match &prepared {
        Prepared::Candidate(candidate) => candidate.built.generation,
        Prepared::Result(result) => panic!("candidate build failed: {result:#?}"),
    };
    manager
        .set_publish_test_hook(PublishTestHook::new(PublishPause::PanicAfterIntent))
        .await;

    let result = manager.finish_single(prepared).await;

    assert_eq!(result.outcome, PackageOperationOutcome::Failed);
    let intent = store
        .package_publish_intent(&plugin_id)
        .await
        .unwrap()
        .expect("post-intent panic must retain recovery state");
    assert_eq!(intent.candidate_generation, generation);
    assert_eq!(intent.correlation_id, "durable-publication-panic");
    assert!(layout.pending_generation(&plugin_id, generation).is_some());
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
    let shutdown = manager
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await;
    assert!(matches!(shutdown, Err(PluginError::FailedPrecondition(_))));
    manager
        .package_tasks
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let logs = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    assert!(logs.contains("plugin_package_publication_task_failed"));
    assert!(logs.contains("durable-publication-panic"));
    assert!(logs.contains("task_panicked"));
    assert!(!logs.contains("injected durable package publication panic"));
}

#[tokio::test]
async fn source_publication_keeps_a_running_generation_and_failed_update_keeps_current() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    fs::write(config_root.join("plugins.lua"), b"return {}\n").unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let plugin_id: PluginId = "oll.package-test".parse().unwrap();
    let declaration = PluginDeclaration {
        remote: "https://example.com/oll-package-test.git".to_owned(),
        mode: DeclarationMode::Source,
        selection: GitSelection::Branch("main".to_owned()),
        release: None,
    };
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let manifest = |step: &str| {
        format!(
            r#"format_version = 1
[plugin]
id = "oll.package-test"
name = "package-test"
[source]
checkout = "source"
steps = [
  ["/bin/mkdir", "-p", "{{install}}/bin"],
  ["{step}", "{{source}}/entry", "{{install}}/bin/plugin"],
]
[runtime]
argv = ["{{install}}/bin/plugin"]
"#
        )
    };
    fs::write(checkout_root.join("entry"), b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(
        checkout_root.join("entry"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    fs::write(checkout_root.join("oll.toml"), manifest("/bin/cp")).unwrap();

    let first = manager
        .prepare_candidate(
            plugin_id.clone(),
            declaration.clone(),
            false,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root.clone(),
                    commit: "1".repeat(40),
                },
            )),
            "package-e2e",
        )
        .await;
    let first = manager.finish_single(first).await;
    assert_eq!(
        first.outcome,
        PackageOperationOutcome::Installed,
        "{:#?}",
        first.diagnostics
    );
    let first_generation = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap()
        .current_generation;
    let instance_id = crate::plugin::PluginInstanceId::new();
    store
        .set_desired_state(&plugin_id, crate::plugin::DesiredPluginState::Running)
        .await
        .unwrap();
    store
        .record_running_instance(&plugin_id, first_generation, instance_id)
        .await
        .unwrap();

    fs::write(checkout_root.join("entry"), b"#!/bin/sh\nexit 7\n").unwrap();
    let second = manager
        .prepare_candidate(
            plugin_id.clone(),
            declaration.clone(),
            true,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root.clone(),
                    commit: "2".repeat(40),
                },
            )),
            "package-e2e",
        )
        .await;
    let second = manager.finish_single(second).await;
    assert_eq!(second.outcome, PackageOperationOutcome::Updated);
    let installed = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    assert_ne!(installed.current_generation, first_generation);
    assert_eq!(installed.running_generation, Some(first_generation));
    assert!(layout.generation(&plugin_id, first_generation).exists());
    assert_eq!(
        fs::read(
            layout
                .generation(&plugin_id, installed.current_generation)
                .join("bin/plugin")
        )
        .unwrap(),
        b"#!/bin/sh\nexit 7\n"
    );

    fs::write(checkout_root.join("oll.toml"), manifest("/bin/false")).unwrap();
    let failed = manager
        .prepare_candidate(
            plugin_id.clone(),
            declaration,
            true,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root,
                    commit: "3".repeat(40),
                },
            )),
            "package-e2e",
        )
        .await;
    let failed = manager.finish_single(failed).await;
    assert_eq!(failed.outcome, PackageOperationOutcome::Failed);
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .unwrap()
            .current_generation,
        installed.current_generation
    );
    assert_eq!(
        layout.current_generation(&plugin_id).unwrap(),
        Some(installed.current_generation)
    );
}
