use super::*;
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn local_source_install_borrows_the_workspace_and_explicit_updates_rebuild() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let workspace = directory.path().join("workspace");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&workspace).unwrap();
    fs::write(
        workspace.join("oll.toml"),
        r#"format_version = 1
[plugin]
id = "oll.local-source"
name = "local-source"
[source]
checkout = "source"
steps = [["/bin/cp", "{source}/entry", "{install}/plugin"]]
[runtime]
argv = ["{install}/plugin"]
"#,
    )
    .unwrap();
    fs::write(workspace.join("entry"), b"#!/bin/sh\necho first\n").unwrap();
    fs::set_permissions(workspace.join("entry"), fs::Permissions::from_mode(0o755)).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let declaration =
        PluginDeclaration::from_local(workspace.join("."), DeclarationMode::Source, None);

    let installed = manager
        .install(
            ExplicitInstallRequest {
                declaration,
                overwrite: None,
            },
            "local-source-install",
        )
        .await
        .unwrap();
    assert_eq!(installed[0].outcome, PackageOperationOutcome::Installed);
    let plugin_id: PluginId = "oll.local-source".parse().unwrap();
    let persisted = read_plugin_declarations(&config_root).unwrap();
    let persisted = persisted.get(&plugin_id).unwrap().clone();
    assert_eq!(
        persisted.local_path(),
        Some(fs::canonicalize(&workspace).unwrap().as_path())
    );
    let first = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    assert!(first.selected_commit.is_none());
    assert_eq!(
        fs::read(
            layout
                .generation(&plugin_id, first.current_generation)
                .join("plugin")
        )
        .unwrap(),
        b"#!/bin/sh\necho first\n"
    );

    let unchanged = manager
        .install_declared("local-source-declared")
        .await
        .unwrap();
    assert_eq!(
        result_for(&unchanged, &plugin_id).outcome,
        PackageOperationOutcome::AlreadySatisfied
    );

    fs::write(workspace.join("entry"), b"#!/bin/sh\necho second\n").unwrap();
    fs::set_permissions(workspace.join("entry"), fs::Permissions::from_mode(0o755)).unwrap();
    let updated = manager
        .update(
            &PluginSelector::Id(plugin_id.clone()),
            "local-source-update",
        )
        .await
        .unwrap();
    assert_eq!(updated[0].outcome, PackageOperationOutcome::Updated);
    let second = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    assert_ne!(second.current_generation, first.current_generation);
    assert_eq!(
        fs::read(
            layout
                .generation(&plugin_id, second.current_generation)
                .join("plugin")
        )
        .unwrap(),
        b"#!/bin/sh\necho second\n"
    );

    let rebuilt = manager
        .install(
            ExplicitInstallRequest {
                declaration: persisted,
                overwrite: None,
            },
            "local-source-reinstall",
        )
        .await
        .unwrap();
    assert_eq!(rebuilt[0].outcome, PackageOperationOutcome::Updated);
    assert!(workspace.join("entry").exists());
}

#[tokio::test]
async fn install_checkout_copies_local_workspace_without_owning_it() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let workspace = directory.path().join("copy-workspace");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&workspace).unwrap();
    fs::write(
        workspace.join("oll.toml"),
        r#"format_version = 1
[plugin]
id = "oll.local-copy"
name = "local-copy"
[source]
checkout = "install"
steps = [["/bin/cp", "{install}/entry", "{install}/plugin"]]
[runtime]
argv = ["{install}/plugin"]
"#,
    )
    .unwrap();
    fs::write(workspace.join("entry"), b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(workspace.join("entry"), fs::Permissions::from_mode(0o755)).unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    let results = manager
        .install(
            ExplicitInstallRequest {
                declaration: PluginDeclaration::from_local(
                    workspace.clone(),
                    DeclarationMode::Source,
                    None,
                ),
                overwrite: None,
            },
            "local-copy-install",
        )
        .await
        .unwrap();
    assert_eq!(results[0].outcome, PackageOperationOutcome::Installed);
    let plugin_id: PluginId = "oll.local-copy".parse().unwrap();
    let installed = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    let generation = layout.generation(&plugin_id, installed.current_generation);
    assert!(generation.join("entry").exists());
    assert!(generation.join("plugin").exists());
    assert!(!workspace.join("plugin").exists());
    assert!(workspace.join("entry").exists());
}

#[tokio::test]
async fn local_workspace_must_not_overlap_oll_owned_state() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    fs::write(config_root.join("oll.toml"), b"not reached").unwrap();
    let (_store, _layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    let results = manager
        .install(
            ExplicitInstallRequest {
                declaration: PluginDeclaration::from_local(
                    config_root.clone(),
                    DeclarationMode::Source,
                    None,
                ),
                overwrite: None,
            },
            "local-overlap",
        )
        .await
        .unwrap();

    assert_eq!(results[0].outcome, PackageOperationOutcome::Failed);
    assert_eq!(results[0].diagnostics[0].code, "local_path_invalid");
    assert!(!config_root.join("plugins.lua").exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_release_uses_a_workspace_relative_package_without_remote_integrity() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let workspace = directory.path().join("release-workspace");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir_all(workspace.join("build")).unwrap();
    let publisher = r#"format_version = 1
[plugin]
id = "oll.local-release"
name = "local-release"
[source]
checkout = "source"
[runtime]
argv = ["{install}/plugin"]
"#;
    fs::write(workspace.join("oll.toml"), publisher).unwrap();
    create_release_archive(&workspace.join("build/dev.tar.gz"), publisher);
    fs::write(
        workspace.join("oll-release.json"),
        serde_json::to_vec(&serde_json::json!({
            "format_version": 1,
            "plugin_id": "oll.local-release",
            "releases": {
                "dev": {
                    "packages": [{
                        "target": crate::plugin::package::local_target().unwrap(),
                        "path": "build/dev.tar.gz",
                        "archive": "tar.gz"
                    }]
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let (store, _layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    let results = manager
        .install(
            ExplicitInstallRequest {
                declaration: PluginDeclaration::from_local(
                    workspace.clone(),
                    DeclarationMode::Release,
                    None,
                ),
                overwrite: None,
            },
            "local-release-install",
        )
        .await
        .unwrap();

    assert_eq!(
        results[0].outcome,
        PackageOperationOutcome::Installed,
        "{:#?}",
        results[0].diagnostics
    );
    let installed = store
        .get_plugin(&PluginSelector::Id("oll.local-release".parse().unwrap()))
        .await
        .unwrap();
    assert_eq!(installed.release_id.as_deref(), Some("dev"));
    assert!(installed.selected_commit.is_none());
    assert!(workspace.join("build/dev.tar.gz").exists());

    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    assert!(lifecycle_log.contains("plugin_package_release_copy_succeeded"));
}
