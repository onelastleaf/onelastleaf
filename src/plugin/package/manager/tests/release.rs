use super::*;
use sha2::{Digest, Sha256};
use url::Url;

#[tokio::test]
async fn release_discovery_requires_the_inherited_correlation_id() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    fs::write(config_root.join("plugins.lua"), b"return {}\n").unwrap();
    let (_, _, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let selector = PluginSelector::Id("oll.test".parse().unwrap());

    assert!(matches!(
        manager.list_releases(&selector, "").await,
        Err(PluginError::InvalidArgument(message))
            if message == "plugin package correlation ID must not be empty"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_install_preserves_one_operation_log_and_correlation_through_publication() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let repository = create_source_repository(directory.path());
    let (_daemon, remote) = start_git_daemon(directory.path(), &repository);
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let declaration = PluginDeclaration {
        remote,
        mode: DeclarationMode::Source,
        selection: GitSelection::Branch("main".to_owned()),
        release: None,
    };

    let hook = PublishTestHook::new(PublishPause::AfterIntent);
    manager.set_publish_test_hook(Arc::clone(&hook)).await;
    let operation = tokio::spawn({
        let manager = manager.clone();
        let declaration = declaration.clone();
        async move {
            manager
                .install_remote(
                    InstallRemoteRequest {
                        declaration,
                        overwrite: None,
                    },
                    "remote-install-e2e",
                )
                .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        hook.wait_until_reached(),
    )
    .await
    .expect("system Git installation did not reach durable publication");
    let plugin_id: PluginId = "oll.remote-install-test".parse().unwrap();
    let intent = store
        .package_publish_intent(&plugin_id)
        .await
        .unwrap()
        .expect("remote installation must retain its durable publish intent");
    assert_eq!(intent.correlation_id, "remote-install-e2e");
    let build_logs = fs::read_dir(layout.plugin_root(&plugin_id).join("build-logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(build_logs.len(), 1, "Git and recipe split their build logs");
    let expected_log_name = format!("{}.log", intent.operation_id);
    assert_eq!(
        build_logs[0].file_name().unwrap().to_string_lossy(),
        expected_log_name.as_str()
    );
    assert!(
        fs::read_to_string(&build_logs[0])
            .unwrap()
            .contains("recipe-log-marker")
    );

    hook.resume();
    let results = tokio::time::timeout(std::time::Duration::from_secs(15), operation)
        .await
        .expect("system Git installation exceeded its test deadline")
        .unwrap()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].outcome,
        PackageOperationOutcome::Installed,
        "{:#?}",
        results[0].diagnostics
    );
    assert_eq!(
        read_plugin_declarations(&config_root)
            .unwrap()
            .get(&plugin_id),
        Some(&declaration)
    );
    assert_eq!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await
            .unwrap()
            .plugin_name
            .as_str(),
        "remote-install-test"
    );
    assert!(fs::read_dir(layout.root()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".discovery-")
    }));
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
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    assert!(!lifecycle_log.contains("recipe-log-marker"));
    let records = lifecycle_log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|record| record["correlation_id"] == "remote-install-e2e")
        .collect::<Vec<_>>();
    for event in [
        "plugin_package_git_selection_succeeded",
        "plugin_package_candidate_build_succeeded",
        "plugin_package_candidate_verification_succeeded",
        "plugin_package_publication_succeeded",
    ] {
        assert!(
            records.iter().any(|record| record["event"] == event),
            "missing package phase event {event}"
        );
    }
    let operation_ids = records
        .iter()
        .filter_map(|record| record["package_operation_id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(operation_ids.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_install_overwrite_is_digest_bound_across_two_rpc_calls() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let repository = create_source_repository(directory.path());
    let (_daemon, remote) = start_git_daemon(directory.path(), &repository);
    let plugin_id: PluginId = "oll.remote-install-test".parse().unwrap();
    let original = test_declaration("original");
    let concurrent = PluginDeclaration {
        remote: "https://example.com/concurrent.git".to_owned(),
        mode: DeclarationMode::Source,
        selection: GitSelection::Branch("concurrent".to_owned()),
        release: None,
    };
    let proposed = PluginDeclaration {
        remote,
        mode: DeclarationMode::Source,
        selection: GitSelection::Branch("main".to_owned()),
        release: None,
    };
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), original);
    write_plugin_declarations(&config_root, &declarations).unwrap();
    let (_store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;

    let failed_discovery = manager
        .install_remote(
            InstallRemoteRequest {
                declaration: PluginDeclaration {
                    remote: proposed.remote.clone(),
                    mode: DeclarationMode::Source,
                    selection: GitSelection::Branch("missing".to_owned()),
                    release: None,
                },
                overwrite: None,
            },
            "failed-remote-discovery",
        )
        .await
        .unwrap();
    assert_eq!(failed_discovery[0].outcome, PackageOperationOutcome::Failed);
    assert_eq!(
        failed_discovery[0].diagnostics[0].code,
        "git_selection_not_found"
    );
    assert_eq!(failed_discovery[0].diagnostics[0].build_log_path, None);

    let confirmation = manager
        .install_remote(
            InstallRemoteRequest {
                declaration: proposed.clone(),
                overwrite: None,
            },
            "overwrite-first-call",
        )
        .await
        .unwrap();
    assert_eq!(
        confirmation[0].outcome,
        PackageOperationOutcome::ConfirmationRequired
    );
    let stale_digest = confirmation[0].confirmation_digest.unwrap();

    declarations.insert(plugin_id.clone(), concurrent.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();
    assert!(matches!(
        manager
            .install_remote(
                InstallRemoteRequest {
                    declaration: proposed.clone(),
                    overwrite: Some(OverwriteAuthorization {
                        plugin_id: plugin_id.clone(),
                        expected_declaration_sha256: stale_digest,
                    }),
                },
                "overwrite-stale-second-call",
            )
            .await,
        Err(PluginError::Aborted(_))
    ));
    assert_eq!(
        read_plugin_declarations(&config_root)
            .unwrap()
            .get(&plugin_id),
        Some(&concurrent)
    );

    let refreshed = manager
        .install_remote(
            InstallRemoteRequest {
                declaration: proposed.clone(),
                overwrite: None,
            },
            "overwrite-refreshed-first-call",
        )
        .await
        .unwrap();
    let current_digest = refreshed[0].confirmation_digest.unwrap();
    let installed = manager
        .install_remote(
            InstallRemoteRequest {
                declaration: proposed.clone(),
                overwrite: Some(OverwriteAuthorization {
                    plugin_id: plugin_id.clone(),
                    expected_declaration_sha256: current_digest,
                }),
            },
            "overwrite-authorized-second-call",
        )
        .await
        .unwrap();
    assert_eq!(installed[0].outcome, PackageOperationOutcome::Installed);
    assert_eq!(
        read_plugin_declarations(&config_root)
            .unwrap()
            .get(&plugin_id),
        Some(&proposed)
    );
    assert!(fs::read_dir(layout.root()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".discovery-")
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_git_phase_event_preserves_correlation_and_redacts_remote_credentials() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    fs::create_dir(&config_root).unwrap();
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        while let Ok(Ok((mut stream, _))) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept()).await
        {
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
        }
    });
    let (_store, _layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let secret = "remote-phase-super-secret";
    let declaration = PluginDeclaration {
        remote: format!("http://user:{secret}@{address}/plugin.git"),
        mode: DeclarationMode::Source,
        selection: GitSelection::Branch("main".to_owned()),
        release: None,
    };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        manager.install_remote(
            InstallRemoteRequest {
                declaration,
                overwrite: None,
            },
            "redacted-git-failure",
        ),
    )
    .await
    .expect("failed Git operation exceeded its test deadline")
    .unwrap();
    server.abort();
    assert_eq!(result[0].outcome, PackageOperationOutcome::Failed);
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();

    assert!(lifecycle_log.contains("plugin_package_git_selection_failed"));
    assert!(lifecycle_log.contains("redacted-git-failure"));
    assert!(lifecycle_log.contains("git_fetch_failed"));
    assert!(!lifecycle_log.contains(secret));
    assert!(!lifecycle_log.contains("http://user:"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn release_publication_ignores_source_dependencies_and_preserves_current_on_mismatch() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("release-checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    fs::write(config_root.join("plugins.lua"), b"return {}\n").unwrap();
    let (store, layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let plugin_id: PluginId = "oll.release-test".parse().unwrap();
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    let publisher = format!(
        r#"format_version = 1
[plugin]
id = "oll.release-test"
name = "release-test"
protocol_fingerprint = "{fingerprint}"
[[source.dependencies]]
executable = "/definitely/missing/release-build-tool"
hint = "Only source installations require this tool."
[runtime]
argv = ["{{install}}/plugin"]
"#
    );
    fs::write(checkout_root.join("oll.toml"), &publisher).unwrap();
    let archive = directory.path().join("release.tar.gz");
    create_release_archive(&archive, &publisher);
    let bytes = fs::read(&archive).unwrap();
    let release_index = serde_json::json!({
        "format_version": 1,
        "plugin_id": plugin_id.as_str(),
        "protocol_fingerprint": fingerprint,
        "releases": {
            "opaque-v1": {
                "artifacts": [{
                    "target": crate::plugin::package::local_target().unwrap(),
                    "url": Url::from_file_path(&archive).unwrap().to_string(),
                    "archive": "tar.gz",
                    "size_bytes": bytes.len(),
                    "sha256": crate::replica::lower_hex(&Sha256::digest(&bytes)),
                }]
            }
        }
    });
    fs::write(
        checkout_root.join("oll-release.json"),
        serde_json::to_vec(&release_index).unwrap(),
    )
    .unwrap();
    let declaration = PluginDeclaration {
        remote: "https://example.com/oll-release-test.git".to_owned(),
        mode: DeclarationMode::Release,
        selection: GitSelection::Revision("1".repeat(40)),
        release: Some("opaque-v1".to_owned()),
    };
    let mut declarations = PluginDeclarations::default();
    declarations.insert(plugin_id.clone(), declaration.clone());
    write_plugin_declarations(&config_root, &declarations).unwrap();

    let prepared = manager
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
            "release-e2e",
        )
        .await;
    let published = manager.finish_single(prepared).await;
    assert_eq!(
        published.outcome,
        PackageOperationOutcome::Installed,
        "{:#?}",
        published.diagnostics
    );
    let installed = store
        .get_plugin(&PluginSelector::Id(plugin_id.clone()))
        .await
        .unwrap();
    assert_eq!(installed.release_id.as_deref(), Some("opaque-v1"));
    assert!(
        layout
            .generation(&plugin_id, installed.current_generation)
            .join("plugin")
            .exists()
    );

    fs::write(&archive, b"corrupt after release index validation input").unwrap();
    let prepared = manager
        .prepare_candidate(
            plugin_id.clone(),
            declaration,
            true,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root,
                    commit: "2".repeat(40),
                },
            )),
            "release-e2e",
        )
        .await;
    let failed = manager.finish_single(prepared).await;
    assert_eq!(failed.outcome, PackageOperationOutcome::Failed);
    assert!(
        failed
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "artifact_checksum_mismatch")
    );
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
    manager
        .logger
        .flush_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    let lifecycle_log = fs::read_to_string(directory.path().join("logs/oll.log")).unwrap();
    assert!(lifecycle_log.contains("plugin_package_release_download_succeeded"));
    assert!(lifecycle_log.contains("plugin_package_release_download_failed"));
    assert!(lifecycle_log.contains("artifact_checksum_mismatch"));
    assert!(
        lifecycle_log
            .lines()
            .filter(|line| line.contains("plugin_package_release_download_"))
            .all(|line| line.contains("\"correlation_id\":\"release-e2e\""))
    );
}

#[tokio::test]
async fn source_installation_still_requires_its_declared_build_dependencies() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("source-checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    fs::write(config_root.join("plugins.lua"), b"return {}\n").unwrap();
    let (_store, _layout, manager, _shutdown) = test_manager(directory.path(), &config_root).await;
    let plugin_id: PluginId = "oll.source-dependency-test".parse().unwrap();
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    fs::write(
        checkout_root.join("oll.toml"),
        format!(
            r#"format_version = 1
[plugin]
id = "oll.source-dependency-test"
name = "source-dependency-test"
protocol_fingerprint = "{fingerprint}"
[[source.dependencies]]
executable = "/definitely/missing/source-build-tool"
hint = "Install the source build tool."
[runtime]
argv = ["/bin/true"]
"#
        ),
    )
    .unwrap();
    let declaration = PluginDeclaration {
        remote: "https://example.com/source-dependency-test.git".to_owned(),
        mode: DeclarationMode::Source,
        selection: GitSelection::Revision("1".repeat(40)),
        release: None,
    };

    let prepared = manager
        .prepare_candidate(
            plugin_id,
            declaration,
            false,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout_root,
                    commit: "1".repeat(40),
                },
            )),
            "source-dependency-check",
        )
        .await;
    let result = manager.finish_single(prepared).await;

    assert_eq!(result.outcome, PackageOperationOutcome::Failed);
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "dependency_missing" && diagnostic.phase == "dependency"
    }));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn daemon_shutdown_cancels_an_active_release_download_without_publication() {
    let directory = tempfile::TempDir::new().unwrap();
    let config_root = directory.path().join("config");
    let checkout_root = directory.path().join("release-checkout");
    fs::create_dir(&config_root).unwrap();
    fs::create_dir(&checkout_root).unwrap();
    fs::write(config_root.join("plugins.lua"), b"return {}\n").unwrap();
    let (store, layout, manager, shutdown) = test_manager(directory.path(), &config_root).await;
    let plugin_id: PluginId = "oll.release-cancel-test".parse().unwrap();
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    fs::write(
        checkout_root.join("oll.toml"),
        format!(
            r#"format_version = 1
[plugin]
id = "oll.release-cancel-test"
name = "release-cancel-test"
protocol_fingerprint = "{fingerprint}"
[runtime]
argv = ["{{install}}/plugin"]
"#
        ),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    fs::write(
        checkout_root.join("oll-release.json"),
        serde_json::to_vec(&serde_json::json!({
            "format_version": 1,
            "plugin_id": plugin_id.as_str(),
            "protocol_fingerprint": fingerprint,
            "releases": {
                "cancel": {
                    "artifacts": [{
                        "target": crate::plugin::package::local_target().unwrap(),
                        "url": format!("http://{address}/release.tar.gz"),
                        "archive": "tar.gz",
                        "size_bytes": 1,
                        "sha256": "00".repeat(32),
                    }]
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let declaration = PluginDeclaration {
        remote: "https://example.com/oll-release-cancel-test.git".to_owned(),
        mode: DeclarationMode::Release,
        selection: GitSelection::Revision("1".repeat(40)),
        release: Some("cancel".to_owned()),
    };
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let manager_task = tokio::spawn({
        let manager = manager.clone();
        let plugin_id = plugin_id.clone();
        async move {
            let prepared = manager
                .prepare_candidate(
                    plugin_id,
                    declaration,
                    false,
                    Some((
                        Uuid::new_v4().to_string(),
                        GitCheckout {
                            source_root: checkout_root,
                            commit: "1".repeat(40),
                        },
                    )),
                    "release-cancel-e2e",
                )
                .await;
            manager.finish_single(prepared).await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), accepted_rx)
        .await
        .expect("release downloader did not connect to the test server")
        .unwrap();
    shutdown.send_replace(Some(
        tokio::time::Instant::now() + std::time::Duration::from_secs(2),
    ));
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), manager_task)
        .await
        .expect("release download ignored daemon shutdown")
        .unwrap();
    server.abort();
    assert_eq!(result.outcome, PackageOperationOutcome::Failed);
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "artifact_download_failed" && diagnostic.phase == "download"
    }));
    assert!(matches!(
        store
            .get_plugin(&PluginSelector::Id(plugin_id.clone()))
            .await,
        Err(PluginError::NotFound(_))
    ));
    assert_eq!(layout.current_generation(&plugin_id).unwrap(), None);
    let plugin_root = layout.plugin_root(&plugin_id);
    assert!(fs::read_dir(&plugin_root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".operation-")
    }));
    assert_eq!(
        fs::read_dir(plugin_root.join("candidates"))
            .unwrap()
            .count(),
        0
    );
}
