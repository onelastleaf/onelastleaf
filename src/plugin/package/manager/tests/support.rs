use super::*;
use sqlx::any::AnyPoolOptions;
use url::Url;

pub(super) async fn test_manager(
    root: &Path,
    config_root: &Path,
) -> (
    PluginStore,
    PackageLayout,
    PackageManager,
    watch::Sender<Option<tokio::time::Instant>>,
) {
    sqlx::any::install_default_drivers();
    let database = root.join(format!("{}.sqlite3", Uuid::new_v4()));
    fs::File::create(&database).unwrap();
    let url = Url::from_file_path(database)
        .unwrap()
        .as_str()
        .replacen("file:", "sqlite:", 1);
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let store = PluginStore::initialize(pool).await.unwrap();
    let layout = PackageLayout::initialize(root.join("packages")).unwrap();
    let (shutdown, receiver) = watch::channel(None);
    let logger = crate::node::logging::NodeLogger::open(
        &root.join("logs"),
        crate::node::identity::NodeIdentity::generate("package-tests".parse().unwrap()),
        None,
    )
    .unwrap();
    let manager = PackageManager::new(
        config_root.to_owned(),
        layout.clone(),
        store.clone(),
        receiver,
        logger,
    );
    (store, layout, manager, shutdown)
}

pub(super) async fn seed_installed_plugin(
    store: &PluginStore,
    layout: &PackageLayout,
    plugin_id: PluginId,
    plugin_name: PluginName,
    declaration: &PluginDeclaration,
) -> InstalledPlugin {
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
    layout
        .publish_candidate(&plugin_id, generation, None)
        .unwrap();
    store
        .finalize_package_publish(&plugin_id, generation)
        .await
        .unwrap()
}

pub(super) fn result_for<'a>(
    results: &'a [PackageOperationResult],
    plugin_id: &PluginId,
) -> &'a PackageOperationResult {
    results
        .iter()
        .find(|result| result.plugin_id.as_ref() == Some(plugin_id))
        .unwrap_or_else(|| panic!("missing package result for {plugin_id}"))
}

pub(super) fn package_intent(
    plugin_id: PluginId,
    plugin_name: PluginName,
    generation: Uuid,
    expected_current_generation: Option<Uuid>,
    declaration: &PluginDeclaration,
    publisher: &str,
) -> PackagePublishIntent {
    let publisher = PublisherManifest::parse(publisher).unwrap();
    let effective = EffectiveManifest::merge(publisher, None).unwrap();
    PackagePublishIntent {
        plugin_id,
        plugin_name,
        operation_id: Uuid::new_v4().to_string(),
        expected_current_generation,
        candidate_generation: generation,
        normalized_declaration: serde_json::to_vec(declaration).unwrap(),
        declaration_sha256: declaration.normalized_sha256(),
        effective_manifest: serde_json::to_vec(&effective).unwrap(),
        selected_commit: Some("0123456789abcdef".to_owned()),
        install_mode: crate::plugin::InstallMode::Source,
        release_id: None,
        correlation_id: "test-correlation".to_owned(),
    }
}

pub(super) fn test_declaration(remote_name: &str) -> PluginDeclaration {
    PluginDeclaration {
        remote: format!("https://example.com/{remote_name}.git"),
        mode: DeclarationMode::Source,
        selection: GitSelection::Default,
        release: None,
    }
}

pub(super) fn test_publisher(plugin_id: &PluginId, plugin_name: &PluginName) -> String {
    test_publisher_with_step(plugin_id, plugin_name, None)
}

pub(super) fn test_publisher_with_step(
    plugin_id: &PluginId,
    plugin_name: &PluginName,
    step: Option<&[&str]>,
) -> String {
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    let source_step = step.map_or_else(String::new, |argv| {
        format!("steps = [{}]\n", serde_json::to_string(argv).unwrap())
    });
    format!(
        r#"format_version = 1
[plugin]
id = "{plugin_id}"
name = "{plugin_name}"
protocol_fingerprint = "{fingerprint}"
[source]
checkout = "source"
{source_step}
[runtime]
argv = ["/bin/true"]
"#
    )
}

pub(super) async fn resolve_local_candidate(
    root: &Path,
    manager: &PackageManager,
    plugin_id: PluginId,
    plugin_name: PluginName,
    declaration: PluginDeclaration,
    step: Option<&[&str]>,
) -> PreparedResolution {
    let checkout = root.join(format!("checkout-{}", plugin_id.as_str()));
    fs::create_dir(&checkout).unwrap();
    fs::write(
        checkout.join("oll.toml"),
        test_publisher_with_step(&plugin_id, &plugin_name, step),
    )
    .unwrap();
    match manager
        .resolve_candidate(
            plugin_id,
            declaration,
            false,
            Some((
                Uuid::new_v4().to_string(),
                GitCheckout {
                    source_root: checkout,
                    commit: "1".repeat(40),
                },
            )),
            InstalledResolution::Lookup,
            "test-candidate-resolution",
        )
        .await
    {
        Resolved::Candidate(candidate) => *candidate,
        Resolved::Result(result) => panic!("candidate resolution failed: {result:#?}"),
    }
}

pub(super) fn create_source_repository(root: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};

    let source = root.join("publisher");
    let repository = root.join("remote.git");
    fs::create_dir(&source).unwrap();
    let fingerprint = crate::replica::lower_hex(&crate::protocol::PROTOCOL_SCHEMA_SHA256);
    fs::write(
        source.join("oll.toml"),
        format!(
            r#"format_version = 1
[plugin]
id = "oll.remote-install-test"
name = "remote-install-test"
protocol_fingerprint = "{fingerprint}"
[source]
checkout = "source"
steps = [
  ["/bin/echo", "recipe-log-marker"],
  ["/bin/cp", "{{source}}/entry", "{{install}}/plugin"],
]
[runtime]
argv = ["{{install}}/plugin"]
"#
        ),
    )
    .unwrap();
    fs::write(source.join("entry"), b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(source.join("entry"), fs::Permissions::from_mode(0o755)).unwrap();
    for arguments in [
        vec!["init", "--quiet", "--initial-branch=main"],
        vec!["add", "oll.toml", "entry"],
        vec![
            "-c",
            "user.name=oll test",
            "-c",
            "user.email=oll@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
    ] {
        let status = Command::new("git")
            .args(arguments)
            .current_dir(&source)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "failed to prepare system Git fixture");
    }
    let status = Command::new("git")
        .args(["clone", "--quiet", "--bare"])
        .arg(&source)
        .arg(&repository)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "failed to create bare system Git fixture");
    repository
}

#[cfg(target_os = "linux")]
pub(super) fn create_release_archive(path: &Path, publisher: &str) {
    use std::io::Write as _;

    let file = fs::File::create(path).unwrap();
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (name, contents, mode) in [
        ("oll.toml", publisher.as_bytes(), 0o600),
        ("plugin", b"#!/bin/sh\nexit 0\n".as_slice(), 0o700),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(mode);
        header.set_cksum();
        archive.append_data(&mut header, name, contents).unwrap();
    }
    let encoder = archive.into_inner().unwrap();
    let mut file = encoder.finish().unwrap();
    file.flush().unwrap();
    file.sync_all().unwrap();
}

pub(super) fn start_git_daemon(root: &Path, repository: &Path) -> (GitDaemon, String) {
    use std::{
        net::{TcpListener, TcpStream},
        process::{Command, Stdio},
        thread,
        time::Duration,
    };

    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut child = Command::new("git")
        .arg("daemon")
        .arg("--reuseaddr")
        .arg("--export-all")
        .arg(format!("--base-path={}", root.display()))
        .arg("--listen=127.0.0.1")
        .arg(format!("--port={port}"))
        .arg(root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let address = (std::net::Ipv4Addr::LOCALHOST, port).into();
    for _ in 0..200 {
        assert!(
            child.try_wait().unwrap().is_none(),
            "system Git daemon exited before accepting connections"
        );
        if TcpStream::connect_timeout(&address, Duration::from_millis(20)).is_ok() {
            let name = repository.file_name().unwrap().to_str().unwrap();
            return (GitDaemon(child), format!("git://127.0.0.1:{port}/{name}"));
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("system Git daemon did not accept connections");
}

pub(super) struct GitDaemon(std::process::Child);

impl Drop for GitDaemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
