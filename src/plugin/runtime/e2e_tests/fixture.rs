use std::{fs, path::Path, sync::Arc, time::Duration};

use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    configuration::{ConfigRuntime, ReplicaStoreConfig},
    node::{
        ParentLivenessPipe,
        identity::{IdentityCoordinator, NodeIdentity},
        logging::NodeLogger,
    },
    plugin::{
        InstallMode, JobState, PackagePublishIntent, PluginId, PluginJobId, PluginRuntime,
        PluginSelector, PluginStore,
        package::{DeclarationMode, GitSelection, PackageLayout, PluginDeclaration},
        runtime::PluginSessionSnapshot,
    },
    protocol::{PROTOCOL_SCHEMA_SHA256, oll},
    replica::ReplicaRuntime,
};

const DOCUMENT_PATH: &str = "/runtime-e2e.md";
const LARGE_DOCUMENT_PATH: &str = "runtime-e2e-large.md";
const LARGE_DOCUMENT_BYTES: usize = 5_000_000;

pub(super) struct RuntimeFixture {
    _directory: TempDir,
    pub(super) plugins: Arc<PluginRuntime>,
    pub(super) selector: PluginSelector,
    pub(super) store: PluginStore,
    replica: Arc<ReplicaRuntime>,
    logger: Arc<NodeLogger>,
    log_dir: std::path::PathBuf,
    plugin_id: PluginId,
    expect_restart: bool,
    _parent_liveness: Arc<ParentLivenessPipe>,
}

impl RuntimeFixture {
    pub(super) async fn start(fake_test_name: &str) -> Self {
        Self::start_with_modes(fake_test_name, false, false).await
    }

    pub(super) async fn start_with_exit_once(fake_test_name: &str, exit_once: bool) -> Self {
        Self::start_with_modes(fake_test_name, exit_once, false).await
    }

    pub(super) async fn start_with_no_read_flood(fake_test_name: &str) -> Self {
        Self::start_with_modes(fake_test_name, false, true).await
    }

    async fn start_with_modes(fake_test_name: &str, exit_once: bool, no_read_flood: bool) -> Self {
        let directory = TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let replica_root = directory.path().join("working-tree");
        let store_path = directory.path().join("replica.sqlite3");
        let log_dir = directory.path().join("logs");
        let artifact_dir = directory.path().join("downloads");
        let plugin_data = directory.path().join("plugin-data");
        fs::create_dir_all(&config_root).unwrap();
        fs::create_dir_all(&replica_root).unwrap();
        fs::write(
            replica_root.join(LARGE_DOCUMENT_PATH),
            "x".repeat(LARGE_DOCUMENT_BYTES),
        )
        .unwrap();
        fs::write(replica_root.join("runtime-e2e.md"), "initial content").unwrap();
        fs::write(
            config_root.join("config.lua"),
            format!(
                "return {{\n  format_version = 1,\n  node = {{\n    replica_root = {:?},\n    replica_store = {{ driver = \"sqlite\", path = {:?} }},\n    log_dir = {:?},\n    artifact_download_dir = {:?},\n    listen = nil,\n    connect = {{}},\n  }},\n}}\n",
                replica_root.to_str().unwrap(),
                store_path.to_str().unwrap(),
                log_dir.to_str().unwrap(),
                artifact_dir.to_str().unwrap(),
            ),
        )
        .unwrap();
        fs::write(config_root.join("plugins.lua"), "return {}\n").unwrap();
        let (config, _) = ConfigRuntime::load(&config_root).unwrap();

        let identity = NodeIdentity::generate("runtime-e2e".parse().unwrap());
        let identities = IdentityCoordinator::new(identity.clone());
        let logger = NodeLogger::open(&log_dir, identity).unwrap();
        let replica = ReplicaRuntime::start(
            config_root.clone(),
            replica_root,
            &ReplicaStoreConfig::Sqlite { path: store_path },
            Arc::clone(&identities),
            Arc::clone(&logger),
        )
        .await
        .unwrap();

        let plugin_id: PluginId = "oll.runtime-e2e".parse().unwrap();
        publish_fake_plugin(
            replica.database_pool(),
            &plugin_data,
            &plugin_id,
            fake_test_name,
            exit_once,
            no_read_flood,
        )
        .await;
        let parent_liveness = Arc::new(ParentLivenessPipe::create().unwrap());
        let plugins = PluginRuntime::start(
            config_root,
            plugin_data,
            artifact_dir,
            config,
            Arc::clone(&replica),
            identities,
            Arc::clone(&logger),
            Arc::clone(&parent_liveness),
            "runtime-e2e-startup",
        )
        .await
        .unwrap();
        let store = PluginStore::initialize(replica.database_pool())
            .await
            .unwrap();

        Self {
            _directory: directory,
            selector: PluginSelector::Id(plugin_id.clone()),
            plugins,
            store,
            replica,
            logger,
            log_dir,
            plugin_id,
            expect_restart: exit_once,
            _parent_liveness: parent_liveness,
        }
    }

    pub(super) async fn assert_desired_stopped_does_not_spawn(&self, deadline: Instant) {
        loop {
            let inspection = self.plugins.inspect_plugin(&self.selector).await.unwrap();
            assert_eq!(
                inspection.installed.desired_state,
                crate::plugin::DesiredPluginState::Stopped
            );
            assert!(
                inspection.process.is_none(),
                "desired-stopped plugin spawned"
            );
            if Instant::now() >= deadline {
                return;
            }
            sleep_briefly(deadline).await;
        }
    }

    pub(super) fn diagnostic_logs(&self) -> String {
        format!(
            "lifecycle log: {}; plugin log: {}",
            fs::read_to_string(self.log_dir.join("oll.log")).unwrap_or_default(),
            fs::read_to_string(self.log_dir.join("plugin.log")).unwrap_or_default(),
        )
    }

    pub(super) async fn wait_for_ready(&self, deadline: Instant) -> PluginSessionSnapshot {
        let startup_deadline = deadline.min(Instant::now() + Duration::from_secs(15));
        loop {
            let inspection = tokio::time::timeout_at(
                startup_deadline,
                self.plugins.inspect_plugin(&self.selector),
            )
            .await
            .expect("plugin inspection blocked during startup")
            .unwrap();
            if inspection.installed.last_lifecycle_failure.is_some() {
                panic!(
                    "plugin startup failed; lifecycle log: {}; plugin log: {}",
                    fs::read_to_string(self.log_dir.join("oll.log")).unwrap_or_default(),
                    fs::read_to_string(self.log_dir.join("plugin.log")).unwrap_or_default(),
                );
            }
            if let Some(process) = inspection.process
                && process.state == crate::plugin::ObservedPluginState::Ready
            {
                return process;
            }
            if Instant::now() >= startup_deadline {
                panic!(
                    "plugin did not become ready; lifecycle log: {}",
                    fs::read_to_string(self.log_dir.join("oll.log")).unwrap_or_default()
                );
            }
            sleep_briefly(startup_deadline).await;
        }
    }

    pub(super) async fn wait_for_document_content(&self, expected: &str, deadline: Instant) {
        loop {
            let response = self
                .replica
                .read_document(oll::ReadDocumentRequest {
                    path: Some(oll::DocumentPath {
                        value: DOCUMENT_PATH.to_owned(),
                    }),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap();
            let content = response
                .document
                .and_then(|document| document.representation)
                .and_then(|representation| match representation {
                    oll::document_snapshot::Representation::Content(content) => Some(content),
                    oll::document_snapshot::Representation::Crdt(_) => None,
                });
            if content.as_deref() == Some(expected) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "fake plugin did not commit the guarded document update; {}",
                self.diagnostic_logs(),
            );
            sleep_briefly(deadline).await;
        }
    }

    pub(super) async fn hold_package_gate(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.plugins.hold_package_gate(&self.plugin_id).await
    }

    pub(super) async fn supervisor_barrier(&self, deadline: Instant) {
        tokio::time::timeout_at(deadline, self.plugins.supervisor_barrier())
            .await
            .expect("plugin supervisor barrier exceeded the E2E deadline")
            .unwrap();
    }

    pub(super) async fn saturate_instance_work_queue(
        &self,
    ) -> crate::plugin::runtime::SaturatedInstanceWorkQueue {
        self.plugins
            .saturate_instance_work_queue(&self.plugin_id)
            .await
            .unwrap()
    }

    pub(super) async fn wait_for_stopped(&self, deadline: Instant) {
        loop {
            let inspection = self.plugins.inspect_plugin(&self.selector).await.unwrap();
            if inspection.process.is_none()
                && inspection.installed.running_instance_id.is_none()
                && inspection.installed.running_generation.is_none()
            {
                return;
            }
            assert_before(deadline, "plugin did not stop before the E2E deadline");
            sleep_briefly(deadline).await;
        }
    }

    pub(super) async fn wait_for_restarted_ready(
        &self,
        prior_instance: crate::plugin::PluginInstanceId,
        deadline: Instant,
    ) -> PluginSessionSnapshot {
        loop {
            if let Some(process) = self
                .plugins
                .inspect_plugin(&self.selector)
                .await
                .unwrap()
                .process
                && process.state == crate::plugin::ObservedPluginState::Ready
                && process.instance_id != prior_instance
            {
                return process;
            }
            assert_before(deadline, "plugin did not restart after its unexpected exit");
            sleep_briefly(deadline).await;
        }
    }

    pub(super) async fn wait_for_job_state(
        &self,
        job_id: PluginJobId,
        expected: JobState,
        deadline: Instant,
    ) {
        loop {
            let job = self.plugins.inspect_job(job_id).await.unwrap().job;
            if job.state == expected {
                return;
            }
            assert_before(deadline, "job did not reach its expected state");
            sleep_briefly(deadline).await;
        }
    }

    pub(super) async fn shutdown_and_verify_logs(self, deadline: Instant) {
        tokio::time::timeout_at(
            deadline,
            self.plugins
                .shutdown(deadline, "runtime-e2e-daemon-shutdown"),
        )
        .await
        .expect("plugin runtime did not stop before the E2E deadline")
        .unwrap();
        tokio::time::timeout_at(deadline, self.replica.shutdown(deadline))
            .await
            .expect("replica runtime did not stop before the E2E deadline")
            .unwrap();
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "no time remained to flush E2E logs");
        self.logger
            .flush_until(std::time::Instant::now() + remaining)
            .unwrap();

        let plugin_records = read_json_lines(&self.log_dir.join("plugin.log"));
        assert!(plugin_records.iter().any(|record| {
            record["event"] == "plugin_log_record"
                && record["target"] == "plugin::runtime_e2e"
                && record["message"] == "host calls completed"
                && record["plugin_id"] == self.plugin_id.as_str()
                && record["parent_call_id"] == 4242
                && record["call_depth"] == 2
                && record["causal_depth"] == 3
                && record["task_id"] == "runtime-e2e-task"
                && record["task_group_id"] == "runtime-e2e-group"
        }));
        assert!(
            plugin_records.iter().any(|record| {
                record["event"] == "plugin_log_record"
                    && record["target"] == "plugin::runtime_e2e"
                    && record["message"] == "shutdown request observed"
                    && record["plugin_id"] == self.plugin_id.as_str()
            }),
            "graceful shutdown log was not observed; {}",
            self.diagnostic_logs(),
        );

        let lifecycle_records = read_json_lines(&self.log_dir.join("oll.log"));
        for event in [
            "plugin_artifact_startup_recovery_started",
            "plugin_artifact_startup_recovery_succeeded",
            "plugin_package_recovery_started",
            "plugin_package_recovery_succeeded",
            "plugin_runtime_started",
            "plugin_system_ready",
        ] {
            assert!(
                lifecycle_records.iter().any(|record| {
                    record["event"] == event && record["correlation_id"] == "runtime-e2e-startup"
                }),
                "plugin startup event {event} lost the node startup correlation"
            );
        }
        assert!(lifecycle_records.iter().any(|record| {
            record["event"] == "plugin_host_call_completed"
                && record["plugin_id"] == self.plugin_id.as_str()
                && record["outcome"] == "success"
                && record["parent_call_id"] == 4242
                && record["call_depth"] == 2
                && record["causal_depth"] == 3
                && record["task_id"] == "runtime-e2e-task"
                && record["task_group_id"] == "runtime-e2e-group"
        }));
        assert!(lifecycle_records.iter().any(|record| {
            record["event"] == "plugin_shutdown_requested"
                && record["plugin_id"] == self.plugin_id.as_str()
        }));
        assert!(!lifecycle_records.iter().any(|record| {
            record["event"] == "plugin_process_signal_sent"
                && record["plugin_id"] == self.plugin_id.as_str()
        }));
        if self.expect_restart {
            assert!(lifecycle_records.iter().any(|record| {
                record["event"] == "plugin_restart_scheduled"
                    && record["plugin_id"] == self.plugin_id.as_str()
                    && record["backoff_ms"] == 1000
            }));
        }
    }

    pub(super) async fn shutdown_without_plugin_process(self, deadline: Instant) {
        tokio::time::timeout_at(
            deadline,
            self.plugins
                .shutdown(deadline, "runtime-e2e-daemon-shutdown"),
        )
        .await
        .expect("plugin runtime did not stop before the E2E deadline")
        .unwrap();
        tokio::time::timeout_at(deadline, self.replica.shutdown(deadline))
            .await
            .expect("replica runtime did not stop before the E2E deadline")
            .unwrap();
    }
}

async fn publish_fake_plugin(
    pool: sqlx::AnyPool,
    plugin_data: &Path,
    plugin_id: &PluginId,
    fake_test_name: &str,
    exit_once: bool,
    no_read_flood: bool,
) {
    let store = PluginStore::initialize(pool).await.unwrap();
    let layout = PackageLayout::initialize(plugin_data.to_owned()).unwrap();
    let generation = Uuid::new_v4();
    let candidate = layout.candidate(plugin_id, generation).unwrap();
    fs::write(candidate.join("runtime-e2e-fixture"), b"test generation").unwrap();
    if exit_once {
        fs::write(candidate.join("exit-once"), b"exit once").unwrap();
    }
    if no_read_flood {
        fs::write(candidate.join("no-read-flood"), b"do not read host output").unwrap();
    }

    let declaration = PluginDeclaration {
        remote: "https://example.invalid/oll-runtime-e2e.git".to_owned(),
        mode: DeclarationMode::Source,
        selection: GitSelection::Default,
        release: None,
    };
    declaration.validate().unwrap();
    let declaration_bytes = serde_json::to_vec(&declaration).unwrap();
    let executable = std::env::current_exe().unwrap();
    let effective_manifest = serde_json::to_vec(&serde_json::json!({
        "plugin_id": plugin_id.as_str(),
        "plugin_name": "runtime-e2e",
        "protocol_fingerprint": crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256),
        "source": { "dependencies": [], "steps": [] },
        "runtime": {
            "argv": [
                executable.to_str().expect("test executable path is UTF-8"),
                "--ignored",
                "--exact",
                fake_test_name,
                "--nocapture",
                "--test-threads=1"
            ]
        }
    }))
    .unwrap();
    let intent = PackagePublishIntent {
        plugin_id: plugin_id.clone(),
        plugin_name: "runtime-e2e".parse().unwrap(),
        operation_id: "install-runtime-e2e-fixture".to_owned(),
        expected_current_generation: None,
        candidate_generation: generation,
        normalized_declaration: declaration_bytes.clone(),
        declaration_sha256: Sha256::digest(&declaration_bytes).into(),
        effective_manifest,
        selected_commit: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
        install_mode: InstallMode::Source,
        release_id: None,
        correlation_id: "install-runtime-e2e-fixture".to_owned(),
    };
    store.prepare_package_publish(&intent).await.unwrap();
    layout
        .publish_candidate(plugin_id, generation, None)
        .unwrap();
    let installed = store
        .finalize_package_publish(plugin_id, generation)
        .await
        .unwrap();
    assert_eq!(
        installed.desired_state,
        crate::plugin::DesiredPluginState::Stopped
    );
}

async fn sleep_briefly(deadline: Instant) {
    tokio::time::sleep_until((Instant::now() + Duration::from_millis(20)).min(deadline)).await;
}

fn assert_before(deadline: Instant, message: &str) {
    assert!(Instant::now() < deadline, "{message}");
}

fn read_json_lines(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
