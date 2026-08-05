use prost::Message as _;
use time::{Duration, OffsetDateTime};
use tonic::Code;
use uuid::Uuid;

use crate::{
    plugin::{
        DesiredPluginState, InstalledPlugin, JobState, NormalizedJobPayload, ObservedPluginState,
        PluginArtifact, PluginArtifactId, PluginError, PluginInstanceId, PluginJob,
        PluginJobInspection, PluginOperationId,
        package::{PackageDiagnostic, PackageOperationOutcome, PackageOperationResult},
        runtime::PluginSessionSnapshot,
    },
    protocol::oll::{self, reconcile_plugin_installations_request},
};

use super::*;

#[test]
fn request_decoding_rejects_invalid_selectors_modes_digests_and_timestamps() {
    let missing = oll::PluginSelector { selector: None };
    assert_eq!(
        decode_selector(Some(&missing)).unwrap_err().code(),
        Code::InvalidArgument
    );

    let malformed = oll::PluginSelector {
        selector: Some(oll::plugin_selector::Selector::PluginId(oll::PluginId {
            value: "not-an-id".to_owned(),
        })),
    };
    assert_eq!(
        decode_selector(Some(&malformed)).unwrap_err().code(),
        Code::InvalidArgument
    );

    let unknown_mode =
        reconcile_plugin_installations_request::Operation::InstallRemote(install_remote(17, None));
    assert_eq!(
        decode_reconcile_operation(Some(unknown_mode))
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    let mut bad_digest = install_remote(oll::PluginPackageMode::Source as i32, None);
    bad_digest.overwrite = Some(oll::PluginOverwriteAuthorization {
        plugin_id: Some(oll::PluginId {
            value: "oll.test".to_owned(),
        }),
        expected_declaration_sha256: vec![0; 31],
    });
    assert_eq!(
        decode_reconcile_operation(Some(
            reconcile_plugin_installations_request::Operation::InstallRemote(bad_digest),
        ))
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );

    let request = oll::StartPluginJobRequest {
        context: None,
        operation_id: "operation-1".to_owned(),
        plugin: Some(plugin_selector()),
        action: "run".to_owned(),
        arguments: Vec::new(),
        deadline: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 1_000_000_000,
        }),
    };
    assert_eq!(
        decode_start_plugin_job(request).unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[test]
fn request_decoding_maps_every_desired_state_and_package_mode() {
    assert_eq!(
        decode_desired_state(oll::PluginDesiredState::Running as i32).unwrap(),
        DesiredPluginState::Running
    );
    assert_eq!(
        decode_desired_state(oll::PluginDesiredState::Stopped as i32).unwrap(),
        DesiredPluginState::Stopped
    );
    assert_eq!(
        decode_desired_state(0).unwrap_err().code(),
        Code::InvalidArgument
    );

    let source = decode_reconcile_operation(Some(
        reconcile_plugin_installations_request::Operation::InstallRemote(install_remote(
            oll::PluginPackageMode::Source as i32,
            None,
        )),
    ));
    assert!(matches!(
        source,
        Ok(DecodedReconcileOperation::InstallRemote(_))
    ));

    let release = decode_reconcile_operation(Some(
        reconcile_plugin_installations_request::Operation::InstallRemote(install_remote(
            oll::PluginPackageMode::Release as i32,
            Some("release-opaque"),
        )),
    ));
    assert!(matches!(
        release,
        Ok(DecodedReconcileOperation::InstallRemote(_))
    ));
}

#[test]
fn installation_results_map_every_outcome_and_redact_remote_credentials() {
    let outcomes = [
        (
            PackageOperationOutcome::Installed,
            oll::PluginInstallationOutcome::Installed,
        ),
        (
            PackageOperationOutcome::Updated,
            oll::PluginInstallationOutcome::Updated,
        ),
        (
            PackageOperationOutcome::Removed,
            oll::PluginInstallationOutcome::Removed,
        ),
        (
            PackageOperationOutcome::AlreadySatisfied,
            oll::PluginInstallationOutcome::AlreadySatisfied,
        ),
        (
            PackageOperationOutcome::ConfirmationRequired,
            oll::PluginInstallationOutcome::ConfirmationRequired,
        ),
        (
            PackageOperationOutcome::Failed,
            oll::PluginInstallationOutcome::Failed,
        ),
    ];
    for (domain, wire) in outcomes {
        let confirmation = domain == PackageOperationOutcome::ConfirmationRequired;
        let response = encode_installation_results(vec![PackageOperationResult {
            plugin_id: Some("oll.test".parse().unwrap()),
            plugin_name: Some("test".parse().unwrap()),
            outcome: domain,
            diagnostics: Vec::new(),
            confirmation_summary: confirmation.then(|| "remote changed".to_owned()),
            confirmation_digest: confirmation.then_some([3; 32]),
        }])
        .unwrap();
        assert_eq!(response.results[0].outcome, wire as i32);
    }

    let response = encode_installation_results(vec![PackageOperationResult {
        plugin_id: None,
        plugin_name: None,
        outcome: PackageOperationOutcome::Failed,
        diagnostics: vec![PackageDiagnostic {
            code: "git_fetch_failed".to_owned(),
            phase: "git".to_owned(),
            message: "fetch failed".to_owned(),
            hint: None,
            build_log_path: None,
            sanitized_remote: Some("https://user:secret@example.com/plugin.git".to_owned()),
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }],
        confirmation_summary: None,
        confirmation_digest: None,
    }])
    .unwrap();
    let remote = response.results[0].diagnostics[0]
        .sanitized_remote
        .as_deref()
        .unwrap();
    assert!(!remote.contains("secret"));

    let response = encode_installation_results(vec![PackageOperationResult {
        plugin_id: Some("oll.test".parse().unwrap()),
        plugin_name: Some("test".parse().unwrap()),
        outcome: PackageOperationOutcome::Failed,
        diagnostics: vec![PackageDiagnostic {
            code: "install_publish_failed".to_owned(),
            phase: "store".to_owned(),
            message: "postgresql://user:password@example.invalid/database".to_owned(),
            hint: None,
            build_log_path: None,
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
        }],
        confirmation_summary: None,
        confirmation_digest: None,
    }])
    .unwrap();
    assert!(
        !response.results[0].diagnostics[0]
            .message
            .contains("password")
    );
}

#[test]
fn process_and_job_state_encoders_map_every_domain_variant() {
    let process_states = [
        (
            ObservedPluginState::Starting,
            oll::PluginProcessState::Starting,
        ),
        (ObservedPluginState::Ready, oll::PluginProcessState::Ready),
        (
            ObservedPluginState::Stopping,
            oll::PluginProcessState::Stopping,
        ),
        (ObservedPluginState::Exited, oll::PluginProcessState::Exited),
        (ObservedPluginState::Failed, oll::PluginProcessState::Failed),
    ];
    for (domain, wire) in process_states {
        let mut installed = installed_plugin();
        let instance_id = PluginInstanceId::new();
        installed.running_generation = Some(installed.current_generation);
        installed.running_instance_id = Some(instance_id);
        let response = encode_plugin_list(vec![crate::plugin::PluginListEntry {
            installed: installed.clone(),
            process: Some(PluginSessionSnapshot {
                state: domain,
                instance_id,
                install_generation: installed.current_generation,
                process_id: Some(42),
                started_at: Some(timestamp()),
                ready_at: None,
                actions: Vec::new(),
            }),
        }])
        .unwrap();
        assert_eq!(response.plugins[0].process_state, wire as i32);
    }

    let job_states = [
        (JobState::Dispatching, oll::PluginAdminJobState::Dispatching),
        (JobState::Running, oll::PluginAdminJobState::Running),
        (JobState::Cancelling, oll::PluginAdminJobState::Cancelling),
        (JobState::Succeeded, oll::PluginAdminJobState::Succeeded),
        (JobState::Failed, oll::PluginAdminJobState::Failed),
        (JobState::Cancelled, oll::PluginAdminJobState::Cancelled),
        (JobState::TimedOut, oll::PluginAdminJobState::TimedOut),
    ];
    for (domain, wire) in job_states {
        let mut job = plugin_job(domain);
        job.state = domain;
        assert_eq!(encode_job_state_response(&job).state, wire as i32);
    }

    let completed_after_acceptance = plugin_job(JobState::Succeeded);
    assert_eq!(
        encode_start_job_response(&completed_after_acceptance).state,
        oll::PluginAdminJobState::Running as i32,
    );
    let mut admission_failure = plugin_job(JobState::Failed);
    admission_failure.accepted_at = None;
    assert_eq!(
        encode_start_job_response(&admission_failure).state,
        oll::PluginAdminJobState::Failed as i32,
    );
}

#[test]
fn stored_config_value_rejects_function_handles() {
    let value = oll::ConfigValue {
        kind: Some(oll::config_value::Kind::FunctionValue(
            oll::ConfigFunctionRef {
                session_id: "session".to_owned(),
                function_id: "function".to_owned(),
            },
        )),
    };
    let mut job = plugin_job(JobState::Succeeded);
    job.result = Some(value.encode_to_vec());
    let error = encode_job_details(PluginJobInspection {
        job,
        plugin_name: "test".parse().unwrap(),
        artifacts: Vec::new(),
    })
    .unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert!(!error.message().contains("session"));
}

#[test]
fn failed_job_details_retain_already_published_artifacts() {
    let job = plugin_job(JobState::Failed);
    let artifact = PluginArtifact {
        artifact_id: PluginArtifactId::new(),
        job_id: job.job_id,
        plugin_id: job.payload.plugin_id.clone(),
        file_name: "partial-report.pdf".to_owned(),
        media_type: "application/pdf".to_owned(),
        size_bytes: 4,
        sha256: [7; 32],
        destination: "/tmp/partial-report.pdf".into(),
        stored_at: timestamp(),
    };

    let response = encode_job_details(PluginJobInspection {
        job,
        plugin_name: "test".parse().unwrap(),
        artifacts: vec![artifact],
    })
    .unwrap();
    assert_eq!(response.job.unwrap().artifacts.len(), 1);
}

#[test]
fn corrupt_store_status_never_exposes_internal_details() {
    let secret = "postgresql://user:password@example.invalid/database";
    let status = plugin_status(PluginError::Store(secret.to_owned()));
    assert_eq!(status.code(), Code::Internal);
    assert!(!status.message().contains("password"));

    let status = plugin_status(PluginError::Aborted("cancelled".to_owned()));
    assert_eq!(status.code(), Code::Aborted);
}

fn install_remote(mode: i32, release_id: Option<&str>) -> oll::InstallRemotePlugin {
    oll::InstallRemotePlugin {
        remote: "https://example.com/plugin.git".to_owned(),
        mode,
        selection: None,
        release_id: release_id.map(str::to_owned),
        overwrite: None,
    }
}

fn plugin_selector() -> oll::PluginSelector {
    oll::PluginSelector {
        selector: Some(oll::plugin_selector::Selector::PluginId(oll::PluginId {
            value: "oll.test".to_owned(),
        })),
    }
}

fn installed_plugin() -> InstalledPlugin {
    InstalledPlugin {
        plugin_id: "oll.test".parse().unwrap(),
        plugin_name: "test".parse().unwrap(),
        normalized_declaration: Vec::new(),
        declaration_sha256: [0; 32],
        effective_manifest: Vec::new(),
        selected_commit: None,
        install_mode: crate::plugin::InstallMode::Source,
        release_id: None,
        current_generation: Uuid::new_v4(),
        running_generation: None,
        running_instance_id: None,
        desired_state: DesiredPluginState::Running,
        restart_sequence: 0,
        consumed_restart_sequence: 0,
        restart_attempt: 0,
        restart_not_before: None,
        last_lifecycle_failure: None,
    }
}

fn plugin_job(state: JobState) -> PluginJob {
    let admitted_at = timestamp();
    PluginJob {
        job_id: crate::plugin::PluginJobId::new(),
        operation_id: "operation-1".parse::<PluginOperationId>().unwrap(),
        payload: NormalizedJobPayload::new(
            "oll.test".parse().unwrap(),
            "run".to_owned(),
            vec!["argument".to_owned()],
            None,
        )
        .unwrap(),
        absolute_deadline: admitted_at + Duration::hours(24),
        state,
        cancellation_reason: None,
        plugin_instance_id: PluginInstanceId::new(),
        admitted_at,
        accepted_at: Some(admitted_at),
        terminal_at: state.is_terminal().then_some(admitted_at),
        updated_at: admitted_at,
        correlation_id: "correlation".to_owned(),
        result: None,
        error_code: (state == JobState::Failed).then(|| "daemon_restarted".to_owned()),
        error_message: None,
    }
}

fn timestamp() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}
