use std::path::PathBuf;

use prost_types::Timestamp;
use time::OffsetDateTime;

use crate::protocol::oll;

use super::{
    DesiredPluginState, InstallMode, JobCancellationReason, JobState, PluginArtifactId,
    PluginError, PluginId, PluginInstanceId, PluginJobId, PluginName, PluginOperationId,
    PluginSelector,
    package::PackageError,
    protocol::{
        PackageDiagnosticContext, decode_admin_job_state, decode_desired_state,
        decode_install_mode, decode_job_cancellation_reason, decode_optional_timestamp,
        decode_plugin_artifact_id, decode_plugin_id, decode_plugin_instance_id,
        decode_plugin_job_id, decode_plugin_name, decode_plugin_operation_id,
        decode_plugin_selector, decode_required_timestamp, decode_runtime_job_state,
        encode_admin_job_state, encode_desired_state, encode_install_mode,
        encode_job_cancellation_reason, encode_package_diagnostic, encode_plugin_artifact_id,
        encode_plugin_id, encode_plugin_instance_id, encode_plugin_job_id, encode_plugin_name,
        encode_plugin_operation_id, encode_plugin_selector, encode_timestamp,
    },
};

#[test]
fn identity_wrappers_validate_domain_grammars_and_round_trip() {
    let plugin_id: PluginId = "oll.anki".parse().unwrap();
    let plugin_name: PluginName = "oll-anki".parse().unwrap();
    let job_id = PluginJobId::new();
    let artifact_id = PluginArtifactId::new();
    let instance_id = PluginInstanceId::new();
    let operation_id: PluginOperationId = "client-operation-1".parse().unwrap();

    assert_eq!(
        decode_plugin_id(Some(&encode_plugin_id(&plugin_id)), "plugin_id").unwrap(),
        plugin_id
    );
    assert_eq!(
        decode_plugin_name(Some(&encode_plugin_name(&plugin_name)), "plugin_name").unwrap(),
        plugin_name
    );
    assert_eq!(
        decode_plugin_job_id(Some(&encode_plugin_job_id(job_id)), "job_id").unwrap(),
        job_id
    );
    assert_eq!(
        decode_plugin_artifact_id(Some(&encode_plugin_artifact_id(artifact_id)), "artifact_id")
            .unwrap(),
        artifact_id
    );
    assert_eq!(
        decode_plugin_instance_id(&encode_plugin_instance_id(instance_id), "instance_id").unwrap(),
        instance_id
    );
    assert_eq!(
        decode_plugin_operation_id(&encode_plugin_operation_id(&operation_id), "operation_id")
            .unwrap(),
        operation_id
    );

    assert_invalid(decode_plugin_id(None, "plugin_id"));
    assert_invalid(decode_plugin_id(
        Some(&oll::PluginId {
            value: "OLL.Anki".to_owned(),
        }),
        "plugin_id",
    ));
    assert_invalid(decode_plugin_job_id(
        Some(&oll::PluginJobId {
            value: "123E4567-E89B-42D3-A456-426614174000".to_owned(),
        }),
        "job_id",
    ));
    assert_invalid(decode_plugin_operation_id("", "operation_id"));
}

#[test]
fn selectors_preserve_the_explicit_wire_variant() {
    let by_id = PluginSelector::Id("oll.anki".parse().unwrap());
    let by_name = PluginSelector::Name("oll-anki".parse().unwrap());

    assert_eq!(
        decode_plugin_selector(Some(&encode_plugin_selector(&by_id)), "plugin").unwrap(),
        by_id
    );
    assert_eq!(
        decode_plugin_selector(Some(&encode_plugin_selector(&by_name)), "plugin").unwrap(),
        by_name
    );
    assert_invalid(decode_plugin_selector(None, "plugin"));
    assert_invalid(decode_plugin_selector(
        Some(&oll::PluginSelector { selector: None }),
        "plugin",
    ));
}

#[test]
fn protocol_enums_reject_unspecified_and_unknown_values() {
    for state in [DesiredPluginState::Running, DesiredPluginState::Stopped] {
        assert_eq!(
            decode_desired_state(encode_desired_state(state), "desired_state").unwrap(),
            state
        );
    }
    for mode in [InstallMode::Source, InstallMode::Release] {
        assert_eq!(
            decode_install_mode(encode_install_mode(mode), "mode").unwrap(),
            mode
        );
    }
    for state in [
        JobState::Dispatching,
        JobState::Running,
        JobState::Cancelling,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
        JobState::TimedOut,
    ] {
        assert_eq!(
            decode_admin_job_state(encode_admin_job_state(state), "state").unwrap(),
            state
        );
    }
    for reason in [
        JobCancellationReason::UserRequest,
        JobCancellationReason::Deadline,
    ] {
        assert_eq!(
            decode_job_cancellation_reason(
                encode_job_cancellation_reason(reason),
                "cancellation_reason"
            )
            .unwrap(),
            reason
        );
    }
    assert_eq!(
        decode_runtime_job_state(oll::JobState::Succeeded as i32, "state").unwrap(),
        JobState::Succeeded
    );

    for invalid in [0, i32::MAX] {
        assert_invalid(decode_desired_state(invalid, "desired_state"));
        assert_invalid(decode_install_mode(invalid, "mode"));
        assert_invalid(decode_admin_job_state(invalid, "state"));
        assert_invalid(decode_runtime_job_state(invalid, "state"));
        assert_invalid(decode_job_cancellation_reason(
            invalid,
            "cancellation_reason",
        ));
    }
}

#[test]
fn timestamps_enforce_the_well_known_type_range_and_nanoseconds() {
    let pre_epoch = Timestamp {
        seconds: -1,
        nanos: 500_000_000,
    };
    let decoded = decode_required_timestamp(Some(&pre_epoch), "created_at").unwrap();
    assert_eq!(decoded.unix_timestamp(), -1);
    assert_eq!(decoded.nanosecond(), 500_000_000);
    assert_eq!(encode_timestamp(decoded, "created_at").unwrap(), pre_epoch);
    assert_eq!(decode_optional_timestamp(None, "deadline").unwrap(), None);

    assert_invalid(decode_required_timestamp(None, "created_at"));
    for timestamp in [
        Timestamp {
            seconds: -62_135_596_801,
            nanos: 0,
        },
        Timestamp {
            seconds: 253_402_300_800,
            nanos: 0,
        },
        Timestamp {
            seconds: 0,
            nanos: -1,
        },
        Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        },
    ] {
        assert_invalid(decode_required_timestamp(Some(&timestamp), "created_at"));
    }

    let before_year_one = OffsetDateTime::from_unix_timestamp(-62_135_596_801).unwrap();
    assert!(matches!(
        encode_timestamp(before_year_one, "stored_at"),
        Err(PluginError::CorruptStore(_))
    ));
}

#[test]
fn package_diagnostics_include_only_explicit_sanitized_context() {
    let plugin_id: PluginId = "oll.anki".parse().unwrap();
    let plugin_name: PluginName = "oll-anki".parse().unwrap();
    let error = PackageError::new("recipe_step_failed", "build", "recipe exited with status 2")
        .with_hint("inspect the retained build log")
        .with_build_log(PathBuf::from("/var/log/oll/build-1.log"));

    let diagnostic = encode_package_diagnostic(
        &error,
        PackageDiagnosticContext {
            plugin_id: Some(&plugin_id),
            plugin_name: Some(&plugin_name),
            sanitized_remote: Some("https://example.invalid/plugins/anki.git"),
            branch: Some("main"),
            revision: None,
            release_id: None,
            target: Some("x86_64-unknown-linux-gnu"),
        },
    );

    assert_eq!(diagnostic.code, "recipe_step_failed");
    assert_eq!(diagnostic.phase, "build");
    assert_eq!(diagnostic.plugin_id.unwrap().value, "oll.anki");
    assert_eq!(diagnostic.plugin_name.unwrap().value, "oll-anki");
    assert_eq!(diagnostic.branch.as_deref(), Some("main"));
    assert_eq!(diagnostic.revision, None);
    assert_eq!(
        diagnostic.hint.as_deref(),
        Some("inspect the retained build log")
    );
    assert_eq!(
        diagnostic.build_log_path.as_deref(),
        Some("/var/log/oll/build-1.log")
    );
}

fn assert_invalid<T>(result: Result<T, PluginError>) {
    assert!(matches!(result, Err(PluginError::InvalidArgument(_))));
}
