use std::io::Cursor;

use serde_json::json;

use crate::{
    cli::{GitSelector, PluginInstallMode, PluginLogTarget},
    protocol::oll::{self, plugin_git_selection, plugin_selector},
};

use super::{
    OverwriteAuthorization, authorize_remote, confirm_overwrite_with, local, new_operation_id,
    output, overwrite_authorization, parse_job_id, parse_plugin_selector, remote_install_request,
};

#[test]
fn selectors_and_job_ids_are_strictly_parsed_before_admin_calls() {
    let plugin_id = parse_plugin_selector("oll.example").unwrap();
    assert!(matches!(
        plugin_id.selector,
        Some(plugin_selector::Selector::PluginId(ref value)) if value.value == "oll.example"
    ));

    let plugin_name = parse_plugin_selector("example").unwrap();
    assert!(matches!(
        plugin_name.selector,
        Some(plugin_selector::Selector::PluginName(ref value)) if value.value == "example"
    ));
    assert!(parse_plugin_selector("Example").is_err());

    let canonical = "c588e4a1-9707-4702-bb74-e50ff948b88e";
    assert_eq!(parse_job_id(canonical).unwrap().value, canonical);
    assert!(parse_job_id("C588E4A1-9707-4702-BB74-E50FF948B88E").is_err());
    assert!(parse_job_id("not-a-job-id").is_err());
}

#[test]
fn generated_operation_ids_are_random_uuid_v4_values() {
    let first = new_operation_id();
    let second = new_operation_id();
    assert_ne!(first, second);
    for value in [first, second] {
        let parsed = uuid::Uuid::parse_str(&value).unwrap();
        assert_eq!(parsed.get_version_num(), 4);
        assert_eq!(parsed.to_string(), value);
    }
}

#[test]
fn overwrite_authorization_preserves_the_original_remote_request() {
    let remote = remote_install_request(
        "ssh://git@example.test/publisher/plugin.git",
        GitSelector::Branch("stable".to_owned()),
        PluginInstallMode::Release {
            release_id: "release-7".to_owned(),
        },
    );
    let authorized = authorize_remote(
        remote.clone(),
        OverwriteAuthorization {
            plugin_id: oll::PluginId {
                value: "oll.example".to_owned(),
            },
            digest: vec![7; 32],
            summary: "declaration changed".to_owned(),
        },
    );

    assert_eq!(authorized.remote, remote.remote);
    assert_eq!(authorized.mode, remote.mode);
    assert_eq!(authorized.release_id, remote.release_id);
    assert!(matches!(
        authorized.selection.and_then(|value| value.selection),
        Some(plugin_git_selection::Selection::Branch(value)) if value == "stable"
    ));
    let overwrite = authorized.overwrite.unwrap();
    assert_eq!(overwrite.plugin_id.unwrap().value, "oll.example");
    assert_eq!(overwrite.expected_declaration_sha256, vec![7; 32]);
}

#[test]
fn confirmation_is_digest_bound_and_defaults_to_no() {
    let response = oll::ReconcilePluginInstallationsResponse {
        results: vec![oll::PluginInstallationResult {
            plugin_id: Some(oll::PluginId {
                value: "oll.example".to_owned(),
            }),
            plugin_name: Some(oll::PluginName {
                value: "example".to_owned(),
            }),
            outcome: oll::PluginInstallationOutcome::ConfirmationRequired as i32,
            diagnostics: vec![],
            confirmation: Some(oll::PluginOverwriteConfirmation {
                redacted_change_summary: "remote changed".to_owned(),
                current_declaration_sha256: vec![3; 32],
            }),
        }],
    };
    let authorization = overwrite_authorization(&response).unwrap().unwrap();
    assert_eq!(authorization.plugin_id.value, "oll.example");
    assert_eq!(authorization.digest, vec![3; 32]);
    assert_eq!(authorization.summary, "remote changed");

    let mut output = Vec::new();
    assert!(confirm_overwrite_with("change", &mut Cursor::new(b"yes\n"), &mut output).unwrap());
    assert!(!confirm_overwrite_with("change", &mut Cursor::new(b""), &mut Vec::new()).unwrap());
    assert!(String::from_utf8(output).unwrap().contains("[y/N]"));
}

#[test]
fn partial_or_unresolved_package_results_fail_the_command() {
    let successful = installation_response(oll::PluginInstallationOutcome::Updated);
    assert!(output::installation_result_status(&successful).is_ok());
    assert_eq!(
        output::installation_result_json(&successful.results[0]).unwrap(),
        json!({
            "plugin_id": "oll.example",
            "plugin_name": "example",
            "outcome": "updated",
            "diagnostics": [],
        })
    );

    for outcome in [
        oll::PluginInstallationOutcome::Failed,
        oll::PluginInstallationOutcome::ConfirmationRequired,
    ] {
        assert!(output::installation_result_status(&installation_response(outcome)).is_err());
    }

    let mut partial = successful;
    let mut failed = installation_response(oll::PluginInstallationOutcome::Failed)
        .results
        .pop()
        .unwrap();
    failed.plugin_id.as_mut().unwrap().value = "oll.failed".to_owned();
    failed.plugin_name.as_mut().unwrap().value = "failed".to_owned();
    failed.diagnostics.push(oll::PluginDiagnostic {
        code: "recipe_step_failed".to_owned(),
        phase: "build".to_owned(),
        message: "the source recipe failed".to_owned(),
        hint: None,
        build_log_path: Some("/plugin-data/oll.failed/build.log".to_owned()),
        ..Default::default()
    });
    partial.results.push(failed);
    assert_eq!(partial.results.len(), 2);
    assert!(output::installation_result_status(&partial).is_err());
    let failed_json = output::installation_result_json(&partial.results[1]).unwrap();
    assert_eq!(failed_json["outcome"], "failed");
    assert_eq!(failed_json["diagnostics"][0]["code"], "recipe_step_failed");
    assert!(failed_json["diagnostics"][0]["hint"].is_null());
    for field in [
        "plugin_id",
        "plugin_name",
        "sanitized_remote",
        "branch",
        "revision",
        "release_id",
        "target",
    ] {
        assert!(failed_json["diagnostics"][0][field].is_null());
    }

    let mut diagnostic = partial.results[1].diagnostics[0].clone();
    diagnostic.plugin_id = Some(oll::PluginId {
        value: "oll.failed".to_owned(),
    });
    diagnostic.plugin_name = Some(oll::PluginName {
        value: "failed".to_owned(),
    });
    diagnostic.sanitized_remote = Some("https://user:secret@example.test/plugin.git".to_owned());
    diagnostic.branch = Some("stable".to_owned());
    diagnostic.release_id = Some("release-7".to_owned());
    diagnostic.target = Some("x86_64-unknown-linux-gnu".to_owned());
    diagnostic.hint = Some("inspect the retained output".to_owned());
    let diagnostic_json = output::diagnostic_json(&diagnostic);
    assert_eq!(diagnostic_json["plugin_id"], "oll.failed");
    assert_eq!(diagnostic_json["plugin_name"], "failed");
    assert_eq!(
        diagnostic_json["sanitized_remote"],
        "https://example.test/plugin.git"
    );
    assert_eq!(diagnostic_json["branch"], "stable");
    assert!(diagnostic_json["revision"].is_null());
    assert_eq!(diagnostic_json["release_id"], "release-7");
    assert_eq!(diagnostic_json["target"], "x86_64-unknown-linux-gnu");
    let human = output::diagnostic_human(&diagnostic);
    assert!(human.contains("[build:recipe_step_failed]"));
    assert!(human.contains("the source recipe failed"));
    assert!(human.contains("remote: https://example.test/plugin.git"));
    assert!(!human.contains("user"));
    assert!(!human.contains("secret"));
    assert!(human.contains("branch: stable"));
    assert!(human.contains("release: release-7"));
    assert!(human.contains("target: x86_64-unknown-linux-gnu"));
    assert!(human.contains("hint: inspect the retained output"));
    assert!(human.contains("build log: /plugin-data/oll.failed/build.log"));
}

#[test]
fn plugin_summary_json_has_the_stable_shape_and_explicit_nulls() {
    let summary = oll::PluginSummary {
        plugin_id: Some(oll::PluginId {
            value: "oll.example".to_owned(),
        }),
        plugin_name: Some(oll::PluginName {
            value: "example".to_owned(),
        }),
        desired_state: oll::PluginDesiredState::Stopped as i32,
        process_state: oll::PluginProcessState::Exited as i32,
        current_generation: None,
        running_generation: None,
        last_error: None,
    };
    let value = output::plugin_summary_json(&summary).unwrap();
    assert_eq!(
        value,
        json!({
            "plugin_id": "oll.example",
            "plugin_name": "example",
            "desired_state": "stopped",
            "process_state": "exited",
            "current_generation": null,
            "running_generation": null,
            "last_error": null,
        })
    );
    assert!(!serde_json::to_string(&value).unwrap().contains('\u{1b}'));
}

#[test]
fn plugin_info_json_contains_every_documented_section() {
    let details = oll::PluginDetails {
        summary: Some(plugin_summary()),
        declaration: Some(oll::PluginDeclaration {
            sanitized_remote: "https://example.test/plugin.git".to_owned(),
            mode: oll::PluginPackageMode::Source as i32,
            selection: None,
            release_id: None,
            normalized_sha256: vec![1; 32],
        }),
        effective_manifest: Some(oll::PluginEffectiveManifest {
            format_version: 1,
            plugin_id: Some(oll::PluginId {
                value: "oll.example".to_owned(),
            }),
            plugin_name: Some(oll::PluginName {
                value: "example".to_owned(),
            }),
            source_dependencies: vec![],
            source_steps: vec![],
            runtime_argv: vec!["plugin".to_owned()],
            source_checkout: oll::PluginSourceCheckout::Source as i32,
        }),
        package_state: Some(oll::PluginPackageState {
            transition_state: oll::PluginPackageTransitionState::Stable as i32,
            selected_git_commit: None,
            current_generation: None,
            candidate_generation: None,
            spawn_blocked: false,
        }),
        restart_state: Some(oll::PluginRestartState {
            requested_sequence: 0,
            applied_sequence: 0,
            consecutive_failures: 0,
            next_attempt_at: None,
            last_failure: None,
        }),
        process_instance: None,
        job_counts: Some(oll::PluginJobCounts::default()),
    };
    let value = output::plugin_details_json(&details).unwrap();
    for field in [
        "declaration",
        "effective_manifest",
        "package_state",
        "restart_state",
        "process_instance",
        "job_counts",
    ] {
        assert!(value.get(field).is_some(), "missing field {field}");
    }
    assert!(value["process_instance"].is_null());
    assert!(value["restart_state"]["next_attempt_at"].is_null());
    assert_eq!(value["effective_manifest"]["source_checkout"], "source");
}

#[test]
fn job_info_json_preserves_arguments_nulls_and_artifact_integrity() {
    let details = oll::PluginJobDetails {
        summary: Some(job_summary()),
        arguments: vec!["".to_owned(), "--input".to_owned(), "x".to_owned()],
        deadline: None,
        accepted_at: Some(timestamp(1_700_000_002)),
        terminal_at: None,
        result: None,
        error: None,
        artifacts: vec![oll::StoredPluginArtifact {
            artifact_id: Some(oll::PluginArtifactId {
                value: "83933a85-64b0-4141-bbc2-296ae99e0f04".to_owned(),
            }),
            file_name: "result.pdf".to_owned(),
            media_type: "application/pdf".to_owned(),
            size_bytes: 12,
            sha256: vec![0xab; 32],
            published_path: "/downloads/result.pdf".to_owned(),
        }],
    };
    let value = output::job_details_json(&details).unwrap();
    assert_eq!(value["arguments"], json!(["", "--input", "x"]));
    assert!(value["deadline"].is_null());
    assert!(value["terminal_at"].is_null());
    assert!(value["error"].is_null());
    assert_eq!(value["artifacts"][0]["size_bytes"], 12);
    assert_eq!(value["artifacts"][0]["sha256"], "ab".repeat(32));
}

#[test]
fn plugin_log_filter_uses_recorded_identity_without_rebinding_names() {
    let source = concat!(
        "{\"plugin_id\":\"oll.one\",\"plugin_name\":\"shared\",\"message\":\"old\"}\n",
        "{\"plugin_id\":\"oll.two\",\"plugin_name\":\"other\",\"message\":\"middle\"}\n",
        "{\"plugin_id\":\"oll.two\",\"plugin_name\":\"shared\",\"message\":\"new\"}\n",
    );

    let mut by_id = Vec::new();
    local::filter_plugin_log_for_test(
        Cursor::new(source),
        &mut by_id,
        PluginLogTarget::Plugin("oll.one".to_owned()),
    )
    .unwrap();
    let by_id = String::from_utf8(by_id).unwrap();
    assert!(by_id.contains("old"));
    assert!(!by_id.contains("middle"));
    assert!(!by_id.contains("new"));

    let mut by_name = Vec::new();
    local::filter_plugin_log_for_test(
        Cursor::new(source),
        &mut by_name,
        PluginLogTarget::Plugin("shared".to_owned()),
    )
    .unwrap();
    let by_name = String::from_utf8(by_name).unwrap();
    assert!(by_name.contains("old"));
    assert!(by_name.contains("new"));
    assert!(!by_name.contains("middle"));
}

#[test]
fn plugin_log_reader_ignores_a_concurrently_written_trailing_record() {
    let source = concat!(
        "{\"plugin_id\":\"oll.one\",\"plugin_name\":\"one\",\"message\":\"complete\"}\n",
        "{\"plugin_id\":\"oll.one\",\"plugin_name\":\"one\",\"message\":\"partial",
    );

    let mut filtered = Vec::new();
    local::filter_plugin_log_for_test(
        Cursor::new(source),
        &mut filtered,
        PluginLogTarget::Plugin("oll.one".to_owned()),
    )
    .unwrap();
    let filtered = String::from_utf8(filtered).unwrap();
    assert!(filtered.contains("complete"));
    assert!(!filtered.contains("partial"));

    let mut unfiltered = Vec::new();
    local::filter_plugin_log_for_test(Cursor::new(source), &mut unfiltered, PluginLogTarget::All)
        .unwrap();
    assert_eq!(String::from_utf8(unfiltered).unwrap(), filtered);
}

#[test]
fn plugin_log_selector_is_validated_before_the_log_file_is_opened() {
    let directory = tempfile::TempDir::new().unwrap();
    let missing_log = directory.path().join("missing-plugin.log");

    let error = local::show_plugin_log(
        &missing_log,
        PluginLogTarget::Plugin("__invalid__".to_owned()),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        crate::node::runtime::NodeError::Operation(_)
    ));

    let error = local::show_plugin_log(&missing_log, PluginLogTarget::All).unwrap_err();
    assert!(matches!(error, crate::node::runtime::NodeError::Io { .. }));
}

fn installation_response(
    outcome: oll::PluginInstallationOutcome,
) -> oll::ReconcilePluginInstallationsResponse {
    oll::ReconcilePluginInstallationsResponse {
        results: vec![oll::PluginInstallationResult {
            plugin_id: Some(oll::PluginId {
                value: "oll.example".to_owned(),
            }),
            plugin_name: Some(oll::PluginName {
                value: "example".to_owned(),
            }),
            outcome: outcome as i32,
            diagnostics: vec![],
            confirmation: None,
        }],
    }
}

fn plugin_summary() -> oll::PluginSummary {
    oll::PluginSummary {
        plugin_id: Some(oll::PluginId {
            value: "oll.example".to_owned(),
        }),
        plugin_name: Some(oll::PluginName {
            value: "example".to_owned(),
        }),
        desired_state: oll::PluginDesiredState::Stopped as i32,
        process_state: oll::PluginProcessState::Exited as i32,
        current_generation: None,
        running_generation: None,
        last_error: None,
    }
}

fn job_summary() -> oll::PluginJobSummary {
    oll::PluginJobSummary {
        job_id: Some(oll::PluginJobId {
            value: "c588e4a1-9707-4702-bb74-e50ff948b88e".to_owned(),
        }),
        plugin_id: Some(oll::PluginId {
            value: "oll.example".to_owned(),
        }),
        plugin_name: Some(oll::PluginName {
            value: "example".to_owned(),
        }),
        operation_id: "operation-1".to_owned(),
        action: "render".to_owned(),
        state: oll::PluginAdminJobState::Running as i32,
        created_at: Some(timestamp(1_700_000_000)),
        updated_at: Some(timestamp(1_700_000_001)),
    }
}

fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}
