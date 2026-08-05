use std::{
    fs,
    path::Path,
    sync::{Arc, atomic::AtomicU64},
};

use sha2::{Digest as _, Sha256};
use sqlx::{AnyPool, any::AnyPoolOptions};
use tempfile::TempDir;
use time::OffsetDateTime;
use url::Url;
use uuid::Uuid;

use crate::{
    plugin::{
        InstallMode, JobAdmission, JobCancellation, JobCancellationReason, JobState,
        NormalizedJobPayload, PackagePublishIntent, PluginInstanceId, PluginJob, PluginJobId,
        PluginOperationId, PluginStore,
    },
    protocol::oll::{self, config_value},
};

use super::{
    InstanceCommand, InstanceSender,
    jobs::job_update_log_fields,
    session::{
        quiescing_allows, send_payload, try_send_payload, validate_envelope,
        validate_handshake_trace,
    },
    supervisor::{dispatch_job_cancellation, retained_operation},
    value::{validate_config_function_arguments, validate_serializable_config_value},
};

#[tokio::test]
async fn business_output_backpressures_instead_of_being_dropped() {
    let (outgoing, mut receiver) = tokio::sync::mpsc::channel(1);
    let ids = Arc::new(AtomicU64::new(1));
    let instance_id = PluginInstanceId::new();
    let trace = oll::TraceContext {
        correlation_id: "outbound-backpressure".to_owned(),
        parent_call_id: None,
        call_depth: 0,
        causal_depth: 0,
        task_id: None,
        task_group_id: None,
    };
    try_send_payload(
        &outgoing,
        &ids,
        "session",
        instance_id,
        trace.clone(),
        None,
        oll::plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: 1 }),
    )
    .unwrap();

    let blocked_outgoing = outgoing.clone();
    let blocked_ids = Arc::clone(&ids);
    let blocked = tokio::spawn(async move {
        send_payload(
            &blocked_outgoing,
            &blocked_ids,
            "session",
            instance_id,
            trace,
            None,
            oll::plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce: 2 }),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!blocked.is_finished());
    receiver.recv().await.unwrap().unwrap();
    blocked.await.unwrap().unwrap();
    let second = receiver.recv().await.unwrap().unwrap();
    assert!(matches!(
        second.payload,
        Some(oll::plugin_envelope::Payload::Heartbeat(oll::Heartbeat {
            nonce: 2
        }))
    ));
}

#[test]
fn plugin_envelopes_reject_stale_session_instance_and_message_ids() {
    let instance_id = PluginInstanceId::new();
    let envelope = |message_id| oll::PluginEnvelope {
        message_id,
        reply_to: None,
        session_id: "session".to_owned(),
        plugin_instance_id: instance_id.to_string(),
        trace: Some(oll::TraceContext {
            correlation_id: "message-id-test".to_owned(),
            parent_call_id: None,
            call_depth: 0,
            causal_depth: 0,
            task_id: None,
            task_group_id: None,
        }),
        payload: Some(oll::plugin_envelope::Payload::Heartbeat(oll::Heartbeat {
            nonce: message_id,
        })),
    };

    let mut plugin_last_seen = 0;
    validate_envelope(&envelope(7), "session", instance_id, &mut plugin_last_seen).unwrap();
    validate_envelope(&envelope(19), "session", instance_id, &mut plugin_last_seen).unwrap();
    assert_eq!(
        plugin_last_seen, 19,
        "gaps must not allocate retained state"
    );
    assert!(
        validate_envelope(&envelope(19), "session", instance_id, &mut plugin_last_seen).is_err(),
        "a duplicate ID must be rejected"
    );
    assert!(
        validate_envelope(&envelope(18), "session", instance_id, &mut plugin_last_seen).is_err(),
        "an out-of-order ID must be rejected"
    );
    assert!(
        validate_envelope(&envelope(0), "session", instance_id, &mut plugin_last_seen).is_err(),
        "zero must be rejected"
    );
    assert_eq!(plugin_last_seen, 19, "rejected IDs must not advance state");

    let mut stale_session = envelope(20);
    stale_session.session_id = "stale-session".to_owned();
    assert!(
        validate_envelope(
            &stale_session,
            "session",
            instance_id,
            &mut plugin_last_seen,
        )
        .is_err(),
        "an envelope from a stale session must be rejected"
    );
    assert_eq!(
        plugin_last_seen, 19,
        "a stale session must not advance message state"
    );

    let mut stale_instance = envelope(20);
    stale_instance.plugin_instance_id = PluginInstanceId::new().to_string();
    assert!(
        validate_envelope(
            &stale_instance,
            "session",
            instance_id,
            &mut plugin_last_seen,
        )
        .is_err(),
        "an envelope from a stale plugin instance must be rejected"
    );
    assert_eq!(
        plugin_last_seen, 19,
        "a stale instance must not advance message state"
    );
    validate_envelope(&envelope(20), "session", instance_id, &mut plugin_last_seen).unwrap();
    assert_eq!(
        plugin_last_seen, 20,
        "stale envelopes must not poison the active session sequence"
    );

    let mut host_last_seen = 0;
    validate_envelope(&envelope(7), "session", instance_id, &mut host_last_seen).unwrap();
    assert_eq!(host_last_seen, 7);
    assert_eq!(plugin_last_seen, 20, "sender sequences are independent");
}

#[test]
fn plugin_handshake_must_inherit_the_exact_lifecycle_trace() {
    let lifecycle = oll::TraceContext {
        correlation_id: "plugin-lifecycle-correlation".to_owned(),
        parent_call_id: None,
        call_depth: 0,
        causal_depth: 0,
        task_id: None,
        task_group_id: None,
    };
    assert!(validate_handshake_trace(&lifecycle, &lifecycle).is_ok());

    let mut wrong_correlation = lifecycle.clone();
    wrong_correlation.correlation_id = "unrelated-correlation".to_owned();
    assert!(validate_handshake_trace(&wrong_correlation, &lifecycle).is_err());

    let mut nested = lifecycle.clone();
    nested.call_depth = 1;
    assert!(validate_handshake_trace(&nested, &lifecycle).is_err());
}

#[test]
fn host_job_update_log_fields_redact_plugin_payloads() {
    let status_secret = "status-secret-never-in-oll-log";
    let result_secret = "result-secret-never-in-oll-log";
    let error_secret = "error-secret-never-in-oll-log";
    let artifact_secret = "artifact-secret-never-in-oll-log";
    let plugin_id = "oll.redaction-test".parse().unwrap();
    let instance_id = PluginInstanceId::new();
    let job_id = PluginJobId::new();
    let update = oll::JobUpdate {
        job_id: None,
        state: oll::JobState::Running as i32,
        progress: Some(0.5),
        status_message: Some(status_secret.to_owned()),
        result: Some(oll::ConfigValue {
            kind: Some(config_value::Kind::StringValue(result_secret.to_owned())),
        }),
        error: Some(oll::ProtocolError {
            code: oll::ErrorCode::Internal as i32,
            message: error_secret.to_owned(),
            retryable: false,
            ..Default::default()
        }),
        artifacts: vec![oll::ArtifactDescriptor {
            artifact_id: None,
            file_name: artifact_secret.to_owned(),
            media_type: "application/octet-stream".to_owned(),
            size_bytes: 17,
            sha256: vec![42; 32],
        }],
    };
    let trace = oll::TraceContext {
        correlation_id: "job-update-trace".to_owned(),
        parent_call_id: Some(41),
        call_depth: 2,
        causal_depth: 3,
        task_id: Some("task-7".to_owned()),
        task_group_id: Some("group-9".to_owned()),
    };

    let fields = job_update_log_fields(
        &plugin_id,
        instance_id,
        job_id,
        JobState::Running,
        None,
        &update,
        &trace,
    );
    let encoded = serde_json::to_string(&fields).unwrap();
    for secret in [status_secret, result_secret, error_secret, artifact_secret] {
        assert!(!encoded.contains(secret));
    }
    assert!(encoded.contains("\"status_message_present\":true"));
    assert!(encoded.contains("\"result_present\":true"));
    assert!(encoded.contains("\"error_present\":true"));
    assert!(encoded.contains("\"artifact_count\":1"));
    assert_eq!(fields["correlation_id"], "job-update-trace");
    assert_eq!(fields["parent_call_id"], 41);
    assert_eq!(fields["call_depth"], 2);
    assert_eq!(fields["causal_depth"], 3);
    assert_eq!(fields["task_id"], "task-7");
    assert_eq!(fields["task_group_id"], "group-9");
}

#[test]
fn quiescing_session_accepts_responses_and_final_log_records_only() {
    use oll::plugin_envelope::Payload;

    assert!(quiescing_allows(&Payload::ShutdownAcknowledged(
        oll::ShutdownAcknowledged {},
    )));
    assert!(quiescing_allows(&Payload::JobAccepted(
        oll::JobAccepted::default(),
    )));
    assert!(quiescing_allows(&Payload::Log(oll::LogRecord::default())));
    assert!(!quiescing_allows(&Payload::HostCall(
        oll::HostCallRequest::default(),
    )));
    assert!(!quiescing_allows(&Payload::ArtifactStart(
        oll::ArtifactTransferStart::default(),
    )));
    assert!(!quiescing_allows(&Payload::JobUpdate(
        oll::JobUpdate::default(),
    )));
}

async fn sqlite_pool(path: &Path) -> AnyPool {
    sqlx::any::install_default_drivers();
    fs::File::create(path).unwrap();
    let url = Url::from_file_path(path)
        .unwrap()
        .as_str()
        .replacen("file:", "sqlite:", 1);
    AnyPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap()
}

#[tokio::test]
async fn retained_operation_retry_precedes_runtime_readiness() {
    let directory = TempDir::new().unwrap();
    let store =
        PluginStore::initialize(sqlite_pool(&directory.path().join("plugin.sqlite3")).await)
            .await
            .unwrap();
    let plugin_id: crate::plugin::PluginId = "oll.retry-test".parse().unwrap();
    let generation = Uuid::new_v4();
    let declaration = b"retry-test";
    let package = PackagePublishIntent {
        plugin_id: plugin_id.clone(),
        plugin_name: "retry-test".parse().unwrap(),
        operation_id: "install-retry-test".to_owned(),
        expected_current_generation: None,
        candidate_generation: generation,
        normalized_declaration: declaration.to_vec(),
        declaration_sha256: Sha256::digest(declaration).into(),
        effective_manifest: b"unused by retained-operation test".to_vec(),
        selected_commit: None,
        install_mode: InstallMode::Source,
        release_id: None,
        correlation_id: "install-retry-test".to_owned(),
    };
    store.prepare_package_publish(&package).await.unwrap();
    store
        .finalize_package_publish(&plugin_id, generation)
        .await
        .unwrap();
    let instance_id = PluginInstanceId::new();
    store
        .set_desired_state(&plugin_id, crate::plugin::DesiredPluginState::Running)
        .await
        .unwrap();
    store
        .record_running_instance(&plugin_id, generation, instance_id)
        .await
        .unwrap();

    let payload = NormalizedJobPayload::new(plugin_id, "render".to_owned(), vec![], None).unwrap();
    let operation_id: PluginOperationId = "retained-operation".parse().unwrap();
    let JobAdmission::Created(created) = store
        .admit_job(
            &operation_id,
            &payload,
            instance_id,
            OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap(),
            "retained-operation-correlation",
        )
        .await
        .unwrap()
    else {
        panic!("first admission must create a job")
    };

    // The package is desired-stopped and there is no active runtime. An
    // idempotent retry must nevertheless return its original durable row.
    let retried = retained_operation(&store, &operation_id, &payload)
        .await
        .unwrap()
        .expect("retained operation");
    assert_eq!(retried.job_id, created.job_id);

    let different = NormalizedJobPayload::new(
        payload.plugin_id.clone(),
        "another-action".to_owned(),
        vec![],
        None,
    )
    .unwrap();
    assert!(
        retained_operation(&store, &operation_id, &different)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancellation_dispatch_returns_without_waiting_for_plugin_acknowledgement() {
    let (work, mut receiver) = tokio::sync::mpsc::channel(1);
    let (shutdown, _shutdown_receiver) = tokio::sync::watch::channel(None);
    let sender = InstanceSender { work, shutdown };
    let job = test_job();
    let session = tokio::spawn(async move {
        let Some(InstanceCommand::CancelJob { dispatched, .. }) = receiver.recv().await else {
            panic!("expected cancellation command");
        };
        dispatched.send(Ok(())).unwrap();
        // Deliberately model a plugin that never sends CancelJobAcknowledged.
        std::future::pending::<()>().await;
    });

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        dispatch_job_cancellation(&sender, &job, JobCancellationReason::UserRequest),
    )
    .await
    .expect("dispatch must not wait for the plugin acknowledgement")
    .unwrap();
    session.abort();
}

#[tokio::test]
async fn repeated_cancellation_dispatches_exactly_one_outbound_request() {
    let (work, mut receiver) = tokio::sync::mpsc::channel(2);
    let (shutdown, _shutdown_receiver) = tokio::sync::watch::channel(None);
    let sender = InstanceSender { work, shutdown };
    let job = test_job();
    let first = JobCancellation {
        job: job.clone(),
        send_request: true,
    };
    let repeated = JobCancellation {
        job: job.clone(),
        send_request: false,
    };

    assert!(first.needs_request_dispatch());
    assert!(!repeated.needs_request_dispatch());
    if first.needs_request_dispatch() {
        let sender = sender.clone();
        let job = first.job.clone();
        tokio::spawn(async move {
            dispatch_job_cancellation(&sender, &job, JobCancellationReason::UserRequest)
                .await
                .unwrap();
        });
    }
    if repeated.needs_request_dispatch() {
        unreachable!("a repeated cancellation must not own request dispatch");
    }

    let Some(InstanceCommand::CancelJob { dispatched, .. }) = receiver.recv().await else {
        panic!("expected one cancellation command");
    };
    dispatched.send(Ok(())).unwrap();
    tokio::task::yield_now().await;
    assert!(receiver.try_recv().is_err());
}

#[test]
fn terminal_cancellation_snapshot_never_requests_plugin_dispatch() {
    let mut job = test_job();
    job.state = JobState::Succeeded;
    job.terminal_at = Some(job.updated_at);
    let raced = JobCancellation {
        job,
        send_request: true,
    };

    assert!(!raced.needs_request_dispatch());
}

#[test]
fn durable_config_values_reject_session_handles_and_invalid_scalars() {
    let function = oll::ConfigValue {
        kind: Some(config_value::Kind::FunctionValue(oll::ConfigFunctionRef {
            session_id: "session".to_owned(),
            function_id: "function".to_owned(),
        })),
    };
    assert!(validate_serializable_config_value(&function).is_err());

    let non_finite = oll::ConfigValue {
        kind: Some(config_value::Kind::NumberValue(f64::NAN)),
    };
    assert!(validate_serializable_config_value(&non_finite).is_err());

    let invalid_duration = oll::ConfigValue {
        kind: Some(config_value::Kind::DurationValue(prost_types::Duration {
            seconds: 1,
            nanos: -1,
        })),
    };
    assert!(validate_serializable_config_value(&invalid_duration).is_err());
}

#[test]
fn function_arguments_accept_only_valid_values_and_current_session_handles() {
    let current = oll::ConfigValue {
        kind: Some(config_value::Kind::ListValue(oll::ConfigList {
            values: vec![oll::ConfigValue {
                kind: Some(config_value::Kind::FunctionValue(oll::ConfigFunctionRef {
                    session_id: "session".to_owned(),
                    function_id: "function".to_owned(),
                })),
            }],
        })),
    };
    validate_config_function_arguments(&[current], "session").unwrap();

    let stale = oll::ConfigValue {
        kind: Some(config_value::Kind::FunctionValue(oll::ConfigFunctionRef {
            session_id: "stale-session".to_owned(),
            function_id: "function".to_owned(),
        })),
    };
    assert!(matches!(
        validate_config_function_arguments(&[stale], "session"),
        Err(crate::plugin::PluginError::FailedPrecondition(_))
    ));

    let mut too_deep = oll::ConfigValue {
        kind: Some(config_value::Kind::NullValue(
            prost_types::NullValue::NullValue as i32,
        )),
    };
    for _ in 0..34 {
        too_deep = oll::ConfigValue {
            kind: Some(config_value::Kind::ListValue(oll::ConfigList {
                values: vec![too_deep],
            })),
        };
    }
    assert!(validate_config_function_arguments(&[too_deep], "session").is_err());
}

#[test]
fn config_value_wire_and_domain_enforce_the_documented_maximum_depth() {
    use prost::Message as _;

    fn nested_values(depth: usize) -> (oll::ConfigValue, oll::ConfigValue) {
        let mut list = oll::ConfigValue {
            kind: Some(config_value::Kind::NullValue(
                prost_types::NullValue::NullValue as i32,
            )),
        };
        let mut map = list.clone();
        for _ in 0..depth {
            list = oll::ConfigValue {
                kind: Some(config_value::Kind::ListValue(oll::ConfigList {
                    values: vec![list],
                })),
            };
            map = oll::ConfigValue {
                kind: Some(config_value::Kind::MapValue(oll::ConfigMap {
                    entries: std::collections::HashMap::from([("value".to_owned(), map)]),
                })),
            };
        }
        (list, map)
    }

    let (maximum_list, maximum_map) = nested_values(33);
    for value in [maximum_list, maximum_map] {
        let encoded = value.encode_to_vec();
        let decoded = oll::ConfigValue::decode(encoded.as_slice()).unwrap();
        validate_config_function_arguments(&[decoded], "session").unwrap();
    }

    let (over_limit_list, over_limit_map) = nested_values(34);
    let list_encoded = over_limit_list.encode_to_vec();
    let decoded_list = oll::ConfigValue::decode(list_encoded.as_slice()).unwrap();
    assert!(validate_config_function_arguments(&[decoded_list], "session").is_err());
    assert!(
        validate_config_function_arguments(std::slice::from_ref(&over_limit_map), "session")
            .is_err()
    );

    let map_encoded = over_limit_map.encode_to_vec();
    assert!(oll::ConfigValue::decode(map_encoded.as_slice()).is_err());
}

fn test_job() -> PluginJob {
    let admitted_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    PluginJob {
        job_id: PluginJobId::new(),
        operation_id: "cancel-test".parse().unwrap(),
        payload: NormalizedJobPayload::new(
            "oll.cancel-test".parse().unwrap(),
            "run".to_owned(),
            vec![],
            None,
        )
        .unwrap(),
        absolute_deadline: admitted_at + time::Duration::hours(1),
        state: JobState::Cancelling,
        cancellation_reason: Some(JobCancellationReason::UserRequest),
        plugin_instance_id: PluginInstanceId::new(),
        admitted_at,
        accepted_at: Some(admitted_at),
        terminal_at: None,
        updated_at: admitted_at,
        correlation_id: "cancel-test-correlation".to_owned(),
        result: None,
        error_code: None,
        error_message: None,
    }
}
