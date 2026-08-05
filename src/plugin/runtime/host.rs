use crate::{
    configuration::{PluginConfigError, PluginConfigErrorKind},
    node::logging::LogLevel,
    plugin::{PluginError, PluginId, PluginInstanceId},
    protocol::oll::{self, host_call_request, host_call_response},
    replica::{OperationSource, ReplicaError},
};

use super::{
    RuntimeDependencies, trace::insert_trace_fields, value::validate_config_function_arguments,
};

pub(super) async fn execute_host_call(
    dependencies: RuntimeDependencies,
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    session_id: String,
    request: oll::HostCallRequest,
    trace: &oll::TraceContext,
) -> oll::HostCallResponse {
    use host_call_request::Call;
    use host_call_response::Result as CallResult;

    let call_kind = host_call_kind(&request);
    dependencies.logger.emit(
        LogLevel::Info,
        "oll::plugin::host_call",
        "plugin_host_call_started",
        &trace.correlation_id,
        host_call_log_fields(plugin_id, instance_id, call_kind, trace),
    );
    let started_at = std::time::Instant::now();
    let result = match request.call {
        Some(Call::ReadDocument(request)) => dependencies
            .replica
            .read_document(request)
            .await
            .map(CallResult::ReadDocument)
            .map_err(replica_error),
        Some(Call::ListDirectory(request)) => dependencies
            .replica
            .list_directory(request)
            .await
            .map(CallResult::ListDirectory)
            .map_err(replica_error),
        Some(Call::GetDirectoryTree(request)) => dependencies
            .replica
            .get_directory_tree(request)
            .await
            .map(CallResult::GetDirectoryTree)
            .map_err(replica_error),
        Some(Call::ReadCrdt(request)) => dependencies
            .replica
            .read_crdt(request)
            .await
            .map(CallResult::ReadCrdt)
            .map_err(replica_error),
        Some(Call::CommitDocuments(request)) => dependencies
            .replica
            .commit_documents(request, OperationSource::Plugin, &trace.correlation_id)
            .await
            .map(CallResult::CommitDocuments)
            .map_err(replica_error),
        Some(Call::GetConfig(request)) => {
            let config = dependencies.config.clone();
            let path = request.path.unwrap_or_default();
            blocking_config(move || config.get_plugin_config(&session_id, &path))
                .await
                .map(|value| CallResult::GetConfig(oll::GetConfigResponse { value: Some(value) }))
        }
        Some(Call::InvokeConfigFunction(request)) => {
            let config = dependencies.config.clone();
            let function = request.function.ok_or_else(|| {
                protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "configuration function is required",
                    false,
                )
            });
            let arguments = request.arguments;
            match function {
                Ok(function) => {
                    if let Err(error) = validate_config_function_arguments(&arguments, &session_id)
                    {
                        Err(plugin_error(error))
                    } else {
                        blocking_config(move || {
                            config.invoke_plugin_config_function(&session_id, &function, &arguments)
                        })
                        .await
                        .map(|results| {
                            CallResult::InvokeConfigFunction(oll::InvokeConfigFunctionResponse {
                                results,
                            })
                        })
                    }
                }
                Err(error) => Err(error),
            }
        }
        None => Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "host call kind is required",
            false,
        )),
    };

    dependencies.logger.emit(
        if result.is_ok() {
            LogLevel::Info
        } else {
            LogLevel::Warn
        },
        "oll::plugin::host_call",
        "plugin_host_call_completed",
        &trace.correlation_id,
        completed_host_call_log_fields(
            plugin_id,
            instance_id,
            call_kind,
            trace,
            result.as_ref().err(),
            u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        ),
    );

    match result {
        Ok(result) => oll::HostCallResponse {
            result: Some(result),
        },
        Err(error) => error_response(error),
    }
}

fn host_call_kind(request: &oll::HostCallRequest) -> &'static str {
    use host_call_request::Call;

    match request.call.as_ref() {
        Some(Call::ReadDocument(_)) => "read_document",
        Some(Call::ListDirectory(_)) => "list_directory",
        Some(Call::GetDirectoryTree(_)) => "get_directory_tree",
        Some(Call::ReadCrdt(_)) => "read_crdt",
        Some(Call::CommitDocuments(_)) => "commit_documents",
        Some(Call::GetConfig(_)) => "get_config",
        Some(Call::InvokeConfigFunction(_)) => "invoke_config_function",
        None => "unspecified",
    }
}

fn host_call_log_fields(
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    call_kind: &'static str,
    trace: &oll::TraceContext,
) -> serde_json::Value {
    let mut fields = serde_json::json!({
        "plugin_id": plugin_id.as_str(),
        "plugin_instance_id": instance_id.to_string(),
        "call_kind": call_kind,
    });
    insert_trace_fields(
        fields
            .as_object_mut()
            .expect("host call log fields are an object"),
        trace,
    );
    fields
}

fn completed_host_call_log_fields(
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    call_kind: &'static str,
    trace: &oll::TraceContext,
    error: Option<&oll::ProtocolError>,
    duration_ms: u64,
) -> serde_json::Value {
    let mut fields = host_call_log_fields(plugin_id, instance_id, call_kind, trace);
    let fields = fields
        .as_object_mut()
        .expect("host call log fields are an object");
    fields.insert(
        "outcome".to_owned(),
        serde_json::Value::String(if error.is_some() {
            "failure".to_owned()
        } else {
            "success".to_owned()
        }),
    );
    fields.insert(
        "error_code".to_owned(),
        error.map_or(serde_json::Value::Null, |error| {
            serde_json::Value::String(
                oll::ErrorCode::try_from(error.code)
                    .ok()
                    .map_or("UNKNOWN", |code| code.as_str_name())
                    .to_owned(),
            )
        }),
    );
    fields.insert(
        "retryable".to_owned(),
        serde_json::Value::Bool(error.is_some_and(|error| error.retryable)),
    );
    fields.insert("duration_ms".to_owned(), duration_ms.into());
    serde_json::Value::Object(fields.clone())
}

async fn blocking_config<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, PluginConfigError> + Send + 'static,
) -> Result<T, oll::ProtocolError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| {
            protocol_error(
                oll::ErrorCode::Internal,
                "plugin configuration task failed",
                false,
            )
        })?
        .map_err(config_error)
}

fn error_response(error: oll::ProtocolError) -> oll::HostCallResponse {
    oll::HostCallResponse {
        result: Some(host_call_response::Result::Error(error)),
    }
}

pub(super) fn protocol_error(
    code: oll::ErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> oll::ProtocolError {
    oll::ProtocolError {
        code: code as i32,
        message: message.into(),
        retryable,
        metadata: Default::default(),
        details: Vec::new(),
    }
}

pub(super) fn plugin_error(error: PluginError) -> oll::ProtocolError {
    let (code, message) = match error {
        PluginError::InvalidArgument(message) => (oll::ErrorCode::InvalidArgument, message),
        PluginError::NotFound(message) => (oll::ErrorCode::NotFound, message),
        PluginError::AlreadyExists(message) => (oll::ErrorCode::AlreadyExists, message),
        PluginError::Aborted(message) => (oll::ErrorCode::Cancelled, message),
        PluginError::FailedPrecondition(message) => (oll::ErrorCode::FailedPrecondition, message),
        PluginError::CorruptStore(_) | PluginError::Store(_) | PluginError::Io { .. } => (
            oll::ErrorCode::Internal,
            "plugin host operation failed; inspect the correlated daemon log".to_owned(),
        ),
    };
    protocol_error(code, message, false)
}

fn replica_error(error: ReplicaError) -> oll::ProtocolError {
    let (code, message, retryable) = match error {
        ReplicaError::Uninitialized => (
            oll::ErrorCode::FailedPrecondition,
            "no local replica yet".to_owned(),
            true,
        ),
        ReplicaError::InvalidArgument(message) | ReplicaError::InvalidSnapshot(message) => {
            (oll::ErrorCode::InvalidArgument, message, false)
        }
        ReplicaError::NotFound(message) => (oll::ErrorCode::NotFound, message, false),
        ReplicaError::AlreadyExists(message) => (oll::ErrorCode::AlreadyExists, message, false),
        ReplicaError::RevisionConflict(message) => {
            (oll::ErrorCode::RevisionConflict, message, true)
        }
        ReplicaError::Configuration(_)
        | ReplicaError::CorruptStore(_)
        | ReplicaError::Io { .. }
        | ReplicaError::Store(_)
        | ReplicaError::Internal(_) => (
            oll::ErrorCode::Internal,
            "replica operation failed; inspect the correlated daemon log".to_owned(),
            false,
        ),
    };
    protocol_error(code, message, retryable)
}

fn config_error(error: PluginConfigError) -> oll::ProtocolError {
    let code = match error.kind() {
        PluginConfigErrorKind::InvalidArgument => oll::ErrorCode::InvalidArgument,
        PluginConfigErrorKind::NotFound => oll::ErrorCode::NotFound,
        PluginConfigErrorKind::AlreadyExists => oll::ErrorCode::AlreadyExists,
        PluginConfigErrorKind::FailedPrecondition => oll::ErrorCode::FailedPrecondition,
        PluginConfigErrorKind::Internal => oll::ErrorCode::Internal,
    };
    let message = if code == oll::ErrorCode::Internal {
        "plugin configuration operation failed; inspect the correlated daemon log".to_owned()
    } else {
        error.to_string()
    };
    protocol_error(code, message, false)
}

#[cfg(test)]
mod tests {
    use std::{io, path::PathBuf};

    use crate::{
        configuration::PluginConfigError,
        plugin::{PluginError, PluginId, PluginInstanceId},
        protocol::oll,
    };

    use super::{completed_host_call_log_fields, config_error, plugin_error, protocol_error};

    #[test]
    fn internal_host_errors_do_not_cross_the_plugin_boundary() {
        let store_secret = "postgresql://user:secret@example.invalid/database";
        let plugin = plugin_error(PluginError::Store(store_secret.to_owned()));
        assert!(!plugin.message.contains(store_secret));
        assert!(plugin.message.contains("inspect the correlated daemon log"));

        let path_secret = "/private/plugin-secret/config.lua";
        let config = config_error(PluginConfigError::Read {
            path: PathBuf::from(path_secret),
            kind: io::ErrorKind::PermissionDenied,
        });
        assert!(!config.message.contains(path_secret));
        assert!(config.message.contains("inspect the correlated daemon log"));

        for error in [
            PluginConfigError::ConfigNotFound {
                path: PathBuf::from(path_secret),
            },
            PluginConfigError::InvalidUtf8 {
                path: PathBuf::from(path_secret),
            },
            PluginConfigError::Evaluation {
                path: PathBuf::from(path_secret),
            },
        ] {
            let config = config_error(error);
            assert!(!config.message.contains(path_secret));
        }
    }

    #[test]
    fn host_call_completion_log_preserves_trace_and_redacts_payloads() {
        let plugin_id: PluginId = "oll.host-log-test".parse().unwrap();
        let instance_id = PluginInstanceId::new();
        let trace = oll::TraceContext {
            correlation_id: "host-call-correlation".to_owned(),
            parent_call_id: Some(42),
            call_depth: 2,
            causal_depth: 3,
            task_id: Some("host-call-task".to_owned()),
            task_group_id: Some("host-call-group".to_owned()),
        };
        let secret = "/private/config.lua: token=correct-horse-battery-staple";
        let error = protocol_error(oll::ErrorCode::Internal, secret, true);
        let fields = completed_host_call_log_fields(
            &plugin_id,
            instance_id,
            "invoke_config_function",
            &trace,
            Some(&error),
            17,
        );

        assert_eq!(fields["plugin_id"], plugin_id.as_str());
        assert_eq!(fields["plugin_instance_id"], instance_id.to_string());
        assert_eq!(fields["call_kind"], "invoke_config_function");
        assert_eq!(fields["outcome"], "failure");
        assert_eq!(fields["error_code"], "ERROR_CODE_INTERNAL");
        assert_eq!(fields["retryable"], true);
        assert_eq!(fields["duration_ms"], 17);
        assert_eq!(fields["correlation_id"], trace.correlation_id);
        assert_eq!(fields["parent_call_id"], 42);
        assert_eq!(fields["call_depth"], 2);
        assert_eq!(fields["causal_depth"], 3);
        assert_eq!(fields["task_id"], "host-call-task");
        assert_eq!(fields["task_group_id"], "host-call-group");
        assert!(!fields.to_string().contains(secret));
    }
}
