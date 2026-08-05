use std::collections::HashMap;

use prost::Message as _;
use tonic::Status;

use crate::{
    plugin::{
        JobDeadline, JobState, PluginArtifact, PluginJob, PluginJobInspection, PluginJobListEntry,
        protocol,
    },
    protocol::oll,
};

use super::{corrupt_plugin_state, plugin_status};

pub(in crate::node::admin) fn encode_start_job_response(
    job: &PluginJob,
) -> oll::StartPluginJobResponse {
    // StartPluginJob reports the admission outcome, not a later execution
    // snapshot. Once JobAccepted was durably recorded, a fast terminal update
    // must not make the same accepted call appear to have returned another
    // state. GetPluginJob remains the current-state API.
    let response_state = if job.accepted_at.is_some() {
        JobState::Running
    } else {
        job.state
    };
    oll::StartPluginJobResponse {
        job_id: Some(protocol::encode_plugin_job_id(job.job_id)),
        state: protocol::encode_admin_job_state(response_state),
    }
}

pub(in crate::node::admin) fn encode_job_state_response(
    job: &PluginJob,
) -> oll::StopPluginJobResponse {
    oll::StopPluginJobResponse {
        job_id: Some(protocol::encode_plugin_job_id(job.job_id)),
        state: protocol::encode_admin_job_state(job.state),
    }
}

pub(in crate::node::admin) fn encode_job_list(
    entries: Vec<PluginJobListEntry>,
) -> Result<oll::ListPluginJobsResponse, Status> {
    Ok(oll::ListPluginJobsResponse {
        jobs: entries
            .into_iter()
            .map(|entry| encode_job_summary(&entry.job, &entry.plugin_name))
            .collect::<Result<_, _>>()?,
    })
}

pub(in crate::node::admin) fn encode_job_details(
    inspection: PluginJobInspection,
) -> Result<oll::GetPluginJobResponse, Status> {
    validate_job_shape(&inspection.job)?;
    let summary = encode_job_summary(&inspection.job, &inspection.plugin_name)?;
    let deadline = match inspection.job.payload.deadline {
        JobDeadline::Default24Hours => None,
        JobDeadline::Explicit(value) => {
            Some(protocol::encode_timestamp(value, "plugin_job.deadline").map_err(plugin_status)?)
        }
    };
    let accepted_at = inspection
        .job
        .accepted_at
        .map(|value| protocol::encode_timestamp(value, "plugin_job.accepted_at"))
        .transpose()
        .map_err(plugin_status)?;
    let terminal_at = inspection
        .job
        .terminal_at
        .map(|value| protocol::encode_timestamp(value, "plugin_job.terminal_at"))
        .transpose()
        .map_err(plugin_status)?;
    let result = inspection
        .job
        .result
        .as_deref()
        .map(decode_stored_result)
        .transpose()?;
    let error = encode_job_error(&inspection.job)?;
    let artifacts = inspection
        .artifacts
        .into_iter()
        .map(|artifact| encode_artifact(&inspection.job, artifact))
        .collect::<Result<_, _>>()?;

    Ok(oll::GetPluginJobResponse {
        job: Some(oll::PluginJobDetails {
            summary: Some(summary),
            arguments: inspection.job.payload.arguments,
            deadline,
            accepted_at,
            terminal_at,
            result,
            error,
            artifacts,
        }),
    })
}

fn encode_job_summary(
    job: &PluginJob,
    plugin_name: &crate::plugin::PluginName,
) -> Result<oll::PluginJobSummary, Status> {
    Ok(oll::PluginJobSummary {
        job_id: Some(protocol::encode_plugin_job_id(job.job_id)),
        plugin_id: Some(protocol::encode_plugin_id(&job.payload.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(plugin_name)),
        operation_id: protocol::encode_plugin_operation_id(&job.operation_id),
        action: job.payload.action.clone(),
        state: protocol::encode_admin_job_state(job.state),
        created_at: Some(
            protocol::encode_timestamp(job.admitted_at, "plugin_job.created_at")
                .map_err(plugin_status)?,
        ),
        updated_at: Some(
            protocol::encode_timestamp(job.updated_at, "plugin_job.updated_at")
                .map_err(plugin_status)?,
        ),
    })
}

fn validate_job_shape(job: &PluginJob) -> Result<(), Status> {
    if job.state.is_terminal() != job.terminal_at.is_some() {
        return Err(corrupt_plugin_state(
            "plugin job terminal timestamp differs from its state",
        ));
    }
    if job.updated_at < job.admitted_at
        || job.accepted_at.is_some_and(|value| value < job.admitted_at)
        || job.terminal_at.is_some_and(|value| value < job.admitted_at)
    {
        return Err(corrupt_plugin_state("plugin job timestamps are unordered"));
    }
    if job.accepted_at.is_some_and(|value| value > job.updated_at)
        || job.terminal_at.is_some_and(|value| value > job.updated_at)
    {
        return Err(corrupt_plugin_state(
            "plugin job timestamp is later than its update timestamp",
        ));
    }
    match job.state {
        JobState::Succeeded => {
            if job.error_code.is_some() || job.error_message.is_some() {
                return Err(corrupt_plugin_state(
                    "successful plugin job has stored error data",
                ));
            }
        }
        JobState::Failed => {
            if job.result.is_some() || job.error_code.as_deref().is_none_or(str::is_empty) {
                return Err(corrupt_plugin_state(
                    "failed plugin job has invalid terminal output",
                ));
            }
        }
        JobState::Cancelled | JobState::TimedOut => {
            if job.result.is_some() || job.error_code.is_some() || job.error_message.is_some() {
                return Err(corrupt_plugin_state(
                    "cancelled plugin job has stored result or error data",
                ));
            }
        }
        JobState::Dispatching | JobState::Running | JobState::Cancelling => {
            if job.result.is_some() || job.error_code.is_some() || job.error_message.is_some() {
                return Err(corrupt_plugin_state(
                    "nonterminal plugin job has terminal output",
                ));
            }
        }
    }
    Ok(())
}

fn decode_stored_result(bytes: &[u8]) -> Result<oll::ConfigValue, Status> {
    let mut remaining = bytes;
    let value = oll::ConfigValue::decode(&mut remaining)
        .map_err(|_| corrupt_plugin_state("plugin job result is not valid protobuf"))?;
    if !remaining.is_empty() {
        return Err(corrupt_plugin_state(
            "plugin job result has trailing protobuf data",
        ));
    }
    crate::plugin::runtime::validate_serializable_config_value(&value)
        .map_err(|_| corrupt_plugin_state("plugin job result is not serializable"))?;
    Ok(value)
}

fn encode_job_error(job: &PluginJob) -> Result<Option<oll::ProtocolError>, Status> {
    match job.state {
        JobState::Failed => {
            let stored_code = job
                .error_code
                .as_deref()
                .ok_or_else(|| corrupt_plugin_state("failed plugin job has no error code"))?;
            let known_code = oll::ErrorCode::from_str_name(stored_code)
                .filter(|code| *code != oll::ErrorCode::Unspecified);
            let (code, message, metadata) = match known_code {
                Some(code) => (
                    code,
                    job.error_message
                        .clone()
                        .unwrap_or_else(|| "the plugin reported a job failure".to_owned()),
                    HashMap::new(),
                ),
                None => {
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "host_error_code".to_owned(),
                        sanitized_host_code(stored_code),
                    );
                    (
                        oll::ErrorCode::Internal,
                        "the host could not complete the plugin job".to_owned(),
                        metadata,
                    )
                }
            };
            Ok(Some(oll::ProtocolError {
                code: code as i32,
                message,
                retryable: code == oll::ErrorCode::Unavailable,
                metadata,
                details: Vec::new(),
            }))
        }
        JobState::Cancelled => Ok(Some(oll::ProtocolError {
            code: oll::ErrorCode::Cancelled as i32,
            message: "the plugin job was cancelled".to_owned(),
            retryable: false,
            metadata: HashMap::new(),
            details: Vec::new(),
        })),
        JobState::TimedOut => Ok(Some(oll::ProtocolError {
            code: oll::ErrorCode::DeadlineExceeded as i32,
            message: "the plugin job exceeded its deadline".to_owned(),
            retryable: true,
            metadata: HashMap::new(),
            details: Vec::new(),
        })),
        JobState::Dispatching | JobState::Running | JobState::Cancelling | JobState::Succeeded => {
            Ok(None)
        }
    }
}

fn sanitized_host_code(value: &str) -> String {
    if !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        value.to_owned()
    } else {
        "plugin_job_failed".to_owned()
    }
}

fn encode_artifact(
    job: &PluginJob,
    artifact: PluginArtifact,
) -> Result<oll::StoredPluginArtifact, Status> {
    if artifact.job_id != job.job_id || artifact.plugin_id != job.payload.plugin_id {
        return Err(corrupt_plugin_state(
            "plugin artifact ownership differs from its job",
        ));
    }
    let published_path = artifact
        .destination
        .to_str()
        .ok_or_else(|| corrupt_plugin_state("plugin artifact path is not UTF-8"))?
        .to_owned();
    Ok(oll::StoredPluginArtifact {
        artifact_id: Some(protocol::encode_plugin_artifact_id(artifact.artifact_id)),
        file_name: artifact.file_name,
        media_type: artifact.media_type,
        size_bytes: artifact.size_bytes,
        sha256: artifact.sha256.to_vec(),
        published_path,
    })
}
