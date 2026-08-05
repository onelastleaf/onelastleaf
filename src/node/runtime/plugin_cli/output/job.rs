use serde_json::{Value, json};

use crate::{node::runtime::NodeError, protocol::oll};

use super::{
    Cell, Tone, config_value_json, details_table, encode_hex, job_state_name,
    optional_timestamp_json, protocol_error_json, required_id, required_name, table,
    timestamp_json, write_human, write_json,
};

pub(in crate::node::runtime::plugin_cli) fn show_job_list(
    response: &oll::ListPluginJobsResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let jobs = response
        .jobs
        .iter()
        .map(summary_json)
        .collect::<Result<Vec<_>, _>>()?;
    if as_json {
        return write_json(&json!({ "jobs": jobs }));
    }
    let rows = jobs
        .iter()
        .map(|job| {
            let state = job["state"].as_str().unwrap_or("unknown");
            vec![
                Cell::plain(job["job_id"].as_str().unwrap_or("")),
                Cell::plain(job["plugin_name"].as_str().unwrap_or("")),
                Cell::plain(job["action"].as_str().unwrap_or("")),
                Cell::toned(state, state_tone(state)),
                Cell::plain(job["updated_at"].as_str().unwrap_or("")),
            ]
        })
        .collect::<Vec<_>>();
    write_human(&table(
        &["JOB ID", "PLUGIN", "ACTION", "STATE", "UPDATED"],
        &rows,
    ))
}

pub(in crate::node::runtime::plugin_cli) fn show_job_info(
    response: &oll::GetPluginJobResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let details = response.job.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned job info without job details".to_owned())
    })?;
    let job = details_json(details)?;
    if as_json {
        return write_json(&json!({ "job": job }));
    }
    write_human(&details_table("JOB DETAILS", &job))
}

pub(in crate::node::runtime::plugin_cli) fn show_started_job(
    response: &oll::StartPluginJobResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let job_id = response
        .job_id
        .as_ref()
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal("daemon omitted admitted PluginJobId".to_owned()))?;
    let state = job_state_name(response.state)?;
    if as_json {
        return write_json(&json!({
            "job_id": job_id,
            "state": state,
        }));
    }
    write_human(&format!("Job {job_id}: {state}\n"))
}

pub(in crate::node::runtime::plugin_cli) fn show_stopped_job(
    response: &oll::StopPluginJobResponse,
) -> Result<(), NodeError> {
    let job_id = response
        .job_id
        .as_ref()
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal("daemon omitted cancelled PluginJobId".to_owned()))?;
    let state = job_state_name(response.state)?;
    write_human(&format!("Job {job_id}: {state}\n"))
}

pub(super) fn summary_json(summary: &oll::PluginJobSummary) -> Result<Value, NodeError> {
    let job_id = summary
        .job_id
        .as_ref()
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal("daemon omitted job summary PluginJobId".to_owned()))?;
    Ok(json!({
        "job_id": job_id,
        "plugin_id": required_id(summary.plugin_id.as_ref(), "job summary PluginId")?,
        "plugin_name": required_name(summary.plugin_name.as_ref(), "job summary PluginName")?,
        "operation_id": summary.operation_id,
        "action": summary.action,
        "state": job_state_name(summary.state)?,
        "created_at": timestamp_json(summary.created_at.as_ref(), "job created_at")?,
        "updated_at": timestamp_json(summary.updated_at.as_ref(), "job updated_at")?,
    }))
}

pub(in crate::node::runtime::plugin_cli) fn details_json(
    details: &oll::PluginJobDetails,
) -> Result<Value, NodeError> {
    let summary = details
        .summary
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted job detail summary".to_owned()))?;
    let mut summary = summary_json(summary)?;
    let Value::Object(ref mut fields) = summary else {
        unreachable!("job summary JSON is always an object");
    };
    fields.insert("arguments".to_owned(), json!(details.arguments));
    fields.insert(
        "deadline".to_owned(),
        optional_timestamp_json(details.deadline.as_ref())?,
    );
    fields.insert(
        "accepted_at".to_owned(),
        optional_timestamp_json(details.accepted_at.as_ref())?,
    );
    fields.insert(
        "terminal_at".to_owned(),
        optional_timestamp_json(details.terminal_at.as_ref())?,
    );
    fields.insert(
        "result".to_owned(),
        details
            .result
            .as_ref()
            .map(config_value_json)
            .transpose()?
            .unwrap_or(Value::Null),
    );
    fields.insert(
        "error".to_owned(),
        details
            .error
            .as_ref()
            .map(protocol_error_json)
            .unwrap_or(Value::Null),
    );
    fields.insert(
        "artifacts".to_owned(),
        Value::Array(
            details
                .artifacts
                .iter()
                .map(artifact_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    );
    Ok(summary)
}

fn artifact_json(artifact: &oll::StoredPluginArtifact) -> Result<Value, NodeError> {
    let artifact_id = artifact
        .artifact_id
        .as_ref()
        .map(|value| value.value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| NodeError::Internal("daemon omitted plugin artifact ID".to_owned()))?;
    if artifact.sha256.len() != 32 {
        return Err(NodeError::Internal(
            "daemon returned a plugin artifact with an invalid SHA-256".to_owned(),
        ));
    }
    Ok(json!({
        "artifact_id": artifact_id,
        "file_name": artifact.file_name,
        "media_type": artifact.media_type,
        "size_bytes": artifact.size_bytes,
        "sha256": encode_hex(&artifact.sha256),
        "published_path": artifact.published_path,
    }))
}

fn state_tone(state: &str) -> Tone {
    match state {
        "succeeded" => Tone::Success,
        "dispatching" | "running" | "cancelling" => Tone::Warning,
        "failed" | "cancelled" | "timed_out" => Tone::Error,
        _ => Tone::Plain,
    }
}
