use std::collections::{HashMap, HashSet};

use prost::Message as _;
use time::OffsetDateTime;

use crate::{
    node::logging::LogLevel,
    plugin::{JobState, PluginArtifact, PluginId, PluginInstanceId, protocol},
    protocol::oll,
};

use super::{
    RuntimeDependencies,
    host::{plugin_error, protocol_error},
    trace::insert_trace_fields,
    value::validate_serializable_config_value,
};

pub(super) async fn handle_job_update(
    dependencies: &RuntimeDependencies,
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    update: oll::JobUpdate,
    trace: &oll::TraceContext,
) -> Result<(), oll::ProtocolError> {
    let job_id = protocol::decode_plugin_job_id(update.job_id.as_ref(), "JobUpdate.job_id")
        .map_err(plugin_error)?;
    let state = protocol::decode_runtime_job_state(update.state, "JobUpdate.state")
        .map_err(plugin_error)?;
    if update
        .progress
        .is_some_and(|progress| !progress.is_finite() || !(0.0..=1.0).contains(&progress))
    {
        return Err(protocol_error(
            oll::ErrorCode::InvalidArgument,
            "JobUpdate.progress must be finite and between zero and one",
            false,
        ));
    }

    let job = dependencies
        .store
        .get_job(job_id)
        .await
        .map_err(plugin_error)?;
    if &job.payload.plugin_id != plugin_id || job.plugin_instance_id != instance_id {
        return Err(protocol_error(
            oll::ErrorCode::FailedPrecondition,
            "JobUpdate belongs to another plugin session",
            false,
        ));
    }
    if trace.correlation_id != job.correlation_id {
        return Err(protocol_error(
            oll::ErrorCode::FailedPrecondition,
            "JobUpdate correlation context differs from the admitted job",
            false,
        ));
    }

    let finished = match state {
        JobState::Running => {
            if update.result.is_some() || update.error.is_some() || !update.artifacts.is_empty() {
                return Err(protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "a running JobUpdate cannot contain terminal output",
                    false,
                ));
            }
            if job.state.is_terminal() {
                // Cancellation or completion won the durable race. A late update is
                // harmless and cannot replace that terminal result.
                return Ok(());
            }
            None
        }
        JobState::Succeeded => {
            if update.error.is_some() {
                return Err(protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "a successful JobUpdate cannot contain an error",
                    false,
                ));
            }
            validate_artifacts(
                dependencies
                    .store
                    .artifacts_for_job(job_id)
                    .await
                    .map_err(plugin_error)?,
                &update.artifacts,
            )?;
            if let Some(result) = update.result.as_ref() {
                validate_serializable_config_value(result).map_err(plugin_error)?;
            }
            let result = update.result.as_ref().map(|value| value.encode_to_vec());
            Some(
                dependencies
                    .store
                    .finish_job(
                        job_id,
                        instance_id,
                        JobState::Succeeded,
                        result.as_deref(),
                        None,
                        None,
                        OffsetDateTime::now_utc(),
                    )
                    .await
                    .map_err(plugin_error)?,
            )
        }
        JobState::Failed => {
            if update.result.is_some() || !update.artifacts.is_empty() {
                return Err(protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "a failed JobUpdate cannot contain a result or artifacts",
                    false,
                ));
            }
            let error = update.error.as_ref().ok_or_else(|| {
                protocol_error(
                    oll::ErrorCode::InvalidArgument,
                    "a failed JobUpdate requires an error",
                    false,
                )
            })?;
            let error_code = oll::ErrorCode::try_from(error.code)
                .ok()
                .filter(|code| *code != oll::ErrorCode::Unspecified)
                .map_or_else(
                    || "plugin_error".to_owned(),
                    |code| code.as_str_name().to_owned(),
                );
            Some(
                dependencies
                    .store
                    .finish_job(
                        job_id,
                        instance_id,
                        JobState::Failed,
                        None,
                        Some(&error_code),
                        Some(&error.message),
                        OffsetDateTime::now_utc(),
                    )
                    .await
                    .map_err(plugin_error)?,
            )
        }
        JobState::Dispatching | JobState::Cancelling | JobState::Cancelled | JobState::TimedOut => {
            unreachable!("runtime protocol exposes only running/succeeded/failed")
        }
    };

    dependencies.logger.emit(
        LogLevel::Info,
        "oll::plugin::job",
        if finished.is_some() {
            "plugin_job_terminal_update"
        } else {
            "plugin_job_progress_update"
        },
        &job.correlation_id,
        job_update_log_fields(
            plugin_id,
            instance_id,
            job_id,
            state,
            finished.as_ref().map(|job| job.state),
            &update,
            trace,
        ),
    );
    Ok(())
}

/// Builds the allowlisted host lifecycle summary without serializing any
/// plugin-supplied status, result, error, or artifact payload.
pub(super) fn job_update_log_fields(
    plugin_id: &PluginId,
    instance_id: PluginInstanceId,
    job_id: crate::plugin::PluginJobId,
    reported_state: JobState,
    durable_state: Option<JobState>,
    update: &oll::JobUpdate,
    trace: &oll::TraceContext,
) -> serde_json::Value {
    let mut fields = serde_json::json!({
        "plugin_id": plugin_id.as_str(),
        "plugin_instance_id": instance_id.to_string(),
        "job_id": job_id.to_string(),
        "reported_state": reported_state.as_str(),
        "durable_state": durable_state.map(JobState::as_str),
        "progress": update.progress,
        "status_message_present": update.status_message.is_some(),
        "status_message_bytes": update.status_message.as_ref().map(|value| value.len()),
        "result_present": update.result.is_some(),
        "error_present": update.error.is_some(),
        "artifact_count": update.artifacts.len(),
    });
    insert_trace_fields(
        fields
            .as_object_mut()
            .expect("job update log fields are an object"),
        trace,
    );
    fields
}

fn validate_artifacts(
    stored: Vec<PluginArtifact>,
    descriptors: &[oll::ArtifactDescriptor],
) -> Result<(), oll::ProtocolError> {
    let stored: HashMap<_, _> = stored
        .into_iter()
        .map(|artifact| (artifact.artifact_id, artifact))
        .collect();
    let mut seen = HashSet::new();
    for descriptor in descriptors {
        let artifact_id = protocol::decode_plugin_artifact_id(
            descriptor.artifact_id.as_ref(),
            "JobUpdate.artifacts.artifact_id",
        )
        .map_err(plugin_error)?;
        if !seen.insert(artifact_id) {
            return Err(protocol_error(
                oll::ErrorCode::InvalidArgument,
                "JobUpdate contains the same artifact more than once",
                false,
            ));
        }
        let Some(artifact) = stored.get(&artifact_id) else {
            return Err(protocol_error(
                oll::ErrorCode::FailedPrecondition,
                "JobUpdate references an artifact that oll has not stored for this job",
                false,
            ));
        };
        if descriptor.file_name != artifact.file_name
            || descriptor.media_type != artifact.media_type
            || descriptor.size_bytes != artifact.size_bytes
            || descriptor.sha256.as_slice() != artifact.sha256
        {
            return Err(protocol_error(
                oll::ErrorCode::FailedPrecondition,
                "JobUpdate artifact metadata differs from the stored artifact",
                false,
            ));
        }
    }
    Ok(())
}
