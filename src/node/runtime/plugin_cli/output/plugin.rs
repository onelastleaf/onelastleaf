use serde_json::{Value, json};

use crate::{node::runtime::NodeError, protocol::oll};

use super::{
    Cell, Tone, desired_state_name, details_table, diagnostic_human, diagnostic_json, encode_hex,
    optional_timestamp_json, process_state_name, required_id, required_name, table, write_human,
    write_json,
};

pub(in crate::node::runtime::plugin_cli) fn show_plugin_list(
    response: &oll::ListPluginsResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let plugins = response
        .plugins
        .iter()
        .map(summary_json)
        .collect::<Result<Vec<_>, _>>()?;
    if as_json {
        return write_json(&json!({ "plugins": plugins }));
    }

    let rows = response
        .plugins
        .iter()
        .zip(&plugins)
        .map(|(encoded, plugin)| {
            let process = plugin["process_state"].as_str().unwrap_or("unknown");
            vec![
                Cell::plain(plugin["plugin_id"].as_str().unwrap_or("")),
                Cell::plain(plugin["plugin_name"].as_str().unwrap_or("")),
                Cell::plain(plugin["desired_state"].as_str().unwrap_or("unknown")),
                Cell::toned(process, state_tone(process)),
                Cell::plain(plugin["current_generation"].as_str().unwrap_or("-")),
                Cell::plain(plugin["running_generation"].as_str().unwrap_or("-")),
                Cell::plain(
                    encoded
                        .last_error
                        .as_ref()
                        .map(diagnostic_human)
                        .unwrap_or_else(|| "-".to_owned()),
                ),
            ]
        })
        .collect::<Vec<_>>();
    write_human(&table(
        &[
            "PLUGIN ID",
            "NAME",
            "DESIRED",
            "PROCESS",
            "CURRENT",
            "RUNNING",
            "ERROR",
        ],
        &rows,
    ))
}

pub(in crate::node::runtime::plugin_cli) fn show_plugin_info(
    response: &oll::GetPluginResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let details = response.plugin.as_ref().ok_or_else(|| {
        NodeError::Internal("daemon returned plugin info without plugin details".to_owned())
    })?;
    let plugin = details_json(details)?;
    if as_json {
        return write_json(&json!({ "plugin": plugin }));
    }

    write_human(&details_table("PLUGIN DETAILS", &plugin))
}

pub(in crate::node::runtime::plugin_cli) fn show_plugin_releases(
    response: &oll::ListPluginReleasesResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let plugin_id = required_id(response.plugin_id.as_ref(), "release PluginId")?;
    let releases = response
        .releases
        .iter()
        .map(|release| {
            json!({
                "release_id": release.release_id,
                "targets": release.targets,
            })
        })
        .collect::<Vec<_>>();
    if as_json {
        return write_json(&json!({
            "plugin_id": plugin_id,
            "releases": releases,
        }));
    }
    let rows = response
        .releases
        .iter()
        .map(|release| {
            vec![
                Cell::plain(release.release_id.clone()),
                Cell::plain(release.targets.join(", ")),
            ]
        })
        .collect::<Vec<_>>();
    let mut output = format!("Plugin: {plugin_id}\n");
    output.push_str(&table(&["RELEASE", "TARGETS"], &rows));
    write_human(&output)
}

pub(in crate::node::runtime::plugin_cli) fn show_installation_results(
    response: &oll::ReconcilePluginInstallationsResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let results = response
        .results
        .iter()
        .map(installation_result_json)
        .collect::<Result<Vec<_>, _>>()?;
    if as_json {
        return write_json(&json!({ "results": results }));
    }
    let rows = results
        .iter()
        .zip(&response.results)
        .map(|(result, encoded)| {
            let outcome = result["outcome"].as_str().unwrap_or("unknown");
            let diagnostics = encoded
                .diagnostics
                .iter()
                .map(diagnostic_human)
                .collect::<Vec<_>>()
                .join("; ");
            vec![
                Cell::plain(result["plugin_id"].as_str().unwrap_or("-")),
                Cell::plain(result["plugin_name"].as_str().unwrap_or("-")),
                Cell::toned(outcome, outcome_tone(outcome)),
                Cell::plain(if diagnostics.is_empty() {
                    "-".to_owned()
                } else {
                    diagnostics
                }),
            ]
        })
        .collect::<Vec<_>>();
    write_human(&table(
        &["PLUGIN ID", "NAME", "OUTCOME", "DIAGNOSTICS"],
        &rows,
    ))
}

pub(in crate::node::runtime::plugin_cli) fn installation_result_status(
    response: &oll::ReconcilePluginInstallationsResponse,
) -> Result<(), NodeError> {
    let mut unresolved = false;
    for result in &response.results {
        let outcome = installation_outcome(result.outcome)?;
        unresolved |= matches!(
            outcome,
            oll::PluginInstallationOutcome::Failed
                | oll::PluginInstallationOutcome::ConfirmationRequired
        );
    }
    if unresolved {
        Err(NodeError::Operation(
            "one or more plugin package operations failed or remain unresolved".to_owned(),
        ))
    } else {
        Ok(())
    }
}

pub(in crate::node::runtime::plugin_cli) fn show_remove(
    response: &oll::RemovePluginResponse,
    as_json: bool,
) -> Result<(), NodeError> {
    let plugin_id = required_id(response.plugin_id.as_ref(), "removed PluginId")?;
    let plugin_name = required_name(response.plugin_name.as_ref(), "removed PluginName")?;
    if as_json {
        return write_json(&json!({
            "results": [{
                "plugin_id": plugin_id,
                "plugin_name": plugin_name,
                "outcome": "removed",
                "diagnostics": [],
            }]
        }));
    }
    write_human(&format!("Removed {plugin_name} ({plugin_id})\n"))
}

pub(in crate::node::runtime::plugin_cli) fn show_desired_state(
    response: &oll::SetPluginDesiredStateResponse,
) -> Result<(), NodeError> {
    let plugin_id = required_id(response.plugin_id.as_ref(), "updated PluginId")?;
    let plugin_name = required_name(response.plugin_name.as_ref(), "updated PluginName")?;
    let state = desired_state_name(response.desired_state)?;
    write_human(&format!("{plugin_name} ({plugin_id}): desired {state}\n"))
}

pub(in crate::node::runtime::plugin_cli) fn show_restart(
    response: &oll::RestartPluginResponse,
) -> Result<(), NodeError> {
    let plugin_id = required_id(response.plugin_id.as_ref(), "restarted PluginId")?;
    let plugin_name = required_name(response.plugin_name.as_ref(), "restarted PluginName")?;
    let state = desired_state_name(response.desired_state)?;
    write_human(&format!(
        "{plugin_name} ({plugin_id}): desired {state}, restart sequence {}\n",
        response.restart_sequence
    ))
}

pub(in crate::node::runtime::plugin_cli) fn summary_json(
    summary: &oll::PluginSummary,
) -> Result<Value, NodeError> {
    Ok(json!({
        "plugin_id": required_id(summary.plugin_id.as_ref(), "summary PluginId")?,
        "plugin_name": required_name(summary.plugin_name.as_ref(), "summary PluginName")?,
        "desired_state": desired_state_name(summary.desired_state)?,
        "process_state": process_state_name(summary.process_state)?,
        "current_generation": summary.current_generation,
        "running_generation": summary.running_generation,
        "last_error": summary.last_error.as_ref().map(diagnostic_json),
    }))
}

pub(in crate::node::runtime::plugin_cli) fn installation_result_json(
    result: &oll::PluginInstallationResult,
) -> Result<Value, NodeError> {
    let outcome = installation_outcome(result.outcome)?;
    Ok(json!({
        "plugin_id": result.plugin_id.as_ref().map(|value| value.value.as_str()),
        "plugin_name": result.plugin_name.as_ref().map(|value| value.value.as_str()),
        "outcome": outcome_name(outcome),
        "diagnostics": result.diagnostics.iter().map(diagnostic_json).collect::<Vec<_>>(),
    }))
}

fn installation_outcome(value: i32) -> Result<oll::PluginInstallationOutcome, NodeError> {
    match oll::PluginInstallationOutcome::try_from(value)
        .unwrap_or(oll::PluginInstallationOutcome::Unspecified)
    {
        oll::PluginInstallationOutcome::Unspecified => Err(NodeError::Internal(
            "daemon returned an unspecified plugin installation outcome".to_owned(),
        )),
        outcome => Ok(outcome),
    }
}

fn outcome_name(outcome: oll::PluginInstallationOutcome) -> &'static str {
    match outcome {
        oll::PluginInstallationOutcome::Installed => "installed",
        oll::PluginInstallationOutcome::Updated => "updated",
        oll::PluginInstallationOutcome::Removed => "removed",
        oll::PluginInstallationOutcome::AlreadySatisfied => "already_satisfied",
        oll::PluginInstallationOutcome::ConfirmationRequired => "confirmation_required",
        oll::PluginInstallationOutcome::Failed => "failed",
        oll::PluginInstallationOutcome::Unspecified => "unknown",
    }
}

pub(in crate::node::runtime::plugin_cli) fn details_json(
    details: &oll::PluginDetails,
) -> Result<Value, NodeError> {
    let summary = details
        .summary
        .as_ref()
        .ok_or_else(|| NodeError::Internal("daemon omitted plugin detail summary".to_owned()))?;
    let mut summary = summary_json(summary)?;
    let Value::Object(ref mut fields) = summary else {
        unreachable!("plugin summary JSON is always an object");
    };
    fields.insert(
        "declaration".to_owned(),
        declaration_json(
            details.declaration.as_ref().ok_or_else(|| {
                NodeError::Internal("daemon omitted plugin declaration".to_owned())
            })?,
        )?,
    );
    fields.insert(
        "effective_manifest".to_owned(),
        manifest_json(details.effective_manifest.as_ref().ok_or_else(|| {
            NodeError::Internal("daemon omitted effective plugin manifest".to_owned())
        })?)?,
    );
    fields.insert(
        "package_state".to_owned(),
        package_state_json(details.package_state.as_ref().ok_or_else(|| {
            NodeError::Internal("daemon omitted plugin package state".to_owned())
        })?)?,
    );
    fields.insert(
        "restart_state".to_owned(),
        restart_state_json(details.restart_state.as_ref().ok_or_else(|| {
            NodeError::Internal("daemon omitted plugin restart state".to_owned())
        })?)?,
    );
    fields.insert(
        "process_instance".to_owned(),
        details
            .process_instance
            .as_ref()
            .map(process_instance_json)
            .transpose()?
            .unwrap_or(Value::Null),
    );
    fields.insert(
        "job_counts".to_owned(),
        job_counts_json(
            details.job_counts.as_ref().ok_or_else(|| {
                NodeError::Internal("daemon omitted plugin job counts".to_owned())
            })?,
        ),
    );
    Ok(summary)
}

fn declaration_json(value: &oll::PluginDeclaration) -> Result<Value, NodeError> {
    let mode = match oll::PluginPackageMode::try_from(value.mode)
        .unwrap_or(oll::PluginPackageMode::Unspecified)
    {
        oll::PluginPackageMode::Source => "source",
        oll::PluginPackageMode::Release => "release",
        oll::PluginPackageMode::Unspecified => {
            return Err(NodeError::Internal(
                "daemon returned an unspecified plugin package mode".to_owned(),
            ));
        }
    };
    let selection = match value
        .selection
        .as_ref()
        .and_then(|value| value.selection.as_ref())
    {
        Some(oll::plugin_git_selection::Selection::Branch(value)) => {
            json!({ "kind": "branch", "value": value })
        }
        Some(oll::plugin_git_selection::Selection::Revision(value)) => {
            json!({ "kind": "revision", "value": value })
        }
        None => Value::Null,
    };
    Ok(json!({
        "sanitized_remote": value.sanitized_remote,
        "mode": mode,
        "selection": selection,
        "release_id": value.release_id,
        "normalized_sha256": encode_hex(&value.normalized_sha256),
    }))
}

fn manifest_json(value: &oll::PluginEffectiveManifest) -> Result<Value, NodeError> {
    Ok(json!({
        "format_version": value.format_version,
        "plugin_id": required_id(value.plugin_id.as_ref(), "manifest PluginId")?,
        "plugin_name": required_name(value.plugin_name.as_ref(), "manifest PluginName")?,
        "protocol_schema_sha256": encode_hex(&value.protocol_schema_sha256),
        "source_dependencies": value.source_dependencies.iter().map(|dependency| json!({
            "executable": dependency.executable,
            "hint": dependency.hint,
        })).collect::<Vec<_>>(),
        "source_steps": value.source_steps.iter().map(|step| json!({
            "argv": step.argv,
        })).collect::<Vec<_>>(),
        "runtime_argv": value.runtime_argv,
    }))
}

fn package_state_json(value: &oll::PluginPackageState) -> Result<Value, NodeError> {
    let state = match oll::PluginPackageTransitionState::try_from(value.transition_state)
        .unwrap_or(oll::PluginPackageTransitionState::Unspecified)
    {
        oll::PluginPackageTransitionState::Stable => "stable",
        oll::PluginPackageTransitionState::Publishing => "publishing",
        oll::PluginPackageTransitionState::Removing => "removing",
        oll::PluginPackageTransitionState::Recovering => "recovering",
        oll::PluginPackageTransitionState::Failed => "failed",
        oll::PluginPackageTransitionState::Unspecified => {
            return Err(NodeError::Internal(
                "daemon returned an unspecified plugin package state".to_owned(),
            ));
        }
    };
    Ok(json!({
        "transition_state": state,
        "selected_git_commit": value.selected_git_commit,
        "current_generation": value.current_generation,
        "candidate_generation": value.candidate_generation,
        "spawn_blocked": value.spawn_blocked,
    }))
}

fn restart_state_json(value: &oll::PluginRestartState) -> Result<Value, NodeError> {
    Ok(json!({
        "requested_sequence": value.requested_sequence,
        "applied_sequence": value.applied_sequence,
        "consecutive_failures": value.consecutive_failures,
        "next_attempt_at": optional_timestamp_json(value.next_attempt_at.as_ref())?,
        "last_failure": value.last_failure.as_ref().map(diagnostic_json),
    }))
}

fn process_instance_json(value: &oll::PluginProcessInstance) -> Result<Value, NodeError> {
    Ok(json!({
        "plugin_instance_id": value.plugin_instance_id,
        "install_generation": value.install_generation,
        "state": process_state_name(value.state)?,
        "process_id": value.process_id,
        "started_at": optional_timestamp_json(value.started_at.as_ref())?,
        "ready_at": optional_timestamp_json(value.ready_at.as_ref())?,
    }))
}

fn job_counts_json(value: &oll::PluginJobCounts) -> Value {
    json!({
        "dispatching": value.dispatching,
        "running": value.running,
        "cancelling": value.cancelling,
        "succeeded": value.succeeded,
        "failed": value.failed,
        "cancelled": value.cancelled,
        "timed_out": value.timed_out,
    })
}

fn outcome_tone(outcome: &str) -> Tone {
    match outcome {
        "installed" | "updated" | "removed" | "already_satisfied" => Tone::Success,
        "confirmation_required" => Tone::Warning,
        "failed" => Tone::Error,
        _ => Tone::Plain,
    }
}

fn state_tone(state: &str) -> Tone {
    match state {
        "ready" => Tone::Success,
        "starting" | "stopping" => Tone::Warning,
        "failed" => Tone::Error,
        _ => Tone::Plain,
    }
}
