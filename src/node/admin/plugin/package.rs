use std::str::FromStr as _;

use sha2::{Digest as _, Sha256};
use tonic::Status;

use crate::{
    cli::GitRemote,
    plugin::{
        InstallMode, InstalledPlugin, ObservedPluginState, PluginId, PluginInspection,
        PluginJobCounts, PluginListEntry,
        package::{
            DeclarationMode, EffectiveManifest, GitSelection, PackageDiagnostic,
            PackageOperationOutcome, PackageOperationResult, PluginDeclaration, ReleaseListing,
        },
        protocol,
    },
    protocol::oll,
};

use super::{corrupt_plugin_state, plugin_status};

pub(in crate::node::admin) fn encode_installation_results(
    results: Vec<PackageOperationResult>,
) -> Result<oll::ReconcilePluginInstallationsResponse, Status> {
    Ok(oll::ReconcilePluginInstallationsResponse {
        results: results
            .into_iter()
            .map(encode_installation_result)
            .collect::<Result<_, _>>()?,
    })
}

pub(in crate::node::admin) fn encode_removal_result(
    result: PackageOperationResult,
) -> Result<oll::RemovePluginResponse, Status> {
    if result.outcome != PackageOperationOutcome::Removed
        || !result.diagnostics.is_empty()
        || result.confirmation_summary.is_some()
        || result.confirmation_digest.is_some()
    {
        return Err(corrupt_plugin_state("invalid successful removal result"));
    }
    Ok(oll::RemovePluginResponse {
        plugin_id: Some(protocol::encode_plugin_id(
            result
                .plugin_id
                .as_ref()
                .ok_or_else(|| corrupt_plugin_state("removal result has no plugin ID"))?,
        )),
        plugin_name: Some(protocol::encode_plugin_name(
            result
                .plugin_name
                .as_ref()
                .ok_or_else(|| corrupt_plugin_state("removal result has no plugin name"))?,
        )),
    })
}

pub(in crate::node::admin) fn encode_plugin_list(
    entries: Vec<PluginListEntry>,
) -> Result<oll::ListPluginsResponse, Status> {
    Ok(oll::ListPluginsResponse {
        plugins: entries
            .into_iter()
            .map(|entry| encode_summary(&entry.installed, entry.process.as_ref()))
            .collect::<Result<_, _>>()?,
    })
}

pub(in crate::node::admin) fn encode_plugin_details(
    inspection: PluginInspection,
) -> Result<oll::GetPluginResponse, Status> {
    let declaration = decode_stored_declaration(&inspection.installed)?;
    let effective = decode_stored_manifest(&inspection.installed)?;
    let summary = encode_summary(&inspection.installed, inspection.process.as_ref())?;
    let process_instance = inspection
        .process
        .as_ref()
        .map(encode_process_instance)
        .transpose()?;
    let package_state = encode_package_state(&inspection);
    let restart_state = encode_restart_state(&inspection.installed)?;
    let job_counts = encode_job_counts(inspection.job_counts);

    Ok(oll::GetPluginResponse {
        plugin: Some(oll::PluginDetails {
            summary: Some(summary),
            declaration: Some(declaration),
            effective_manifest: Some(effective),
            package_state: Some(package_state),
            restart_state: Some(restart_state),
            process_instance,
            job_counts: Some(job_counts),
        }),
    })
}

pub(in crate::node::admin) fn encode_plugin_releases(
    plugin_id: PluginId,
    releases: Vec<ReleaseListing>,
) -> oll::ListPluginReleasesResponse {
    oll::ListPluginReleasesResponse {
        plugin_id: Some(protocol::encode_plugin_id(&plugin_id)),
        releases: releases
            .into_iter()
            .map(|release| oll::PluginRelease {
                release_id: release.release_id,
                targets: release.targets,
            })
            .collect(),
    }
}

pub(in crate::node::admin) fn encode_set_desired_state_response(
    plugin: &InstalledPlugin,
) -> oll::SetPluginDesiredStateResponse {
    oll::SetPluginDesiredStateResponse {
        plugin_id: Some(protocol::encode_plugin_id(&plugin.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(&plugin.plugin_name)),
        desired_state: protocol::encode_desired_state(plugin.desired_state),
    }
}

pub(in crate::node::admin) fn encode_restart_response(
    plugin: &InstalledPlugin,
) -> oll::RestartPluginResponse {
    oll::RestartPluginResponse {
        plugin_id: Some(protocol::encode_plugin_id(&plugin.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(&plugin.plugin_name)),
        desired_state: protocol::encode_desired_state(plugin.desired_state),
        restart_sequence: plugin.restart_sequence,
    }
}

fn encode_installation_result(
    result: PackageOperationResult,
) -> Result<oll::PluginInstallationResult, Status> {
    let confirmation = match result.outcome {
        PackageOperationOutcome::ConfirmationRequired => {
            let summary = result.confirmation_summary.ok_or_else(|| {
                corrupt_plugin_state("confirmation result has no redacted summary")
            })?;
            if summary.is_empty() {
                return Err(corrupt_plugin_state("confirmation summary is empty"));
            }
            let digest = result.confirmation_digest.ok_or_else(|| {
                corrupt_plugin_state("confirmation result has no declaration digest")
            })?;
            Some(oll::PluginOverwriteConfirmation {
                redacted_change_summary: summary,
                current_declaration_sha256: digest.to_vec(),
            })
        }
        _ if result.confirmation_summary.is_some() || result.confirmation_digest.is_some() => {
            return Err(corrupt_plugin_state(
                "non-confirmation result has confirmation data",
            ));
        }
        _ => None,
    };
    let diagnostics = result
        .diagnostics
        .iter()
        .map(|diagnostic| {
            encode_diagnostic(
                diagnostic,
                result.plugin_id.as_ref(),
                result.plugin_name.as_ref(),
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(oll::PluginInstallationResult {
        plugin_id: result.plugin_id.as_ref().map(protocol::encode_plugin_id),
        plugin_name: result
            .plugin_name
            .as_ref()
            .map(protocol::encode_plugin_name),
        outcome: encode_installation_outcome(result.outcome),
        diagnostics,
        confirmation,
    })
}

fn encode_diagnostic(
    diagnostic: &PackageDiagnostic,
    plugin_id: Option<&PluginId>,
    plugin_name: Option<&crate::plugin::PluginName>,
) -> Result<oll::PluginDiagnostic, Status> {
    if diagnostic.code.is_empty() || diagnostic.phase.is_empty() || diagnostic.message.is_empty() {
        return Err(corrupt_plugin_state("package diagnostic is incomplete"));
    }
    let sanitized_remote = diagnostic.sanitized_remote.as_deref().map(|remote| {
        GitRemote::from_str(remote)
            .map(|parsed| parsed.to_string())
            .unwrap_or_else(|_| "<invalid-remote>".to_owned())
    });
    Ok(oll::PluginDiagnostic {
        code: diagnostic.code.clone(),
        phase: diagnostic.phase.clone(),
        message: if diagnostic.phase == "store" {
            "plugin package persistence failed; inspect the correlated daemon log".to_owned()
        } else {
            diagnostic.message.clone()
        },
        plugin_id: plugin_id.map(protocol::encode_plugin_id),
        plugin_name: plugin_name.map(protocol::encode_plugin_name),
        sanitized_remote,
        branch: diagnostic.branch.clone(),
        revision: diagnostic.revision.clone(),
        release_id: diagnostic.release_id.clone(),
        target: diagnostic.target.clone(),
        hint: diagnostic.hint.clone(),
        build_log_path: diagnostic
            .build_log_path
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
    })
}

fn encode_installation_outcome(outcome: PackageOperationOutcome) -> i32 {
    match outcome {
        PackageOperationOutcome::Installed => oll::PluginInstallationOutcome::Installed as i32,
        PackageOperationOutcome::Updated => oll::PluginInstallationOutcome::Updated as i32,
        PackageOperationOutcome::Removed => oll::PluginInstallationOutcome::Removed as i32,
        PackageOperationOutcome::AlreadySatisfied => {
            oll::PluginInstallationOutcome::AlreadySatisfied as i32
        }
        PackageOperationOutcome::ConfirmationRequired => {
            oll::PluginInstallationOutcome::ConfirmationRequired as i32
        }
        PackageOperationOutcome::Failed => oll::PluginInstallationOutcome::Failed as i32,
    }
}

fn decode_stored_declaration(
    installed: &InstalledPlugin,
) -> Result<oll::PluginDeclaration, Status> {
    let actual_digest: [u8; 32] = Sha256::digest(&installed.normalized_declaration).into();
    if actual_digest != installed.declaration_sha256 {
        return Err(corrupt_plugin_state("stored declaration digest differs"));
    }
    let declaration: PluginDeclaration = serde_json::from_slice(&installed.normalized_declaration)
        .map_err(|_| corrupt_plugin_state("stored declaration is not valid JSON"))?;
    declaration
        .validate()
        .map_err(|_| corrupt_plugin_state("stored declaration is invalid"))?;
    let canonical = serde_json::to_vec(&declaration)
        .map_err(|_| corrupt_plugin_state("stored declaration cannot be normalized"))?;
    if canonical != installed.normalized_declaration
        || declaration.normalized_sha256() != installed.declaration_sha256
    {
        return Err(corrupt_plugin_state("stored declaration is not canonical"));
    }
    let mode = match declaration.mode {
        DeclarationMode::Source => InstallMode::Source,
        DeclarationMode::Release => InstallMode::Release,
    };
    if mode != installed.install_mode || declaration.release != installed.release_id {
        return Err(corrupt_plugin_state(
            "stored declaration differs from installed package metadata",
        ));
    }
    let selection = match &declaration.selection {
        GitSelection::Default => None,
        GitSelection::Branch(branch) => Some(oll::PluginGitSelection {
            selection: Some(oll::plugin_git_selection::Selection::Branch(branch.clone())),
        }),
        GitSelection::Revision(revision) => Some(oll::PluginGitSelection {
            selection: Some(oll::plugin_git_selection::Selection::Revision(
                revision.clone(),
            )),
        }),
    };
    Ok(oll::PluginDeclaration {
        sanitized_remote: declaration.sanitized_remote(),
        mode: protocol::encode_install_mode(mode),
        selection,
        release_id: declaration.release,
        normalized_sha256: installed.declaration_sha256.to_vec(),
    })
}

fn decode_stored_manifest(
    installed: &InstalledPlugin,
) -> Result<oll::PluginEffectiveManifest, Status> {
    let manifest: EffectiveManifest = serde_json::from_slice(&installed.effective_manifest)
        .map_err(|_| corrupt_plugin_state("stored effective manifest is not valid JSON"))?;
    manifest
        .validate()
        .map_err(|_| corrupt_plugin_state("stored effective manifest is invalid"))?;
    let canonical = serde_json::to_vec(&manifest)
        .map_err(|_| corrupt_plugin_state("stored effective manifest cannot be normalized"))?;
    if canonical != installed.effective_manifest
        || manifest.plugin_id != installed.plugin_id.as_str()
        || manifest.plugin_name != installed.plugin_name.as_str()
    {
        return Err(corrupt_plugin_state(
            "stored effective manifest differs from installed identity",
        ));
    }
    Ok(oll::PluginEffectiveManifest {
        format_version: 1,
        plugin_id: Some(protocol::encode_plugin_id(&installed.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(&installed.plugin_name)),
        source_dependencies: manifest
            .source
            .dependencies
            .into_iter()
            .map(|(executable, hint)| oll::PluginDependency { executable, hint })
            .collect(),
        source_steps: manifest
            .source
            .steps
            .into_iter()
            .map(|argv| oll::PluginRecipeStep { argv })
            .collect(),
        runtime_argv: manifest.runtime.argv,
        source_checkout: match manifest.source.checkout {
            crate::plugin::package::SourceCheckout::Source => {
                oll::PluginSourceCheckout::Source as i32
            }
            crate::plugin::package::SourceCheckout::Install => {
                oll::PluginSourceCheckout::Install as i32
            }
            crate::plugin::package::SourceCheckout::Generation => {
                oll::PluginSourceCheckout::Generation as i32
            }
        },
    })
}

fn encode_summary(
    installed: &InstalledPlugin,
    process: Option<&crate::plugin::runtime::PluginSessionSnapshot>,
) -> Result<oll::PluginSummary, Status> {
    if let Some(process) = process
        && (installed.running_generation != Some(process.install_generation)
            || installed.running_instance_id != Some(process.instance_id))
    {
        return Err(corrupt_plugin_state(
            "runtime process identity differs from installed state",
        ));
    }
    let process_state = process.map_or_else(
        || {
            if installed.last_lifecycle_failure.is_some() {
                oll::PluginProcessState::Failed as i32
            } else {
                oll::PluginProcessState::Exited as i32
            }
        },
        |process| encode_process_state(process.state),
    );
    Ok(oll::PluginSummary {
        plugin_id: Some(protocol::encode_plugin_id(&installed.plugin_id)),
        plugin_name: Some(protocol::encode_plugin_name(&installed.plugin_name)),
        desired_state: protocol::encode_desired_state(installed.desired_state),
        process_state,
        current_generation: Some(installed.current_generation.to_string()),
        running_generation: installed.running_generation.map(|value| value.to_string()),
        last_error: lifecycle_failure(installed),
    })
}

fn encode_process_state(state: ObservedPluginState) -> i32 {
    match state {
        ObservedPluginState::Starting => oll::PluginProcessState::Starting as i32,
        ObservedPluginState::Ready => oll::PluginProcessState::Ready as i32,
        ObservedPluginState::Stopping => oll::PluginProcessState::Stopping as i32,
        ObservedPluginState::Exited => oll::PluginProcessState::Exited as i32,
        ObservedPluginState::Failed => oll::PluginProcessState::Failed as i32,
    }
}

fn encode_process_instance(
    process: &crate::plugin::runtime::PluginSessionSnapshot,
) -> Result<oll::PluginProcessInstance, Status> {
    Ok(oll::PluginProcessInstance {
        plugin_instance_id: protocol::encode_plugin_instance_id(process.instance_id),
        install_generation: process.install_generation.to_string(),
        state: encode_process_state(process.state),
        process_id: process.process_id.unwrap_or_default(),
        started_at: process
            .started_at
            .map(|value| protocol::encode_timestamp(value, "plugin.started_at"))
            .transpose()
            .map_err(plugin_status)?,
        ready_at: process
            .ready_at
            .map(|value| protocol::encode_timestamp(value, "plugin.ready_at"))
            .transpose()
            .map_err(plugin_status)?,
    })
}

fn encode_package_state(inspection: &PluginInspection) -> oll::PluginPackageState {
    let transition_state = if inspection.removal_intent.is_some() {
        oll::PluginPackageTransitionState::Removing
    } else if inspection.package_publish_intent.is_some() {
        oll::PluginPackageTransitionState::Publishing
    } else {
        oll::PluginPackageTransitionState::Stable
    };
    oll::PluginPackageState {
        transition_state: transition_state as i32,
        selected_git_commit: inspection.installed.selected_commit.clone(),
        current_generation: Some(inspection.installed.current_generation.to_string()),
        candidate_generation: inspection
            .package_publish_intent
            .as_ref()
            .map(|intent| intent.candidate_generation.to_string()),
        spawn_blocked: inspection.package_publish_intent.is_some()
            || inspection.removal_intent.is_some(),
    }
}

fn encode_restart_state(installed: &InstalledPlugin) -> Result<oll::PluginRestartState, Status> {
    Ok(oll::PluginRestartState {
        requested_sequence: installed.restart_sequence,
        applied_sequence: installed.consumed_restart_sequence,
        consecutive_failures: installed.restart_attempt,
        next_attempt_at: installed
            .restart_not_before
            .map(|value| protocol::encode_timestamp(value, "plugin.restart_not_before"))
            .transpose()
            .map_err(plugin_status)?,
        last_failure: lifecycle_failure(installed),
    })
}

fn lifecycle_failure(installed: &InstalledPlugin) -> Option<oll::PluginDiagnostic> {
    installed
        .last_lifecycle_failure
        .as_deref()
        .map(|stored| oll::PluginDiagnostic {
            code: sanitized_failure_code(stored).to_owned(),
            phase: "runtime".to_owned(),
            message: "the previous plugin process did not complete normally".to_owned(),
            plugin_id: Some(protocol::encode_plugin_id(&installed.plugin_id)),
            plugin_name: Some(protocol::encode_plugin_name(&installed.plugin_name)),
            sanitized_remote: None,
            branch: None,
            revision: None,
            release_id: None,
            target: None,
            hint: None,
            build_log_path: None,
        })
}

fn sanitized_failure_code(stored: &str) -> &str {
    if !stored.is_empty()
        && stored.len() <= 96
        && stored
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        stored
    } else {
        "plugin_lifecycle_failed"
    }
}

fn encode_job_counts(counts: PluginJobCounts) -> oll::PluginJobCounts {
    oll::PluginJobCounts {
        dispatching: counts.dispatching,
        running: counts.running,
        cancelling: counts.cancelling,
        succeeded: counts.succeeded,
        failed: counts.failed,
        cancelled: counts.cancelled,
        timed_out: counts.timed_out,
    }
}
