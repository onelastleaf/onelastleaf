mod local;
mod output;
mod progress;

#[cfg(test)]
mod tests;

use std::{
    io::{self, BufRead, Write},
    path::Path,
};

use uuid::Uuid;

use crate::{
    cli::{
        ClientDependency, GitSelector, JobIntent, PluginInstallIntent, PluginInstallMode,
        PluginIntent,
    },
    node::{admin, lock::admin_socket_path, logging::new_correlation_id},
    plugin::{
        PluginJobId as DomainJobId, PluginSelector as DomainPluginSelector,
        protocol as plugin_protocol,
    },
    protocol::oll::{self, reconcile_plugin_installations_request},
};

use super::{NodeError, blocking::in_runtime};
use progress::CommandProgress;

pub(super) fn execute_plugin(
    intent: PluginIntent,
    dependency: ClientDependency,
) -> Result<(), NodeError> {
    match (intent, dependency) {
        (PluginIntent::Validate, ClientDependency::ConfigRoot(config_root)) => {
            local::validate_package_configuration(&config_root)
        }
        (PluginIntent::ViewLog { target }, ClientDependency::LogDir(log_dir)) => {
            local::show_plugin_log(&log_dir.join("plugin.log"), target)
        }
        (intent, ClientDependency::ConfigRoot(config_root)) => {
            in_runtime(execute_admin_plugin(intent, &config_root))
        }
        _ => Err(NodeError::Internal(
            "plugin command was prepared with an invalid dependency".to_owned(),
        )),
    }
}

pub(super) fn execute_job(intent: JobIntent, config_root: &Path) -> Result<(), NodeError> {
    in_runtime(execute_admin_job(intent, config_root))
}

async fn execute_admin_plugin(intent: PluginIntent, config_root: &Path) -> Result<(), NodeError> {
    match intent {
        PluginIntent::Install(install) => install_plugins(config_root, install).await,
        PluginIntent::Reconcile { json } => {
            let request = reconcile_request(
                reconcile_plugin_installations_request::Operation::ExactReconciliation(
                    oll::ExactPluginReconciliation {},
                ),
            );
            reconcile_and_render(config_root, request, json, "Reconciling plugins").await
        }
        PluginIntent::List { json } => {
            let response = admin::list_plugins(
                &admin_socket_path(config_root),
                oll::ListPluginsRequest { context: None },
                new_correlation_id(),
            )
            .await?;
            output::show_plugin_list(&response, json)
        }
        PluginIntent::Info { selector, json } => {
            let response = admin::get_plugin(
                &admin_socket_path(config_root),
                oll::GetPluginRequest {
                    context: None,
                    plugin: Some(parse_plugin_selector(&selector)?),
                },
                new_correlation_id(),
            )
            .await?;
            output::show_plugin_info(&response, json)
        }
        PluginIntent::Releases { selector, json } => {
            let response = {
                let _progress = CommandProgress::start("Loading plugin releases", !json);
                admin::list_plugin_releases(
                    &admin_socket_path(config_root),
                    oll::ListPluginReleasesRequest {
                        context: None,
                        plugin: Some(parse_plugin_selector(&selector)?),
                    },
                    new_correlation_id(),
                )
                .await?
            };
            output::show_plugin_releases(&response, json)
        }
        PluginIntent::Start { selector } => {
            set_desired_state(config_root, &selector, oll::PluginDesiredState::Running).await
        }
        PluginIntent::Stop { selector } => {
            set_desired_state(config_root, &selector, oll::PluginDesiredState::Stopped).await
        }
        PluginIntent::Restart { selector } => {
            let response = admin::restart_plugin(
                &admin_socket_path(config_root),
                oll::RestartPluginRequest {
                    context: None,
                    plugin: Some(parse_plugin_selector(&selector)?),
                },
                new_correlation_id(),
            )
            .await?;
            output::show_restart(&response)
        }
        PluginIntent::Update { selector, json } => {
            let request =
                reconcile_request(reconcile_plugin_installations_request::Operation::Update(
                    oll::UpdatePluginInstallation {
                        plugin: Some(parse_plugin_selector(&selector)?),
                    },
                ));
            reconcile_and_render(config_root, request, json, "Updating plugin").await
        }
        PluginIntent::Remove { selector, json } => {
            let response = {
                let _progress = CommandProgress::start("Removing plugin", !json);
                admin::remove_plugin(
                    &admin_socket_path(config_root),
                    oll::RemovePluginRequest {
                        context: None,
                        plugin: Some(parse_plugin_selector(&selector)?),
                    },
                    new_correlation_id(),
                )
                .await?
            };
            output::show_remove(&response, json)
        }
        PluginIntent::Call {
            selector,
            action,
            arguments,
            operation_id,
            json,
        } => {
            let response = admin::start_plugin_job(
                &admin_socket_path(config_root),
                oll::StartPluginJobRequest {
                    context: None,
                    operation_id: operation_id.unwrap_or_else(new_operation_id),
                    plugin: Some(parse_plugin_selector(&selector)?),
                    action,
                    arguments,
                    deadline: None,
                },
                new_correlation_id(),
            )
            .await?;
            output::show_started_job(&response, json)
        }
        PluginIntent::Validate | PluginIntent::ViewLog { .. } => Err(NodeError::Internal(
            "local plugin command reached the Admin dispatcher".to_owned(),
        )),
    }
}

async fn install_plugins(
    config_root: &Path,
    install: PluginInstallIntent,
) -> Result<(), NodeError> {
    match install {
        PluginInstallIntent::Declared { json } => {
            let request = reconcile_request(
                reconcile_plugin_installations_request::Operation::InstallDeclared(
                    oll::InstallDeclaredPlugins {},
                ),
            );
            reconcile_and_render(config_root, request, json, "Installing plugins").await
        }
        PluginInstallIntent::Remote {
            remote,
            selector,
            mode,
            json,
        } => {
            let remote = remote_install_request(remote.as_str(), selector, mode);
            install_remote_with_confirmation(config_root, remote, json).await
        }
    }
}

async fn install_remote_with_confirmation(
    config_root: &Path,
    remote: oll::InstallRemotePlugin,
    json: bool,
) -> Result<(), NodeError> {
    let correlation_id = new_correlation_id();
    let socket = admin_socket_path(config_root);
    let first = {
        let _progress = CommandProgress::start("Installing plugin", !json);
        admin::reconcile_plugin_installations(
            &socket,
            remote_reconcile_request(remote.clone()),
            correlation_id.clone(),
        )
        .await?
    };

    let Some(authorization) = overwrite_authorization(&first)? else {
        output::show_installation_results(&first, json)?;
        return output::installation_result_status(&first);
    };
    if json {
        output::show_installation_results(&first, true)?;
        return Err(NodeError::Operation(
            "plugin declaration overwrite requires confirmation".to_owned(),
        ));
    }

    if !confirm_overwrite(&authorization.summary)? {
        output::show_installation_results(&first, false)?;
        return Err(NodeError::Operation(
            "plugin declaration overwrite was not confirmed".to_owned(),
        ));
    }

    let response = {
        let _progress = CommandProgress::start("Installing plugin", true);
        admin::reconcile_plugin_installations(
            &socket,
            remote_reconcile_request(authorize_remote(remote, authorization)),
            correlation_id,
        )
        .await?
    };
    output::show_installation_results(&response, false)?;
    output::installation_result_status(&response)
}

async fn reconcile_and_render(
    config_root: &Path,
    request: oll::ReconcilePluginInstallationsRequest,
    json: bool,
    progress_label: &'static str,
) -> Result<(), NodeError> {
    let response = {
        let _progress = CommandProgress::start(progress_label, !json);
        admin::reconcile_plugin_installations(
            &admin_socket_path(config_root),
            request,
            new_correlation_id(),
        )
        .await?
    };
    output::show_installation_results(&response, json)?;
    output::installation_result_status(&response)
}

async fn set_desired_state(
    config_root: &Path,
    selector: &str,
    desired_state: oll::PluginDesiredState,
) -> Result<(), NodeError> {
    let response = admin::set_plugin_desired_state(
        &admin_socket_path(config_root),
        oll::SetPluginDesiredStateRequest {
            context: None,
            plugin: Some(parse_plugin_selector(selector)?),
            desired_state: desired_state as i32,
        },
        new_correlation_id(),
    )
    .await?;
    output::show_desired_state(&response)
}

async fn execute_admin_job(intent: JobIntent, config_root: &Path) -> Result<(), NodeError> {
    match intent {
        JobIntent::List { limit, json } => {
            let response = admin::list_plugin_jobs(
                &admin_socket_path(config_root),
                oll::ListPluginJobsRequest {
                    context: None,
                    limit: u32::from(limit),
                },
                new_correlation_id(),
            )
            .await?;
            output::show_job_list(&response, json)
        }
        JobIntent::Info { job_id, json } => {
            let response = admin::get_plugin_job(
                &admin_socket_path(config_root),
                oll::GetPluginJobRequest {
                    context: None,
                    job_id: Some(parse_job_id(&job_id)?),
                },
                new_correlation_id(),
            )
            .await?;
            output::show_job_info(&response, json)
        }
        JobIntent::Stop { job_id } => {
            let response = admin::stop_plugin_job(
                &admin_socket_path(config_root),
                oll::StopPluginJobRequest {
                    context: None,
                    job_id: Some(parse_job_id(&job_id)?),
                },
                new_correlation_id(),
            )
            .await?;
            output::show_stopped_job(&response)
        }
    }
}

fn reconcile_request(
    operation: reconcile_plugin_installations_request::Operation,
) -> oll::ReconcilePluginInstallationsRequest {
    oll::ReconcilePluginInstallationsRequest {
        context: None,
        operation: Some(operation),
    }
}

fn remote_reconcile_request(
    remote: oll::InstallRemotePlugin,
) -> oll::ReconcilePluginInstallationsRequest {
    reconcile_request(reconcile_plugin_installations_request::Operation::InstallRemote(remote))
}

fn remote_install_request(
    remote: &str,
    selector: GitSelector,
    mode: PluginInstallMode,
) -> oll::InstallRemotePlugin {
    let selection = match selector {
        GitSelector::Default => None,
        GitSelector::Revision(revision) => Some(oll::PluginGitSelection {
            selection: Some(oll::plugin_git_selection::Selection::Revision(revision)),
        }),
        GitSelector::Branch(branch) => Some(oll::PluginGitSelection {
            selection: Some(oll::plugin_git_selection::Selection::Branch(branch)),
        }),
    };
    let (mode, release_id) = match mode {
        PluginInstallMode::Source => (oll::PluginPackageMode::Source, None),
        PluginInstallMode::Release { release_id } => {
            (oll::PluginPackageMode::Release, Some(release_id))
        }
    };
    oll::InstallRemotePlugin {
        remote: remote.to_owned(),
        mode: mode as i32,
        selection,
        release_id,
        overwrite: None,
    }
}

struct OverwriteAuthorization {
    plugin_id: oll::PluginId,
    digest: Vec<u8>,
    summary: String,
}

fn overwrite_authorization(
    response: &oll::ReconcilePluginInstallationsResponse,
) -> Result<Option<OverwriteAuthorization>, NodeError> {
    let mut required = response.results.iter().filter(|result| {
        oll::PluginInstallationOutcome::try_from(result.outcome)
            .is_ok_and(|outcome| outcome == oll::PluginInstallationOutcome::ConfirmationRequired)
    });
    let Some(result) = required.next() else {
        return Ok(None);
    };
    if required.next().is_some() || response.results.len() != 1 {
        return Err(NodeError::Internal(
            "remote installation returned an invalid confirmation result set".to_owned(),
        ));
    }
    let plugin_id = result.plugin_id.clone().ok_or_else(|| {
        NodeError::Internal("overwrite confirmation omitted plugin_id".to_owned())
    })?;
    let confirmation = result.confirmation.as_ref().ok_or_else(|| {
        NodeError::Internal("overwrite confirmation omitted authorization details".to_owned())
    })?;
    if confirmation.current_declaration_sha256.len() != 32 {
        return Err(NodeError::Internal(
            "overwrite confirmation returned an invalid declaration digest".to_owned(),
        ));
    }
    Ok(Some(OverwriteAuthorization {
        plugin_id,
        digest: confirmation.current_declaration_sha256.clone(),
        summary: confirmation.redacted_change_summary.clone(),
    }))
}

fn authorize_remote(
    mut remote: oll::InstallRemotePlugin,
    authorization: OverwriteAuthorization,
) -> oll::InstallRemotePlugin {
    remote.overwrite = Some(oll::PluginOverwriteAuthorization {
        plugin_id: Some(authorization.plugin_id),
        expected_declaration_sha256: authorization.digest,
    });
    remote
}

fn confirm_overwrite(summary: &str) -> Result<bool, NodeError> {
    let stdin = io::stdin();
    let mut stdout = io::stderr().lock();
    let mut input = stdin.lock();
    confirm_overwrite_with(summary, &mut input, &mut stdout)
}

fn confirm_overwrite_with(
    summary: &str,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<bool, NodeError> {
    write!(
        output,
        "oll: {summary}\nOverwrite the existing plugin declaration? [y/N] "
    )
    .and_then(|()| output.flush())
    .map_err(|error| NodeError::io("write plugin overwrite confirmation", error))?;
    let mut answer = String::new();
    let count = input
        .read_line(&mut answer)
        .map_err(|error| NodeError::io("read plugin overwrite confirmation", error))?;
    Ok(count != 0 && matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"))
}

fn parse_plugin_selector(value: &str) -> Result<oll::PluginSelector, NodeError> {
    let selector = value.parse::<DomainPluginSelector>().map_err(|error| {
        NodeError::Operation(format!("invalid plugin selector `{value}`: {error}"))
    })?;
    Ok(plugin_protocol::encode_plugin_selector(&selector))
}

fn parse_job_id(value: &str) -> Result<oll::PluginJobId, NodeError> {
    let job_id = value.parse::<DomainJobId>().map_err(|error| {
        NodeError::Operation(format!("invalid plugin job ID `{value}`: {error}"))
    })?;
    Ok(plugin_protocol::encode_plugin_job_id(job_id))
}

fn new_operation_id() -> String {
    Uuid::new_v4().to_string()
}
