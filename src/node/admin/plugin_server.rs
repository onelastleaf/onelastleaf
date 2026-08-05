use tonic::Status;

use crate::protocol::oll;

use super::{
    AdminState,
    plugin::{
        DecodedReconcileOperation, decode_desired_state, decode_job_id, decode_job_limit,
        decode_reconcile_operation, decode_selector, decode_start_plugin_job,
        encode_installation_results, encode_job_details, encode_job_list,
        encode_job_state_response, encode_plugin_details, encode_plugin_list,
        encode_plugin_releases, encode_removal_result, encode_restart_response,
        encode_set_desired_state_response, encode_start_job_response, plugin_status,
    },
};

pub(super) async fn reconcile_installations(
    state: &AdminState,
    request: oll::ReconcilePluginInstallationsRequest,
    correlation_id: &str,
) -> Result<oll::ReconcilePluginInstallationsResponse, Status> {
    let results = match decode_reconcile_operation(request.operation)? {
        DecodedReconcileOperation::InstallDeclared => {
            state.plugins.install_declared(correlation_id).await
        }
        DecodedReconcileOperation::InstallRemote(request) => {
            state.plugins.install_remote(request, correlation_id).await
        }
        DecodedReconcileOperation::Update(selector) => {
            state.plugins.update(&selector, correlation_id).await
        }
        DecodedReconcileOperation::Exact => state.plugins.reconcile_exact(correlation_id).await,
    }
    .map_err(plugin_status)?;
    encode_installation_results(results)
}

pub(super) async fn remove(
    state: &AdminState,
    request: oll::RemovePluginRequest,
    correlation_id: &str,
) -> Result<oll::RemovePluginResponse, Status> {
    let selector = decode_selector(request.plugin.as_ref())?;
    let result = state
        .plugins
        .remove(&selector, correlation_id)
        .await
        .map_err(plugin_status)?;
    encode_removal_result(result)
}

pub(super) async fn list(state: &AdminState) -> Result<oll::ListPluginsResponse, Status> {
    let plugins = state.plugins.list_plugins().await.map_err(plugin_status)?;
    encode_plugin_list(plugins)
}

pub(super) async fn get(
    state: &AdminState,
    request: oll::GetPluginRequest,
) -> Result<oll::GetPluginResponse, Status> {
    let selector = decode_selector(request.plugin.as_ref())?;
    let plugin = state
        .plugins
        .inspect_plugin(&selector)
        .await
        .map_err(plugin_status)?;
    encode_plugin_details(plugin)
}

pub(super) async fn list_releases(
    state: &AdminState,
    request: oll::ListPluginReleasesRequest,
    correlation_id: &str,
) -> Result<oll::ListPluginReleasesResponse, Status> {
    let selector = decode_selector(request.plugin.as_ref())?;
    let (plugin_id, releases) = state
        .plugins
        .list_releases(&selector, correlation_id)
        .await
        .map_err(plugin_status)?;
    Ok(encode_plugin_releases(plugin_id, releases))
}

pub(super) async fn set_desired_state(
    state: &AdminState,
    request: oll::SetPluginDesiredStateRequest,
    correlation_id: &str,
) -> Result<oll::SetPluginDesiredStateResponse, Status> {
    let selector = decode_selector(request.plugin.as_ref())?;
    let desired_state = decode_desired_state(request.desired_state)?;
    let plugin = state
        .plugins
        .set_desired_state(&selector, desired_state, correlation_id)
        .await
        .map_err(plugin_status)?;
    Ok(encode_set_desired_state_response(&plugin))
}

pub(super) async fn restart(
    state: &AdminState,
    request: oll::RestartPluginRequest,
    correlation_id: &str,
) -> Result<oll::RestartPluginResponse, Status> {
    let selector = decode_selector(request.plugin.as_ref())?;
    let plugin = state
        .plugins
        .restart(&selector, correlation_id)
        .await
        .map_err(plugin_status)?;
    Ok(encode_restart_response(&plugin))
}

pub(super) async fn start_job(
    state: &AdminState,
    request: oll::StartPluginJobRequest,
    correlation_id: &str,
) -> Result<oll::StartPluginJobResponse, Status> {
    let request = decode_start_plugin_job(request)?;
    let job = state
        .plugins
        .start_job(
            &request.plugin,
            &request.operation_id,
            request.action,
            request.arguments,
            request.deadline,
            correlation_id,
        )
        .await
        .map_err(plugin_status)?;
    Ok(encode_start_job_response(&job))
}

pub(super) async fn list_jobs(
    state: &AdminState,
    request: oll::ListPluginJobsRequest,
) -> Result<oll::ListPluginJobsResponse, Status> {
    let limit = decode_job_limit(request.limit)?;
    let jobs = state
        .plugins
        .list_jobs(limit)
        .await
        .map_err(plugin_status)?;
    encode_job_list(jobs)
}

pub(super) async fn get_job(
    state: &AdminState,
    request: oll::GetPluginJobRequest,
) -> Result<oll::GetPluginJobResponse, Status> {
    let job_id = decode_job_id(request.job_id.as_ref())?;
    let job = state
        .plugins
        .inspect_job(job_id)
        .await
        .map_err(plugin_status)?;
    encode_job_details(job)
}

pub(super) async fn stop_job(
    state: &AdminState,
    request: oll::StopPluginJobRequest,
    correlation_id: &str,
) -> Result<oll::StopPluginJobResponse, Status> {
    let job_id = decode_job_id(request.job_id.as_ref())?;
    let job = state
        .plugins
        .stop_job(job_id, correlation_id)
        .await
        .map_err(plugin_status)?;
    Ok(encode_job_state_response(&job))
}
