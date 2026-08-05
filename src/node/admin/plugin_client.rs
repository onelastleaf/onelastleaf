use std::path::Path;

use tonic::{Request, Response};

use crate::{
    node::runtime::NodeError,
    protocol::oll::{
        GetPluginJobRequest, GetPluginJobResponse, GetPluginRequest, GetPluginResponse,
        ListPluginJobsRequest, ListPluginJobsResponse, ListPluginReleasesRequest,
        ListPluginReleasesResponse, ListPluginsRequest, ListPluginsResponse,
        ReconcilePluginInstallationsRequest, ReconcilePluginInstallationsResponse,
        RemovePluginRequest, RemovePluginResponse, RestartPluginRequest, RestartPluginResponse,
        SetPluginDesiredStateRequest, SetPluginDesiredStateResponse, StartPluginJobRequest,
        StartPluginJobResponse, StopPluginJobRequest, StopPluginJobResponse,
    },
};

use super::{ADMIN_SHORT_CALL_DEADLINE, client};

pub async fn reconcile_plugin_installations(
    socket: &Path,
    mut request: ReconcilePluginInstallationsRequest,
    correlation_id: String,
) -> Result<ReconcilePluginInstallationsResponse, NodeError> {
    request.context = Some(client::call_context(correlation_id));
    let mut admin = client::connect(socket).await?;
    admin
        .reconcile_plugin_installations(request)
        .await
        .map(Response::into_inner)
        .map_err(client::status_error)
}

pub async fn remove_plugin(
    socket: &Path,
    mut request: RemovePluginRequest,
    correlation_id: String,
) -> Result<RemovePluginResponse, NodeError> {
    request.context = Some(client::call_context(correlation_id));
    let mut admin = client::connect(socket).await?;
    admin
        .remove_plugin(request)
        .await
        .map(Response::into_inner)
        .map_err(client::status_error)
}

pub async fn list_plugin_releases(
    socket: &Path,
    mut request: ListPluginReleasesRequest,
    correlation_id: String,
) -> Result<ListPluginReleasesResponse, NodeError> {
    request.context = Some(client::call_context(correlation_id));
    let mut admin = client::connect(socket).await?;
    admin
        .list_plugin_releases(request)
        .await
        .map(Response::into_inner)
        .map_err(client::status_error)
}

macro_rules! short_plugin_call {
    (
        $function:ident,
        $request:ty,
        $response:ty,
        $method:ident
    ) => {
        pub async fn $function(
            socket: &Path,
            mut request: $request,
            correlation_id: String,
        ) -> Result<$response, NodeError> {
            request.context = Some(client::call_context(correlation_id));
            let mut request = Request::new(request);
            request.set_timeout(ADMIN_SHORT_CALL_DEADLINE);
            let mut admin = client::connect(socket).await?;
            admin
                .$method(request)
                .await
                .map(Response::into_inner)
                .map_err(client::status_error)
        }
    };
}

short_plugin_call!(
    list_plugins,
    ListPluginsRequest,
    ListPluginsResponse,
    list_plugins
);
short_plugin_call!(get_plugin, GetPluginRequest, GetPluginResponse, get_plugin);
short_plugin_call!(
    set_plugin_desired_state,
    SetPluginDesiredStateRequest,
    SetPluginDesiredStateResponse,
    set_plugin_desired_state
);
short_plugin_call!(
    restart_plugin,
    RestartPluginRequest,
    RestartPluginResponse,
    restart_plugin
);
short_plugin_call!(
    start_plugin_job,
    StartPluginJobRequest,
    StartPluginJobResponse,
    start_plugin_job
);
short_plugin_call!(
    list_plugin_jobs,
    ListPluginJobsRequest,
    ListPluginJobsResponse,
    list_plugin_jobs
);
short_plugin_call!(
    get_plugin_job,
    GetPluginJobRequest,
    GetPluginJobResponse,
    get_plugin_job
);
short_plugin_call!(
    stop_plugin_job,
    StopPluginJobRequest,
    StopPluginJobResponse,
    stop_plugin_job
);
