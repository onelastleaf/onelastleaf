use time::OffsetDateTime;
use tonic::Status;

use crate::{
    cli::GitRemote,
    plugin::{
        DesiredPluginState, PluginJobId, PluginOperationId, PluginSelector,
        package::{
            DeclarationMode, GitSelection, InstallRemoteRequest, OverwriteAuthorization,
            PluginDeclaration,
        },
        protocol,
    },
    protocol::oll::{self, reconcile_plugin_installations_request},
};

use super::plugin_status;

#[derive(Debug)]
pub(in crate::node::admin) enum DecodedReconcileOperation {
    InstallDeclared,
    InstallRemote(InstallRemoteRequest),
    Update(PluginSelector),
    Exact,
}

#[derive(Debug)]
pub(in crate::node::admin) struct DecodedStartPluginJob {
    pub(in crate::node::admin) operation_id: PluginOperationId,
    pub(in crate::node::admin) plugin: PluginSelector,
    pub(in crate::node::admin) action: String,
    pub(in crate::node::admin) arguments: Vec<String>,
    pub(in crate::node::admin) deadline: Option<OffsetDateTime>,
}

pub(in crate::node::admin) fn decode_reconcile_operation(
    operation: Option<reconcile_plugin_installations_request::Operation>,
) -> Result<DecodedReconcileOperation, Status> {
    use reconcile_plugin_installations_request::Operation;

    match operation {
        Some(Operation::InstallDeclared(_)) => Ok(DecodedReconcileOperation::InstallDeclared),
        Some(Operation::InstallRemote(request)) => {
            decode_install_remote(request).map(DecodedReconcileOperation::InstallRemote)
        }
        Some(Operation::Update(request)) => {
            decode_selector(request.plugin.as_ref()).map(DecodedReconcileOperation::Update)
        }
        Some(Operation::ExactReconciliation(_)) => Ok(DecodedReconcileOperation::Exact),
        None => Err(Status::invalid_argument(
            "plugin reconciliation operation is required",
        )),
    }
}

pub(in crate::node::admin) fn decode_selector(
    value: Option<&oll::PluginSelector>,
) -> Result<PluginSelector, Status> {
    protocol::decode_plugin_selector(value, "plugin").map_err(plugin_status)
}

pub(in crate::node::admin) fn decode_desired_state(
    value: i32,
) -> Result<DesiredPluginState, Status> {
    protocol::decode_desired_state(value, "desired_state").map_err(plugin_status)
}

pub(in crate::node::admin) fn decode_job_id(
    value: Option<&oll::PluginJobId>,
) -> Result<PluginJobId, Status> {
    protocol::decode_plugin_job_id(value, "job_id").map_err(plugin_status)
}

pub(in crate::node::admin) fn decode_job_limit(value: u32) -> Result<usize, Status> {
    if !(1..=1000).contains(&value) {
        return Err(Status::invalid_argument(
            "plugin job list limit must be in 1..=1000",
        ));
    }
    usize::try_from(value).map_err(|_| Status::invalid_argument("plugin job limit is too large"))
}

pub(in crate::node::admin) fn decode_start_plugin_job(
    request: oll::StartPluginJobRequest,
) -> Result<DecodedStartPluginJob, Status> {
    let operation_id = protocol::decode_plugin_operation_id(&request.operation_id, "operation_id")
        .map_err(plugin_status)?;
    let plugin = decode_selector(request.plugin.as_ref())?;
    if request.action.is_empty() {
        return Err(Status::invalid_argument("plugin action must not be empty"));
    }
    let deadline = protocol::decode_optional_timestamp(request.deadline.as_ref(), "deadline")
        .map_err(plugin_status)?;
    Ok(DecodedStartPluginJob {
        operation_id,
        plugin,
        action: request.action,
        arguments: request.arguments,
        deadline,
    })
}

fn decode_install_remote(
    request: oll::InstallRemotePlugin,
) -> Result<InstallRemoteRequest, Status> {
    request
        .remote
        .parse::<GitRemote>()
        .map_err(|_| Status::invalid_argument("install_remote.remote is invalid"))?;
    let mode = protocol::decode_install_mode(request.mode, "install_remote.mode")
        .map_err(plugin_status)?;
    let selection = match request.selection {
        None => GitSelection::Default,
        Some(selection) => match selection.selection {
            Some(oll::plugin_git_selection::Selection::Branch(value)) if !value.is_empty() => {
                GitSelection::Branch(value)
            }
            Some(oll::plugin_git_selection::Selection::Revision(value)) if !value.is_empty() => {
                GitSelection::Revision(value)
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "install_remote branch or revision must not be empty",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "install_remote.selection must select a branch or revision",
                ));
            }
        },
    };
    let declaration_mode = match mode {
        crate::plugin::InstallMode::Source => DeclarationMode::Source,
        crate::plugin::InstallMode::Release => DeclarationMode::Release,
    };
    match (&declaration_mode, request.release_id.as_deref()) {
        (DeclarationMode::Source, Some(_)) => {
            return Err(Status::invalid_argument(
                "install_remote.release_id is forbidden in source mode",
            ));
        }
        (DeclarationMode::Release, None | Some("")) => {
            return Err(Status::invalid_argument(
                "install_remote.release_id is required in release mode",
            ));
        }
        _ => {}
    }
    let declaration = PluginDeclaration {
        remote: request.remote,
        mode: declaration_mode,
        selection,
        release: request.release_id,
    };
    declaration
        .validate()
        .map_err(|_| Status::invalid_argument("install_remote declaration is invalid"))?;
    let overwrite = request
        .overwrite
        .map(|overwrite| {
            let plugin_id = protocol::decode_plugin_id(
                overwrite.plugin_id.as_ref(),
                "install_remote.overwrite.plugin_id",
            )
            .map_err(plugin_status)?;
            let expected_declaration_sha256 = overwrite
                .expected_declaration_sha256
                .as_slice()
                .try_into()
                .map_err(|_| {
                    Status::invalid_argument(
                        "install_remote.overwrite.expected_declaration_sha256 must be exactly 32 bytes",
                    )
                })?;
            Ok::<OverwriteAuthorization, Status>(OverwriteAuthorization {
                plugin_id,
                expected_declaration_sha256,
            })
        })
        .transpose()?;
    Ok(InstallRemoteRequest {
        declaration,
        overwrite,
    })
}
