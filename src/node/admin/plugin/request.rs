use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

use time::OffsetDateTime;
use tonic::Status;

use crate::{
    plugin::{
        DesiredPluginState, PluginJobId, PluginOperationId, PluginSelector,
        package::{
            DeclarationLocation, DeclarationMode, ExplicitInstallRequest, GitSelection,
            OverwriteAuthorization, PluginDeclaration,
        },
        protocol,
    },
    protocol::oll::{self, reconcile_plugin_installations_request},
};

use super::plugin_status;

#[derive(Debug)]
pub(in crate::node::admin) enum DecodedReconcileOperation {
    InstallDeclared,
    Install(ExplicitInstallRequest),
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
        Some(Operation::Install(request)) => {
            decode_install(request).map(DecodedReconcileOperation::Install)
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

fn decode_install(request: oll::InstallPlugin) -> Result<ExplicitInstallRequest, Status> {
    let mode =
        protocol::decode_install_mode(request.mode, "install.mode").map_err(plugin_status)?;
    let declaration_mode = match mode {
        crate::plugin::InstallMode::Source => DeclarationMode::Source,
        crate::plugin::InstallMode::Release => DeclarationMode::Release,
    };
    match (&declaration_mode, request.release_id.as_deref()) {
        (DeclarationMode::Source, Some(_)) => {
            return Err(Status::invalid_argument(
                "install.release_id is forbidden in source mode",
            ));
        }
        (DeclarationMode::Release, Some("")) => {
            return Err(Status::invalid_argument(
                "install.release_id must not be empty",
            ));
        }
        _ => {}
    }
    let location = match request.location {
        Some(oll::install_plugin::Location::Remote(remote)) => {
            let selection = decode_git_selection(request.selection)?;
            DeclarationLocation::Git { remote, selection }
        }
        Some(oll::install_plugin::Location::LocalPath(path)) => {
            if request.selection.is_some() {
                return Err(Status::invalid_argument(
                    "install.selection is forbidden for a local path",
                ));
            }
            DeclarationLocation::Local {
                path: decode_native_path(path, "install.local_path")?,
            }
        }
        None => {
            return Err(Status::invalid_argument("install.location is required"));
        }
    };
    let declaration = PluginDeclaration {
        location,
        mode: declaration_mode,
        release: request.release_id,
    };
    declaration
        .validate()
        .map_err(|_| Status::invalid_argument("install declaration is invalid"))?;
    let overwrite = request
        .overwrite
        .map(|overwrite| {
            let plugin_id = protocol::decode_plugin_id(
                overwrite.plugin_id.as_ref(),
                "install.overwrite.plugin_id",
            )
            .map_err(plugin_status)?;
            let expected_declaration_sha256 = overwrite
                .expected_declaration_sha256
                .as_slice()
                .try_into()
                .map_err(|_| {
                    Status::invalid_argument(
                        "install.overwrite.expected_declaration_sha256 must be exactly 32 bytes",
                    )
                })?;
            Ok::<OverwriteAuthorization, Status>(OverwriteAuthorization {
                plugin_id,
                expected_declaration_sha256,
            })
        })
        .transpose()?;
    Ok(ExplicitInstallRequest {
        declaration,
        overwrite,
    })
}

fn decode_git_selection(
    selection: Option<oll::PluginGitSelection>,
) -> Result<GitSelection, Status> {
    match selection {
        None => Ok(GitSelection::Default),
        Some(selection) => match selection.selection {
            Some(oll::plugin_git_selection::Selection::Branch(value)) if !value.is_empty() => {
                Ok(GitSelection::Branch(value))
            }
            Some(oll::plugin_git_selection::Selection::Revision(value)) if !value.is_empty() => {
                Ok(GitSelection::Revision(value))
            }
            Some(_) => Err(Status::invalid_argument(
                "install branch or revision must not be empty",
            )),
            None => Err(Status::invalid_argument(
                "install.selection must select a branch or revision",
            )),
        },
    }
}

fn decode_native_path(value: oll::NativePath, field: &str) -> Result<PathBuf, Status> {
    if value.unix_path.is_empty() || value.unix_path.contains(&0) {
        return Err(Status::invalid_argument(format!(
            "{field} must be a non-empty native path without NUL bytes"
        )));
    }
    let path = PathBuf::from(OsString::from_vec(value.unix_path));
    if !path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "{field} must be absolute"
        )));
    }
    Ok(path)
}
