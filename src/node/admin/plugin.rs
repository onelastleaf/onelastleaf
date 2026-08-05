mod job;
mod package;
mod request;

#[cfg(test)]
mod tests;

use tonic::Status;

use crate::plugin::PluginError;

pub(super) use job::{
    encode_job_details, encode_job_list, encode_job_state_response, encode_start_job_response,
};
pub(super) use package::{
    encode_installation_results, encode_plugin_details, encode_plugin_list, encode_plugin_releases,
    encode_removal_result, encode_restart_response, encode_set_desired_state_response,
};
pub(super) use request::{
    DecodedReconcileOperation, decode_desired_state, decode_job_id, decode_job_limit,
    decode_reconcile_operation, decode_selector, decode_start_plugin_job,
};

pub(super) fn plugin_status(error: PluginError) -> Status {
    match error {
        PluginError::InvalidArgument(message) => Status::invalid_argument(message),
        PluginError::NotFound(message) => Status::not_found(message),
        PluginError::AlreadyExists(message) => Status::already_exists(message),
        PluginError::Aborted(message) => Status::aborted(message),
        PluginError::FailedPrecondition(message) => Status::failed_precondition(message),
        PluginError::CorruptStore(_) | PluginError::Store(_) | PluginError::Io { .. } => {
            Status::internal("plugin operation failed; inspect the correlated daemon log")
        }
    }
}

pub(super) fn corrupt_plugin_state(problem: &'static str) -> Status {
    plugin_status(PluginError::CorruptStore(problem.to_owned()))
}
