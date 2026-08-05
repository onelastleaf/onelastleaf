#[cfg(test)]
use std::path::Path;

use prost_types::Timestamp;
use time::OffsetDateTime;

use crate::protocol::oll;

#[cfg(test)]
use super::package::PackageError;
use super::{
    DesiredPluginState, InstallMode, JobCancellationReason, JobState, PluginArtifactId,
    PluginError, PluginId, PluginInstanceId, PluginJobId, PluginName, PluginOperationId,
    PluginSelector,
};

const PROTOBUF_TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800;
const PROTOBUF_TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799;

pub(crate) fn decode_plugin_id(
    value: Option<&oll::PluginId>,
    field: &'static str,
) -> Result<PluginId, PluginError> {
    let value = value.ok_or_else(|| invalid_argument(field, "is required"))?;
    value
        .value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_id(value: &PluginId) -> oll::PluginId {
    oll::PluginId {
        value: value.to_string(),
    }
}

pub(crate) fn decode_plugin_name(
    value: Option<&oll::PluginName>,
    field: &'static str,
) -> Result<PluginName, PluginError> {
    let value = value.ok_or_else(|| invalid_argument(field, "is required"))?;
    value
        .value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_name(value: &PluginName) -> oll::PluginName {
    oll::PluginName {
        value: value.to_string(),
    }
}

pub(crate) fn decode_plugin_job_id(
    value: Option<&oll::PluginJobId>,
    field: &'static str,
) -> Result<PluginJobId, PluginError> {
    let value = value.ok_or_else(|| invalid_argument(field, "is required"))?;
    value
        .value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_job_id(value: PluginJobId) -> oll::PluginJobId {
    oll::PluginJobId {
        value: value.to_string(),
    }
}

pub(crate) fn decode_plugin_artifact_id(
    value: Option<&oll::PluginArtifactId>,
    field: &'static str,
) -> Result<PluginArtifactId, PluginError> {
    let value = value.ok_or_else(|| invalid_argument(field, "is required"))?;
    value
        .value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_artifact_id(value: PluginArtifactId) -> oll::PluginArtifactId {
    oll::PluginArtifactId {
        value: value.to_string(),
    }
}

#[cfg(test)]
pub(crate) fn decode_plugin_instance_id(
    value: &str,
    field: &'static str,
) -> Result<PluginInstanceId, PluginError> {
    value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_instance_id(value: PluginInstanceId) -> String {
    value.to_string()
}

pub(crate) fn decode_plugin_operation_id(
    value: &str,
    field: &'static str,
) -> Result<PluginOperationId, PluginError> {
    value
        .parse()
        .map_err(|reason| invalid_argument(field, reason))
}

pub(crate) fn encode_plugin_operation_id(value: &PluginOperationId) -> String {
    value.to_string()
}

pub(crate) fn decode_plugin_selector(
    value: Option<&oll::PluginSelector>,
    field: &'static str,
) -> Result<PluginSelector, PluginError> {
    use oll::plugin_selector::Selector;

    match value.and_then(|value| value.selector.as_ref()) {
        Some(Selector::PluginId(value)) => {
            decode_plugin_id(Some(value), field).map(PluginSelector::Id)
        }
        Some(Selector::PluginName(value)) => {
            decode_plugin_name(Some(value), field).map(PluginSelector::Name)
        }
        None => Err(invalid_argument(field, "must select a plugin ID or name")),
    }
}

pub(crate) fn encode_plugin_selector(value: &PluginSelector) -> oll::PluginSelector {
    use oll::plugin_selector::Selector;

    let selector = match value {
        PluginSelector::Id(value) => Selector::PluginId(encode_plugin_id(value)),
        PluginSelector::Name(value) => Selector::PluginName(encode_plugin_name(value)),
    };
    oll::PluginSelector {
        selector: Some(selector),
    }
}

pub(crate) fn decode_desired_state(
    value: i32,
    field: &'static str,
) -> Result<DesiredPluginState, PluginError> {
    match oll::PluginDesiredState::try_from(value).ok() {
        Some(oll::PluginDesiredState::Running) => Ok(DesiredPluginState::Running),
        Some(oll::PluginDesiredState::Stopped) => Ok(DesiredPluginState::Stopped),
        Some(oll::PluginDesiredState::Unspecified) | None => {
            Err(invalid_argument(field, "is unspecified or unknown"))
        }
    }
}

pub(crate) fn encode_desired_state(value: DesiredPluginState) -> i32 {
    match value {
        DesiredPluginState::Running => oll::PluginDesiredState::Running as i32,
        DesiredPluginState::Stopped => oll::PluginDesiredState::Stopped as i32,
    }
}

pub(crate) fn decode_install_mode(
    value: i32,
    field: &'static str,
) -> Result<InstallMode, PluginError> {
    match oll::PluginPackageMode::try_from(value).ok() {
        Some(oll::PluginPackageMode::Source) => Ok(InstallMode::Source),
        Some(oll::PluginPackageMode::Release) => Ok(InstallMode::Release),
        Some(oll::PluginPackageMode::Unspecified) | None => {
            Err(invalid_argument(field, "is unspecified or unknown"))
        }
    }
}

pub(crate) fn encode_install_mode(value: InstallMode) -> i32 {
    match value {
        InstallMode::Source => oll::PluginPackageMode::Source as i32,
        InstallMode::Release => oll::PluginPackageMode::Release as i32,
    }
}

#[cfg(test)]
pub(crate) fn decode_admin_job_state(
    value: i32,
    field: &'static str,
) -> Result<JobState, PluginError> {
    match oll::PluginAdminJobState::try_from(value).ok() {
        Some(oll::PluginAdminJobState::Dispatching) => Ok(JobState::Dispatching),
        Some(oll::PluginAdminJobState::Running) => Ok(JobState::Running),
        Some(oll::PluginAdminJobState::Cancelling) => Ok(JobState::Cancelling),
        Some(oll::PluginAdminJobState::Succeeded) => Ok(JobState::Succeeded),
        Some(oll::PluginAdminJobState::Failed) => Ok(JobState::Failed),
        Some(oll::PluginAdminJobState::Cancelled) => Ok(JobState::Cancelled),
        Some(oll::PluginAdminJobState::TimedOut) => Ok(JobState::TimedOut),
        Some(oll::PluginAdminJobState::Unspecified) | None => {
            Err(invalid_argument(field, "is unspecified or unknown"))
        }
    }
}

pub(crate) fn encode_admin_job_state(value: JobState) -> i32 {
    match value {
        JobState::Dispatching => oll::PluginAdminJobState::Dispatching as i32,
        JobState::Running => oll::PluginAdminJobState::Running as i32,
        JobState::Cancelling => oll::PluginAdminJobState::Cancelling as i32,
        JobState::Succeeded => oll::PluginAdminJobState::Succeeded as i32,
        JobState::Failed => oll::PluginAdminJobState::Failed as i32,
        JobState::Cancelled => oll::PluginAdminJobState::Cancelled as i32,
        JobState::TimedOut => oll::PluginAdminJobState::TimedOut as i32,
    }
}

pub(crate) fn decode_runtime_job_state(
    value: i32,
    field: &'static str,
) -> Result<JobState, PluginError> {
    match oll::JobState::try_from(value).ok() {
        Some(oll::JobState::Running) => Ok(JobState::Running),
        Some(oll::JobState::Succeeded) => Ok(JobState::Succeeded),
        Some(oll::JobState::Failed) => Ok(JobState::Failed),
        Some(oll::JobState::Unspecified) | None => {
            Err(invalid_argument(field, "is unspecified or unknown"))
        }
    }
}

#[cfg(test)]
pub(crate) fn decode_job_cancellation_reason(
    value: i32,
    field: &'static str,
) -> Result<JobCancellationReason, PluginError> {
    match oll::JobCancellationReason::try_from(value).ok() {
        Some(oll::JobCancellationReason::UserRequest) => Ok(JobCancellationReason::UserRequest),
        Some(oll::JobCancellationReason::Deadline) => Ok(JobCancellationReason::Deadline),
        Some(oll::JobCancellationReason::Unspecified) | None => {
            Err(invalid_argument(field, "is unspecified or unknown"))
        }
    }
}

pub(crate) fn encode_job_cancellation_reason(value: JobCancellationReason) -> i32 {
    match value {
        JobCancellationReason::UserRequest => oll::JobCancellationReason::UserRequest as i32,
        JobCancellationReason::Deadline => oll::JobCancellationReason::Deadline as i32,
    }
}

pub(crate) fn decode_required_timestamp(
    value: Option<&Timestamp>,
    field: &'static str,
) -> Result<OffsetDateTime, PluginError> {
    decode_timestamp(
        value.ok_or_else(|| invalid_argument(field, "is required"))?,
        field,
    )
}

pub(crate) fn decode_optional_timestamp(
    value: Option<&Timestamp>,
    field: &'static str,
) -> Result<Option<OffsetDateTime>, PluginError> {
    value
        .map(|value| decode_timestamp(value, field))
        .transpose()
}

pub(crate) fn encode_timestamp(
    value: OffsetDateTime,
    field: &'static str,
) -> Result<Timestamp, PluginError> {
    let seconds = value.unix_timestamp();
    if !(PROTOBUF_TIMESTAMP_MIN_SECONDS..=PROTOBUF_TIMESTAMP_MAX_SECONDS).contains(&seconds) {
        return Err(PluginError::CorruptStore(format!(
            "{field} is outside the protobuf Timestamp range"
        )));
    }
    Ok(Timestamp {
        seconds,
        nanos: i32::try_from(value.nanosecond()).expect("nanoseconds always fit in i32"),
    })
}

fn decode_timestamp(value: &Timestamp, field: &'static str) -> Result<OffsetDateTime, PluginError> {
    if !(PROTOBUF_TIMESTAMP_MIN_SECONDS..=PROTOBUF_TIMESTAMP_MAX_SECONDS).contains(&value.seconds) {
        return Err(invalid_argument(
            field,
            "is outside the protobuf Timestamp range",
        ));
    }
    let nanos = u32::try_from(value.nanos)
        .ok()
        .filter(|nanos| *nanos < 1_000_000_000)
        .ok_or_else(|| invalid_argument(field, "has invalid nanoseconds"))?;
    OffsetDateTime::from_unix_timestamp(value.seconds)
        .and_then(|value| value.replace_nanosecond(nanos))
        .map_err(|_| invalid_argument(field, "is outside the supported time range"))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PackageDiagnosticContext<'a> {
    pub plugin_id: Option<&'a PluginId>,
    pub plugin_name: Option<&'a PluginName>,
    pub sanitized_remote: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub revision: Option<&'a str>,
    pub release_id: Option<&'a str>,
    pub target: Option<&'a str>,
}

#[cfg(test)]
pub(crate) fn encode_package_diagnostic(
    error: &PackageError,
    context: PackageDiagnosticContext<'_>,
) -> oll::PluginDiagnostic {
    oll::PluginDiagnostic {
        code: error.code().to_owned(),
        phase: error.phase().to_owned(),
        message: error.message().to_owned(),
        plugin_id: context.plugin_id.map(encode_plugin_id),
        plugin_name: context.plugin_name.map(encode_plugin_name),
        sanitized_remote: context.sanitized_remote.map(str::to_owned),
        branch: context.branch.map(str::to_owned),
        revision: context.revision.map(str::to_owned),
        release_id: context.release_id.map(str::to_owned),
        target: context.target.map(str::to_owned),
        hint: error.hint().map(str::to_owned),
        build_log_path: error.build_log_path().map(display_path),
    }
}

#[cfg(test)]
fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn invalid_argument(field: &'static str, reason: impl AsRef<str>) -> PluginError {
    PluginError::InvalidArgument(format!("{field} {}", reason.as_ref()))
}
