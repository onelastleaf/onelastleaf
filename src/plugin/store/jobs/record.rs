use sqlx::{Row, any::AnyRow};

use super::super::{
    super::{
        JobCancellationReason, JobDeadline, JobState, NormalizedJobPayload, PluginError, PluginId,
        PluginJob,
    },
    convert::{decode_arguments, optional_timestamp, parse_timestamp},
    store_error,
};

pub(super) fn parse_job(row: &AnyRow) -> Result<PluginJob, PluginError> {
    let plugin_id: PluginId = row
        .try_get::<String, _>("plugin_id")
        .map_err(store_error)?
        .parse()
        .map_err(PluginError::CorruptStore)?;
    let action: String = row.try_get("action").map_err(store_error)?;
    if action.is_empty() {
        return Err(PluginError::CorruptStore(
            "plugin job action is empty".to_owned(),
        ));
    }
    let arguments = decode_arguments(
        &row.try_get::<Vec<u8>, _>("arguments")
            .map_err(store_error)?,
    )?;
    let deadline = match row
        .try_get::<String, _>("deadline_kind")
        .map_err(store_error)?
        .as_str()
    {
        "default_24_hours" => {
            if row
                .try_get::<Option<i64>, _>("explicit_deadline_seconds")
                .map_err(store_error)?
                .is_some()
                || row
                    .try_get::<Option<i64>, _>("explicit_deadline_nanos")
                    .map_err(store_error)?
                    .is_some()
            {
                return Err(PluginError::CorruptStore(
                    "default plugin job deadline has explicit fields".to_owned(),
                ));
            }
            JobDeadline::Default24Hours
        }
        "explicit" => JobDeadline::Explicit(
            optional_timestamp(
                row.try_get("explicit_deadline_seconds")
                    .map_err(store_error)?,
                row.try_get("explicit_deadline_nanos")
                    .map_err(store_error)?,
                "plugin job explicit deadline",
            )?
            .ok_or_else(|| {
                PluginError::CorruptStore("explicit plugin job deadline is missing".to_owned())
            })?,
        ),
        _ => {
            return Err(PluginError::CorruptStore(
                "plugin job deadline kind is invalid".to_owned(),
            ));
        }
    };
    let payload = NormalizedJobPayload {
        plugin_id,
        action,
        arguments,
        deadline,
    };
    let stored_payload: Vec<u8> = row.try_get("normalized_payload").map_err(store_error)?;
    if payload.canonical_bytes() != stored_payload {
        return Err(PluginError::CorruptStore(
            "plugin job normalized payload is inconsistent".to_owned(),
        ));
    }
    let correlation_id: String = row.try_get("correlation_id").map_err(store_error)?;
    if correlation_id.is_empty() {
        return Err(PluginError::CorruptStore(
            "plugin job correlation ID is empty".to_owned(),
        ));
    }
    Ok(PluginJob {
        job_id: row
            .try_get::<String, _>("job_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        operation_id: row
            .try_get::<String, _>("operation_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        payload,
        absolute_deadline: parse_timestamp(
            row.try_get("absolute_deadline_seconds")
                .map_err(store_error)?,
            row.try_get("absolute_deadline_nanos")
                .map_err(store_error)?,
            "plugin job absolute deadline",
        )?,
        state: JobState::parse(&row.try_get::<String, _>("state").map_err(store_error)?)?,
        cancellation_reason: row
            .try_get::<Option<String>, _>("cancellation_reason")
            .map_err(store_error)?
            .map(|value| JobCancellationReason::parse(&value))
            .transpose()?,
        plugin_instance_id: row
            .try_get::<String, _>("plugin_instance_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        admitted_at: parse_timestamp(
            row.try_get("admitted_at_seconds").map_err(store_error)?,
            row.try_get("admitted_at_nanos").map_err(store_error)?,
            "plugin job admission time",
        )?,
        accepted_at: optional_timestamp(
            row.try_get("accepted_at_seconds").map_err(store_error)?,
            row.try_get("accepted_at_nanos").map_err(store_error)?,
            "plugin job acceptance time",
        )?,
        terminal_at: optional_timestamp(
            row.try_get("terminal_at_seconds").map_err(store_error)?,
            row.try_get("terminal_at_nanos").map_err(store_error)?,
            "plugin job terminal time",
        )?,
        updated_at: parse_timestamp(
            row.try_get("updated_at_seconds").map_err(store_error)?,
            row.try_get("updated_at_nanos").map_err(store_error)?,
            "plugin job update time",
        )?,
        correlation_id,
        result: row.try_get("result").map_err(store_error)?,
        error_code: row.try_get("error_code").map_err(store_error)?,
        error_message: row.try_get("error_message").map_err(store_error)?,
    })
}
