use sqlx::Row;
use time::OffsetDateTime;

use super::{
    super::{
        super::{
            JobAdmission, JobDeadline, JobState, NormalizedJobPayload, PluginError, PluginId,
            PluginInstanceId, PluginJob, PluginJobCounts, PluginJobId, PluginOperationId,
        },
        PluginStore,
        convert::{encode_arguments, timestamp_parts},
        store_error,
    },
    record::parse_job,
};

impl PluginStore {
    pub async fn admit_job(
        &self,
        operation_id: &PluginOperationId,
        payload: &NormalizedJobPayload,
        plugin_instance_id: PluginInstanceId,
        admitted_at: OffsetDateTime,
        correlation_id: &str,
    ) -> Result<JobAdmission, PluginError> {
        if correlation_id.is_empty() {
            return Err(PluginError::InvalidArgument(
                "plugin job correlation ID must not be empty".to_owned(),
            ));
        }
        let absolute_deadline = payload.absolute_deadline(admitted_at);
        if absolute_deadline <= admitted_at {
            return Err(PluginError::InvalidArgument(
                "plugin job deadline must be in the future".to_owned(),
            ));
        }
        let canonical = payload.canonical_bytes();
        let job_id = PluginJobId::new();
        let (admitted_seconds, admitted_nanos) = timestamp_parts(admitted_at);
        let (deadline_seconds, deadline_nanos) = timestamp_parts(absolute_deadline);
        let (deadline_kind, explicit_seconds, explicit_nanos) = match payload.deadline {
            JobDeadline::Default24Hours => ("default_24_hours", None, None),
            JobDeadline::Explicit(value) => {
                let (seconds, nanos) = timestamp_parts(value);
                ("explicit", Some(seconds), Some(nanos))
            }
        };
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let insert = sqlx::query(
            "INSERT INTO plugin_jobs (
                job_id, operation_id, plugin_id, normalized_payload, action,
                arguments, deadline_kind, explicit_deadline_seconds,
                explicit_deadline_nanos, absolute_deadline_seconds,
                absolute_deadline_nanos, state, cancellation_reason,
                plugin_instance_id, admitted_at_seconds, admitted_at_nanos,
                accepted_at_seconds, accepted_at_nanos,
                terminal_at_seconds, terminal_at_nanos,
                updated_at_seconds, updated_at_nanos, correlation_id,
                result, error_code, error_message
             ) SELECT
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                'dispatching', NULL, $12, $13, $14,
                NULL, NULL, NULL, NULL, $13, $14, $15,
                NULL, NULL, NULL
             FROM plugins
             WHERE plugin_id = $3
               AND desired_state = 'running'
               AND running_instance_id = $12
               AND NOT EXISTS (
                   SELECT 1 FROM plugin_removal_intents WHERE plugin_id = $3
               )",
        )
        .bind(job_id.to_string())
        .bind(operation_id.as_str())
        .bind(payload.plugin_id.as_str())
        .bind(&canonical)
        .bind(&payload.action)
        .bind(encode_arguments(&payload.arguments))
        .bind(deadline_kind)
        .bind(explicit_seconds)
        .bind(explicit_nanos)
        .bind(deadline_seconds)
        .bind(deadline_nanos)
        .bind(plugin_instance_id.to_string())
        .bind(admitted_seconds)
        .bind(admitted_nanos)
        .bind(correlation_id)
        .execute(&mut *transaction)
        .await;
        match insert {
            Ok(result) if result.rows_affected() == 1 => {
                let row = sqlx::query("SELECT * FROM plugin_jobs WHERE job_id = $1")
                    .bind(job_id.to_string())
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                let job = parse_job(&row)?;
                transaction.commit().await.map_err(store_error)?;
                Ok(JobAdmission::Created(job))
            }
            Ok(_) => {
                let row = sqlx::query("SELECT * FROM plugin_jobs WHERE operation_id = $1")
                    .bind(operation_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(store_error)?;
                match row.as_ref().map(parse_job).transpose()? {
                    Some(existing) if existing.payload.canonical_bytes() == canonical => {
                        Ok(JobAdmission::Existing(existing))
                    }
                    Some(_) => Err(PluginError::AlreadyExists(
                        "plugin operation ID is already bound to another normalized payload"
                            .to_owned(),
                    )),
                    None => Err(PluginError::FailedPrecondition(
                        "plugin is not running or is being removed".to_owned(),
                    )),
                }
            }
            Err(error)
                if error
                    .as_database_error()
                    .is_some_and(|database| database.is_unique_violation()) =>
            {
                transaction.rollback().await.map_err(store_error)?;
                let existing = self.job_by_operation_id(operation_id).await?;
                if existing.payload.canonical_bytes() == canonical {
                    Ok(JobAdmission::Existing(existing))
                } else {
                    Err(PluginError::AlreadyExists(
                        "plugin operation ID is already bound to another normalized payload"
                            .to_owned(),
                    ))
                }
            }
            Err(error) => Err(store_error(error)),
        }
    }

    pub async fn get_job(&self, job_id: PluginJobId) -> Result<PluginJob, PluginError> {
        let row = sqlx::query("SELECT * FROM plugin_jobs WHERE job_id = $1")
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .ok_or_else(|| PluginError::NotFound(format!("plugin job {job_id} was not found")))?;
        parse_job(&row)
    }

    pub async fn job_by_operation_id(
        &self,
        operation_id: &PluginOperationId,
    ) -> Result<PluginJob, PluginError> {
        let row = sqlx::query("SELECT * FROM plugin_jobs WHERE operation_id = $1")
            .bind(operation_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .ok_or_else(|| {
                PluginError::NotFound(format!("plugin operation {} was not found", operation_id))
            })?;
        parse_job(&row)
    }

    pub async fn list_jobs(
        &self,
        plugin_id: Option<&PluginId>,
        limit: usize,
    ) -> Result<Vec<PluginJob>, PluginError> {
        let limit = i64::try_from(limit).map_err(|_| {
            PluginError::InvalidArgument("plugin job list limit is out of range".to_owned())
        })?;
        let rows = if let Some(plugin_id) = plugin_id {
            sqlx::query(
                "SELECT * FROM plugin_jobs
                 WHERE plugin_id = $1
                 ORDER BY admitted_at_seconds DESC, admitted_at_nanos DESC, job_id DESC
                 LIMIT $2",
            )
            .bind(plugin_id.as_str())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT * FROM plugin_jobs
                 ORDER BY admitted_at_seconds DESC, admitted_at_nanos DESC, job_id DESC
                 LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(store_error)?;
        rows.iter().map(parse_job).collect()
    }

    pub async fn job_counts(&self, plugin_id: &PluginId) -> Result<PluginJobCounts, PluginError> {
        let rows = sqlx::query(
            "SELECT state, COUNT(*) AS job_count
             FROM plugin_jobs
             WHERE plugin_id = $1
             GROUP BY state",
        )
        .bind(plugin_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        let mut counts = PluginJobCounts::default();
        for row in rows {
            let state = JobState::parse(&row.try_get::<String, _>("state").map_err(store_error)?)?;
            let stored = row.try_get::<i64, _>("job_count").map_err(store_error)?;
            let count = u64::try_from(stored).map_err(|_| {
                PluginError::CorruptStore("plugin job count is negative".to_owned())
            })?;
            match state {
                JobState::Dispatching => counts.dispatching = count,
                JobState::Running => counts.running = count,
                JobState::Cancelling => counts.cancelling = count,
                JobState::Succeeded => counts.succeeded = count,
                JobState::Failed => counts.failed = count,
                JobState::Cancelled => counts.cancelled = count,
                JobState::TimedOut => counts.timed_out = count,
            }
        }
        Ok(counts)
    }
}
