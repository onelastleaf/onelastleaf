use time::OffsetDateTime;

use super::{
    super::{
        super::{
            JobCancellation, JobCancellationReason, JobState, PluginError, PluginInstanceId,
            PluginJob, PluginJobId,
        },
        PluginStore,
        convert::timestamp_parts,
        store_error,
    },
    record::parse_job,
};

impl PluginStore {
    pub async fn mark_job_accepted(
        &self,
        job_id: PluginJobId,
        instance_id: PluginInstanceId,
        updated_at: OffsetDateTime,
    ) -> Result<PluginJob, PluginError> {
        let (seconds, nanos) = timestamp_parts(updated_at);
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let update = sqlx::query(
            "UPDATE plugin_jobs SET state = 'running',
                 accepted_at_seconds = $1, accepted_at_nanos = $2,
                 updated_at_seconds = $1, updated_at_nanos = $2
             WHERE job_id = $3 AND plugin_instance_id = $4
               AND state = 'dispatching'",
        )
        .bind(seconds)
        .bind(nanos)
        .bind(job_id.to_string())
        .bind(instance_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        let row = sqlx::query("SELECT * FROM plugin_jobs WHERE job_id = $1")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?
            .ok_or_else(|| PluginError::NotFound(format!("plugin job {job_id} was not found")))?;
        let current = parse_job(&row)?;
        if update.rows_affected() == 0
            && current.plugin_instance_id != instance_id
            && !current.state.is_terminal()
        {
            return Err(PluginError::FailedPrecondition(
                "plugin job acceptance belongs to a stale instance".to_owned(),
            ));
        }
        transaction.commit().await.map_err(store_error)?;
        Ok(current)
    }

    pub async fn begin_job_cancellation(
        &self,
        job_id: PluginJobId,
        reason: JobCancellationReason,
        updated_at: OffsetDateTime,
    ) -> Result<JobCancellation, PluginError> {
        let (seconds, nanos) = timestamp_parts(updated_at);
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let result = sqlx::query(
            "UPDATE plugin_jobs
             SET state = 'cancelling', cancellation_reason = $1,
                 updated_at_seconds = $2, updated_at_nanos = $3
             WHERE job_id = $4 AND state IN ('dispatching', 'running')",
        )
        .bind(reason.as_str())
        .bind(seconds)
        .bind(nanos)
        .bind(job_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        let row = sqlx::query("SELECT * FROM plugin_jobs WHERE job_id = $1")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?
            .ok_or_else(|| PluginError::NotFound(format!("plugin job {job_id} was not found")))?;
        let cancellation = JobCancellation {
            job: parse_job(&row)?,
            send_request: result.rows_affected() == 1,
        };
        transaction.commit().await.map_err(store_error)?;
        Ok(cancellation)
    }

    pub async fn complete_job_cancellation(
        &self,
        job_id: PluginJobId,
        instance_id: PluginInstanceId,
        updated_at: OffsetDateTime,
    ) -> Result<PluginJob, PluginError> {
        let current = self.get_job(job_id).await?;
        if current.plugin_instance_id != instance_id && !current.state.is_terminal() {
            return Err(PluginError::FailedPrecondition(
                "plugin cancellation acknowledgement belongs to a stale instance".to_owned(),
            ));
        }
        if current.state != JobState::Cancelling {
            return Ok(current);
        }
        let terminal = match current.cancellation_reason {
            Some(JobCancellationReason::UserRequest) => JobState::Cancelled,
            Some(JobCancellationReason::Deadline) => JobState::TimedOut,
            None => {
                return Err(PluginError::CorruptStore(
                    "cancelling plugin job has no reason".to_owned(),
                ));
            }
        };
        self.finish_job(job_id, instance_id, terminal, None, None, None, updated_at)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn finish_job(
        &self,
        job_id: PluginJobId,
        instance_id: PluginInstanceId,
        state: JobState,
        result: Option<&[u8]>,
        error_code: Option<&str>,
        error_message: Option<&str>,
        updated_at: OffsetDateTime,
    ) -> Result<PluginJob, PluginError> {
        if !state.is_terminal() {
            return Err(PluginError::InvalidArgument(
                "finish_job requires a terminal state".to_owned(),
            ));
        }
        let (seconds, nanos) = timestamp_parts(updated_at);
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let update = sqlx::query(
            "UPDATE plugin_jobs
             SET state = $1, result = $2, error_code = $3, error_message = $4,
                 terminal_at_seconds = $5, terminal_at_nanos = $6,
                 updated_at_seconds = $5, updated_at_nanos = $6
             WHERE job_id = $7 AND plugin_instance_id = $8
               AND state IN ('dispatching', 'running', 'cancelling')",
        )
        .bind(state.as_str())
        .bind(result)
        .bind(error_code)
        .bind(error_message)
        .bind(seconds)
        .bind(nanos)
        .bind(job_id.to_string())
        .bind(instance_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        let row = sqlx::query("SELECT * FROM plugin_jobs WHERE job_id = $1")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?
            .ok_or_else(|| PluginError::NotFound(format!("plugin job {job_id} was not found")))?;
        let current = parse_job(&row)?;
        if update.rows_affected() == 0
            && current.plugin_instance_id != instance_id
            && !current.state.is_terminal()
        {
            return Err(PluginError::FailedPrecondition(
                "plugin job output belongs to a stale instance".to_owned(),
            ));
        }
        transaction.commit().await.map_err(store_error)?;
        self.publish_job_terminal(job_id);
        Ok(current)
    }

    pub async fn fail_nonterminal_jobs_on_startup(
        &self,
        updated_at: OffsetDateTime,
    ) -> Result<u64, PluginError> {
        let (seconds, nanos) = timestamp_parts(updated_at);
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let jobs = sqlx::query(
            "UPDATE plugin_jobs SET state = 'failed', error_code = 'daemon_restarted',
                 terminal_at_seconds = $1, terminal_at_nanos = $2,
                 updated_at_seconds = $1, updated_at_nanos = $2
             WHERE state IN ('dispatching', 'running', 'cancelling')
             RETURNING job_id",
        )
        .bind(seconds)
        .bind(nanos)
        .fetch_all(&mut *transaction)
        .await
        .map_err(store_error)?;
        let job_ids = jobs
            .iter()
            .map(|row| {
                sqlx::Row::try_get::<String, _>(row, "job_id")
                    .map_err(store_error)?
                    .parse()
                    .map_err(PluginError::CorruptStore)
            })
            .collect::<Result<Vec<PluginJobId>, PluginError>>()?;
        sqlx::query(
            "UPDATE plugins SET running_generation = NULL, running_instance_id = NULL
             WHERE running_instance_id IS NOT NULL",
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)?;
        for job_id in &job_ids {
            self.publish_job_terminal(*job_id);
        }
        Ok(job_ids.len() as u64)
    }
}
