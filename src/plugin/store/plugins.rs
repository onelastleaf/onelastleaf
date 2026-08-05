use sqlx::{Row, any::AnyRow};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    super::{
        DesiredPluginState, InstallMode, InstalledPlugin, PluginError, PluginInstanceId,
        PluginSelector,
    },
    PluginStore,
    convert::{hash_array, optional_timestamp, parse_uuid, timestamp_parts},
    store_error,
};

impl PluginStore {
    pub async fn get_plugin(
        &self,
        selector: &PluginSelector,
    ) -> Result<InstalledPlugin, PluginError> {
        let row = match selector {
            PluginSelector::Id(id) => {
                sqlx::query("SELECT * FROM plugins WHERE plugin_id = $1")
                    .bind(id.as_str())
                    .fetch_optional(&self.pool)
                    .await
            }
            PluginSelector::Name(name) => {
                sqlx::query("SELECT * FROM plugins WHERE plugin_name = $1")
                    .bind(name.as_str())
                    .fetch_optional(&self.pool)
                    .await
            }
        }
        .map_err(store_error)?
        .ok_or_else(|| PluginError::NotFound(format!("plugin {selector} is not installed")))?;
        parse_plugin(&row)
    }

    pub async fn list_plugins(&self) -> Result<Vec<InstalledPlugin>, PluginError> {
        sqlx::query("SELECT * FROM plugins ORDER BY plugin_id")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?
            .iter()
            .map(parse_plugin)
            .collect()
    }

    pub async fn set_desired_state(
        &self,
        plugin_id: &super::super::PluginId,
        desired_state: DesiredPluginState,
    ) -> Result<InstalledPlugin, PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let result = sqlx::query("UPDATE plugins SET desired_state = $1 WHERE plugin_id = $2")
            .bind(desired_state.as_str())
            .bind(plugin_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::NotFound(format!(
                "plugin {plugin_id} is not installed"
            )));
        }
        let row = sqlx::query("SELECT * FROM plugins WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        let plugin = parse_plugin(&row)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(plugin)
    }

    pub async fn request_restart(
        &self,
        plugin_id: &super::super::PluginId,
    ) -> Result<InstalledPlugin, PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let result = sqlx::query(
            "UPDATE plugins
             SET desired_state = 'running', restart_sequence = restart_sequence + 1
             WHERE plugin_id = $1",
        )
        .bind(plugin_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::NotFound(format!(
                "plugin {plugin_id} is not installed"
            )));
        }
        let row = sqlx::query("SELECT * FROM plugins WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        let plugin = parse_plugin(&row)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(plugin)
    }

    pub async fn record_running_instance(
        &self,
        plugin_id: &super::super::PluginId,
        generation: Uuid,
        instance_id: PluginInstanceId,
    ) -> Result<(), PluginError> {
        let result = sqlx::query(
            "UPDATE plugins
             SET running_generation = $1, running_instance_id = $2,
                 consumed_restart_sequence = restart_sequence
             WHERE plugin_id = $3 AND current_generation = $1
               AND desired_state = 'running' AND running_instance_id IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM plugin_package_publish_intents
                   WHERE plugin_id = $3
               )
               AND NOT EXISTS (
                   SELECT 1 FROM plugin_removal_intents
                   WHERE plugin_id = $3
               )",
        )
        .bind(generation.to_string())
        .bind(instance_id.to_string())
        .bind(plugin_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::FailedPrecondition(
                "plugin already has a running instance or is not installed".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn record_instance_ready(
        &self,
        plugin_id: &super::super::PluginId,
        instance_id: PluginInstanceId,
    ) -> Result<(), PluginError> {
        let result = sqlx::query(
            "UPDATE plugins
             SET restart_attempt = 0, restart_not_before_seconds = NULL,
                 restart_not_before_nanos = NULL, last_lifecycle_failure = NULL
             WHERE plugin_id = $1 AND running_instance_id = $2",
        )
        .bind(plugin_id.as_str())
        .bind(instance_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::FailedPrecondition(
                "plugin ready transition belongs to a stale instance".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn settle_ended_instance(
        &self,
        plugin_id: &super::super::PluginId,
        instance_id: PluginInstanceId,
        lifecycle_failure: Option<&str>,
        ended_at: OffsetDateTime,
        job_error_code: &str,
    ) -> Result<(), PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let instance_id = instance_id.to_string();
        let cleared = sqlx::query(
            "UPDATE plugins
             SET running_generation = NULL, running_instance_id = NULL,
                 restart_attempt = CASE WHEN $1 = 1 THEN 0 ELSE restart_attempt END,
                 restart_not_before_seconds = NULL, restart_not_before_nanos = NULL,
                 last_lifecycle_failure = $2
             WHERE plugin_id = $3 AND running_instance_id = $4",
        )
        .bind(i64::from(lifecycle_failure.is_none()))
        .bind(lifecycle_failure)
        .bind(plugin_id.as_str())
        .bind(&instance_id)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if cleared.rows_affected() == 0 {
            let running_instance = sqlx::query_scalar::<_, Option<String>>(
                "SELECT running_instance_id FROM plugins WHERE plugin_id = $1",
            )
            .bind(plugin_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?
            .ok_or_else(|| PluginError::NotFound(format!("plugin {plugin_id} is not installed")))?;
            if running_instance
                .as_deref()
                .is_some_and(|current| current != instance_id.as_str())
            {
                return Err(PluginError::FailedPrecondition(
                    "plugin exit belongs to a stale instance".to_owned(),
                ));
            }
        }

        let (seconds, nanos) = timestamp_parts(ended_at);
        let jobs = sqlx::query(
            "UPDATE plugin_jobs SET state = 'failed', error_code = $1,
                 terminal_at_seconds = $2, terminal_at_nanos = $3,
                 updated_at_seconds = $2, updated_at_nanos = $3
             WHERE plugin_instance_id = $4
               AND state IN ('dispatching', 'running', 'cancelling')
             RETURNING job_id",
        )
        .bind(job_error_code)
        .bind(seconds)
        .bind(nanos)
        .bind(instance_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(store_error)?;
        let job_ids = jobs
            .iter()
            .map(|row| {
                row.try_get::<String, _>("job_id")
                    .map_err(store_error)?
                    .parse()
                    .map_err(PluginError::CorruptStore)
            })
            .collect::<Result<Vec<super::super::PluginJobId>, PluginError>>()?;
        transaction.commit().await.map_err(store_error)?;
        for job_id in job_ids {
            self.publish_job_terminal(job_id);
        }
        Ok(())
    }

    pub async fn record_restart_backoff(
        &self,
        plugin_id: &super::super::PluginId,
        attempt: u32,
        not_before: Option<OffsetDateTime>,
    ) -> Result<(), PluginError> {
        let attempt = i64::from(attempt);
        let (seconds, nanos) = not_before.map(timestamp_parts).unzip();
        let result = sqlx::query(
            "UPDATE plugins
             SET restart_attempt = $1, restart_not_before_seconds = $2,
                 restart_not_before_nanos = $3
             WHERE plugin_id = $4",
        )
        .bind(attempt)
        .bind(seconds)
        .bind(nanos)
        .bind(plugin_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::NotFound(format!(
                "plugin {plugin_id} is not installed"
            )));
        }
        Ok(())
    }
}

pub(super) fn parse_plugin(row: &AnyRow) -> Result<InstalledPlugin, PluginError> {
    let restart_sequence = row
        .try_get::<i64, _>("restart_sequence")
        .map_err(store_error)?;
    let consumed_restart_sequence = row
        .try_get::<i64, _>("consumed_restart_sequence")
        .map_err(store_error)?;
    let restart_attempt = row
        .try_get::<i64, _>("restart_attempt")
        .map_err(store_error)?;
    Ok(InstalledPlugin {
        plugin_id: row
            .try_get::<String, _>("plugin_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        plugin_name: row
            .try_get::<String, _>("plugin_name")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        normalized_declaration: row.try_get("normalized_declaration").map_err(store_error)?,
        declaration_sha256: hash_array(
            row.try_get("declaration_sha256").map_err(store_error)?,
            "plugin declaration digest",
        )?,
        effective_manifest: row.try_get("effective_manifest").map_err(store_error)?,
        selected_commit: row.try_get("selected_commit").map_err(store_error)?,
        install_mode: InstallMode::parse(
            &row.try_get::<String, _>("install_mode")
                .map_err(store_error)?,
        )?,
        release_id: row.try_get("release_id").map_err(store_error)?,
        current_generation: parse_uuid(
            &row.try_get::<String, _>("current_generation")
                .map_err(store_error)?,
            "plugin current generation",
        )?,
        running_generation: row
            .try_get::<Option<String>, _>("running_generation")
            .map_err(store_error)?
            .map(|value| parse_uuid(&value, "plugin running generation"))
            .transpose()?,
        running_instance_id: row
            .try_get::<Option<String>, _>("running_instance_id")
            .map_err(store_error)?
            .map(|value| value.parse().map_err(PluginError::CorruptStore))
            .transpose()?,
        desired_state: DesiredPluginState::parse(
            &row.try_get::<String, _>("desired_state")
                .map_err(store_error)?,
        )?,
        restart_sequence: u64::try_from(restart_sequence).map_err(|_| {
            PluginError::CorruptStore("plugin restart sequence is negative".to_owned())
        })?,
        consumed_restart_sequence: u64::try_from(consumed_restart_sequence).map_err(|_| {
            PluginError::CorruptStore("plugin consumed restart sequence is negative".to_owned())
        })?,
        restart_attempt: u32::try_from(restart_attempt).map_err(|_| {
            PluginError::CorruptStore("plugin restart attempt is out of range".to_owned())
        })?,
        restart_not_before: optional_timestamp(
            row.try_get("restart_not_before_seconds")
                .map_err(store_error)?,
            row.try_get("restart_not_before_nanos")
                .map_err(store_error)?,
            "plugin restart deadline",
        )?,
        last_lifecycle_failure: row.try_get("last_lifecycle_failure").map_err(store_error)?,
    })
}
