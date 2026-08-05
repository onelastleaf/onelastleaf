use sqlx::{Row, any::AnyRow};

use super::super::{
    super::{PluginError, PluginId, RemovalIntent, RemovalPhase},
    PluginStore,
    convert::{hash_array, path_bytes, path_from_bytes},
    store_error,
};

impl PluginStore {
    pub async fn prepare_removal(&self, intent: &RemovalIntent) -> Result<(), PluginError> {
        if intent.operation_id.is_empty() || intent.correlation_id.is_empty() {
            return Err(PluginError::InvalidArgument(
                "plugin removal operation and correlation IDs must not be empty".to_owned(),
            ));
        }
        if !intent.trash_path.is_absolute() {
            return Err(PluginError::InvalidArgument(
                "plugin removal trash path must be absolute".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        // Serialize against artifact publication admission before making the
        // removal intent visible. Once this transaction commits, a later
        // artifact transition observes the intent and cannot create work that
        // outlives package/job deletion.
        let installed = sqlx::query(
            "UPDATE plugins SET restart_sequence = restart_sequence WHERE plugin_id = $1",
        )
        .bind(intent.plugin_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if installed.rows_affected() != 1 {
            return Err(PluginError::NotFound(format!(
                "plugin {} is not installed",
                intent.plugin_id
            )));
        }
        if let Some(row) = sqlx::query("SELECT * FROM plugin_removal_intents WHERE plugin_id = $1")
            .bind(intent.plugin_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?
        {
            if parse_removal_intent(&row)? == *intent {
                return Ok(());
            }
            return Err(PluginError::AlreadyExists(
                "plugin already has another removal intent".to_owned(),
            ));
        }
        let publishing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM plugin_package_publish_intents WHERE plugin_id = $1",
        )
        .bind(intent.plugin_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if publishing != 0 {
            return Err(PluginError::FailedPrecondition(
                "plugin has an unfinished package publication".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO plugin_removal_intents (
                plugin_id, operation_id, plugins_lua_sha256,
                prepared_plugins_lua, trash_path, phase, correlation_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(intent.plugin_id.as_str())
        .bind(&intent.operation_id)
        .bind(intent.plugins_lua_sha256.as_slice())
        .bind(&intent.prepared_plugins_lua)
        .bind(path_bytes(&intent.trash_path))
        .bind(intent.phase.as_str())
        .bind(&intent.correlation_id)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn removal_intent(
        &self,
        plugin_id: &PluginId,
    ) -> Result<Option<RemovalIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_removal_intents WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(parse_removal_intent)
            .transpose()
    }

    pub async fn removal_intents(&self) -> Result<Vec<RemovalIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_removal_intents ORDER BY plugin_id")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?
            .iter()
            .map(parse_removal_intent)
            .collect()
    }

    pub async fn discard_prepared_removal(
        &self,
        plugin_id: &PluginId,
    ) -> Result<bool, PluginError> {
        let result = sqlx::query(
            "DELETE FROM plugin_removal_intents
             WHERE plugin_id = $1 AND phase = 'prepared'",
        )
        .bind(plugin_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn advance_removal(
        &self,
        plugin_id: &PluginId,
        expected: RemovalPhase,
        next: RemovalPhase,
    ) -> Result<(), PluginError> {
        let valid = matches!(
            (expected, next),
            (RemovalPhase::Prepared, RemovalPhase::DeclarationPublished)
                | (
                    RemovalPhase::DeclarationPublished,
                    RemovalPhase::PackageTrashed
                )
        );
        if !valid {
            return Err(PluginError::InvalidArgument(
                "invalid plugin removal phase transition".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE plugin_removal_intents SET phase = $1
             WHERE plugin_id = $2 AND phase = $3",
        )
        .bind(next.as_str())
        .bind(plugin_id.as_str())
        .bind(expected.as_str())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(PluginError::FailedPrecondition(
                "plugin removal phase changed".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn finalize_removal(&self, plugin_id: &PluginId) -> Result<(), PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let phase: Option<String> =
            sqlx::query_scalar("SELECT phase FROM plugin_removal_intents WHERE plugin_id = $1")
                .bind(plugin_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if phase.as_deref() != Some(RemovalPhase::PackageTrashed.as_str()) {
            return Err(PluginError::FailedPrecondition(
                "plugin removal has not reached package_trash".to_owned(),
            ));
        }
        let artifact_intents: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM plugin_artifact_publish_intents WHERE plugin_id = $1",
        )
        .bind(plugin_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if artifact_intents != 0 {
            return Err(PluginError::FailedPrecondition(
                "plugin has an unfinished artifact publication".to_owned(),
            ));
        }
        let running: Option<String> =
            sqlx::query_scalar("SELECT running_instance_id FROM plugins WHERE plugin_id = $1")
                .bind(plugin_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?
                .flatten();
        if running.is_some() {
            return Err(PluginError::FailedPrecondition(
                "plugin process has not stopped before removal finalization".to_owned(),
            ));
        }
        let nonterminal_jobs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM plugin_jobs
             WHERE plugin_id = $1
               AND state IN ('dispatching', 'running', 'cancelling')",
        )
        .bind(plugin_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if nonterminal_jobs != 0 {
            return Err(PluginError::FailedPrecondition(
                "plugin has nonterminal jobs before removal finalization".to_owned(),
            ));
        }
        sqlx::query("DELETE FROM plugin_artifacts WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        sqlx::query("DELETE FROM plugin_jobs WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        sqlx::query("DELETE FROM plugins WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        sqlx::query("DELETE FROM plugin_removal_intents WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }
}

fn parse_removal_intent(row: &AnyRow) -> Result<RemovalIntent, PluginError> {
    Ok(RemovalIntent {
        plugin_id: row
            .try_get::<String, _>("plugin_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        operation_id: row.try_get("operation_id").map_err(store_error)?,
        plugins_lua_sha256: hash_array(
            row.try_get("plugins_lua_sha256").map_err(store_error)?,
            "plugins.lua digest",
        )?,
        prepared_plugins_lua: row.try_get("prepared_plugins_lua").map_err(store_error)?,
        trash_path: path_from_bytes(row.try_get("trash_path").map_err(store_error)?)?,
        phase: RemovalPhase::parse(&row.try_get::<String, _>("phase").map_err(store_error)?)?,
        correlation_id: row.try_get("correlation_id").map_err(store_error)?,
    })
}
