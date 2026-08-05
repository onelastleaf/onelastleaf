use sqlx::{Row, any::AnyRow};

use super::super::{
    super::{InstallMode, InstalledPlugin, PackagePublishIntent, PluginError, PluginId},
    PluginStore,
    convert::{hash_array, parse_uuid},
    plugins::parse_plugin,
    store_error,
};

impl PluginStore {
    pub async fn prepare_package_publish(
        &self,
        intent: &PackagePublishIntent,
    ) -> Result<(), PluginError> {
        validate_package_intent(intent)?;
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let current: Option<String> =
            sqlx::query_scalar("SELECT current_generation FROM plugins WHERE plugin_id = $1")
                .bind(intent.plugin_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if current.as_deref()
            != intent
                .expected_current_generation
                .map(|value| value.to_string())
                .as_deref()
        {
            return Err(PluginError::FailedPrecondition(
                "plugin current generation changed before publish intent".to_owned(),
            ));
        }
        let removing: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM plugin_removal_intents WHERE plugin_id = $1")
                .bind(intent.plugin_id.as_str())
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if removing != 0 {
            return Err(PluginError::FailedPrecondition(
                "plugin is being removed".to_owned(),
            ));
        }
        let name_conflict: i64 = sqlx::query_scalar(
            "SELECT (
                (SELECT COUNT(*) FROM plugins
                 WHERE plugin_name = $1 AND plugin_id <> $2) +
                (SELECT COUNT(*) FROM plugin_package_publish_intents
                 WHERE plugin_name = $1 AND plugin_id <> $2)
             )",
        )
        .bind(intent.plugin_name.as_str())
        .bind(intent.plugin_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if name_conflict != 0 {
            return Err(PluginError::AlreadyExists(format!(
                "plugin name {} is already installed or being published",
                intent.plugin_name
            )));
        }
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM plugin_package_publish_intents WHERE plugin_id = $1",
        )
        .bind(intent.plugin_id.as_str())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if existing != 0 {
            return Err(PluginError::AlreadyExists(
                "plugin already has a package publish intent".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO plugin_package_publish_intents (
                plugin_id, plugin_name, operation_id,
                expected_current_generation, candidate_generation,
                normalized_declaration, declaration_sha256, effective_manifest,
                selected_commit, install_mode, release_id, correlation_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(intent.plugin_id.as_str())
        .bind(intent.plugin_name.as_str())
        .bind(&intent.operation_id)
        .bind(
            intent
                .expected_current_generation
                .map(|value| value.to_string()),
        )
        .bind(intent.candidate_generation.to_string())
        .bind(&intent.normalized_declaration)
        .bind(intent.declaration_sha256.as_slice())
        .bind(&intent.effective_manifest)
        .bind(&intent.selected_commit)
        .bind(intent.install_mode.as_str())
        .bind(&intent.release_id)
        .bind(&intent.correlation_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_unique)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn package_publish_intent(
        &self,
        plugin_id: &PluginId,
    ) -> Result<Option<PackagePublishIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_package_publish_intents WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(parse_package_intent)
            .transpose()
    }

    pub async fn package_publish_intents(&self) -> Result<Vec<PackagePublishIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_package_publish_intents ORDER BY plugin_id")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?
            .iter()
            .map(parse_package_intent)
            .collect()
    }

    pub async fn discard_package_publish_intent(
        &self,
        plugin_id: &PluginId,
        candidate_generation: uuid::Uuid,
    ) -> Result<bool, PluginError> {
        let result = sqlx::query(
            "DELETE FROM plugin_package_publish_intents
             WHERE plugin_id = $1 AND candidate_generation = $2",
        )
        .bind(plugin_id.as_str())
        .bind(candidate_generation.to_string())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn finalize_package_publish(
        &self,
        plugin_id: &PluginId,
        candidate_generation: uuid::Uuid,
    ) -> Result<InstalledPlugin, PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let row = sqlx::query(
            "SELECT * FROM plugin_package_publish_intents
             WHERE plugin_id = $1 AND candidate_generation = $2",
        )
        .bind(plugin_id.as_str())
        .bind(candidate_generation.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| PluginError::NotFound("package publish intent was not found".to_owned()))?;
        let intent = parse_package_intent(&row)?;
        let current: Option<String> =
            sqlx::query_scalar("SELECT current_generation FROM plugins WHERE plugin_id = $1")
                .bind(plugin_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if current.as_deref()
            != intent
                .expected_current_generation
                .map(|value| value.to_string())
                .as_deref()
        {
            return Err(PluginError::FailedPrecondition(
                "plugin current generation changed before publish finalization".to_owned(),
            ));
        }
        let conflict: Option<String> = sqlx::query_scalar(
            "SELECT plugin_id FROM plugins WHERE plugin_name = $1 AND plugin_id <> $2",
        )
        .bind(intent.plugin_name.as_str())
        .bind(plugin_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        if conflict.is_some() {
            return Err(PluginError::AlreadyExists(format!(
                "plugin name {} is already installed",
                intent.plugin_name
            )));
        }
        if current.is_some() {
            sqlx::query(
                "UPDATE plugins SET plugin_name = $1,
                    normalized_declaration = $2, declaration_sha256 = $3,
                    effective_manifest = $4, selected_commit = $5,
                    install_mode = $6, release_id = $7,
                    current_generation = $8
                 WHERE plugin_id = $9 AND current_generation = $10",
            )
            .bind(intent.plugin_name.as_str())
            .bind(&intent.normalized_declaration)
            .bind(intent.declaration_sha256.as_slice())
            .bind(&intent.effective_manifest)
            .bind(&intent.selected_commit)
            .bind(intent.install_mode.as_str())
            .bind(&intent.release_id)
            .bind(intent.candidate_generation.to_string())
            .bind(plugin_id.as_str())
            .bind(
                intent
                    .expected_current_generation
                    .map(|value| value.to_string()),
            )
            .execute(&mut *transaction)
            .await
            .map_err(map_unique)?;
        } else {
            sqlx::query(
                "INSERT INTO plugins (
                    plugin_id, plugin_name, normalized_declaration,
                    declaration_sha256, effective_manifest, selected_commit,
                    install_mode, release_id, current_generation,
                    running_generation, running_instance_id, desired_state,
                    restart_sequence, consumed_restart_sequence, restart_attempt,
                    restart_not_before_seconds, restart_not_before_nanos,
                    last_lifecycle_failure
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    NULL, NULL, 'stopped', 0, 0, 0, NULL, NULL, NULL
                 )",
            )
            .bind(plugin_id.as_str())
            .bind(intent.plugin_name.as_str())
            .bind(&intent.normalized_declaration)
            .bind(intent.declaration_sha256.as_slice())
            .bind(&intent.effective_manifest)
            .bind(&intent.selected_commit)
            .bind(intent.install_mode.as_str())
            .bind(&intent.release_id)
            .bind(intent.candidate_generation.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(map_unique)?;
        }
        sqlx::query(
            "DELETE FROM plugin_package_publish_intents
             WHERE plugin_id = $1 AND candidate_generation = $2",
        )
        .bind(plugin_id.as_str())
        .bind(candidate_generation.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        let row = sqlx::query("SELECT * FROM plugins WHERE plugin_id = $1")
            .bind(plugin_id.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        let plugin = parse_plugin(&row)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(plugin)
    }
}

fn validate_package_intent(intent: &PackagePublishIntent) -> Result<(), PluginError> {
    if intent.operation_id.is_empty() || intent.correlation_id.is_empty() {
        return Err(PluginError::InvalidArgument(
            "package operation and correlation IDs must not be empty".to_owned(),
        ));
    }
    if intent.candidate_generation.get_version_num() != 4 {
        return Err(PluginError::InvalidArgument(
            "candidate install generation must be UUID v4".to_owned(),
        ));
    }
    if intent.normalized_declaration.is_empty() || intent.effective_manifest.is_empty() {
        return Err(PluginError::InvalidArgument(
            "package declaration and effective manifest must not be empty".to_owned(),
        ));
    }
    if intent.expected_current_generation == Some(intent.candidate_generation) {
        return Err(PluginError::InvalidArgument(
            "candidate install generation must differ from the current generation".to_owned(),
        ));
    }
    if intent.release_id.as_ref().is_some_and(String::is_empty) {
        return Err(PluginError::InvalidArgument(
            "release ID must not be empty".to_owned(),
        ));
    }
    match (intent.install_mode, intent.release_id.as_ref()) {
        (InstallMode::Source, Some(_)) => Err(PluginError::InvalidArgument(
            "source package cannot select a release".to_owned(),
        )),
        (InstallMode::Release, None) => Err(PluginError::InvalidArgument(
            "release package must select a release".to_owned(),
        )),
        _ => Ok(()),
    }
}

fn parse_package_intent(row: &AnyRow) -> Result<PackagePublishIntent, PluginError> {
    Ok(PackagePublishIntent {
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
        operation_id: row.try_get("operation_id").map_err(store_error)?,
        expected_current_generation: row
            .try_get::<Option<String>, _>("expected_current_generation")
            .map_err(store_error)?
            .map(|value| parse_uuid(&value, "expected current plugin generation"))
            .transpose()?,
        candidate_generation: parse_uuid(
            &row.try_get::<String, _>("candidate_generation")
                .map_err(store_error)?,
            "candidate plugin generation",
        )?,
        normalized_declaration: row.try_get("normalized_declaration").map_err(store_error)?,
        declaration_sha256: hash_array(
            row.try_get("declaration_sha256").map_err(store_error)?,
            "package declaration digest",
        )?,
        effective_manifest: row.try_get("effective_manifest").map_err(store_error)?,
        selected_commit: row.try_get("selected_commit").map_err(store_error)?,
        install_mode: InstallMode::parse(
            &row.try_get::<String, _>("install_mode")
                .map_err(store_error)?,
        )?,
        release_id: row.try_get("release_id").map_err(store_error)?,
        correlation_id: row.try_get("correlation_id").map_err(store_error)?,
    })
}

fn map_unique(error: sqlx::Error) -> PluginError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        PluginError::AlreadyExists("plugin ID or name is already installed".to_owned())
    } else {
        store_error(error)
    }
}
