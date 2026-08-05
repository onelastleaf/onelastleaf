use std::path::Path;

use sqlx::{Row, any::AnyRow};
use time::OffsetDateTime;

use super::{
    super::{ArtifactPublishIntent, PluginArtifact, PluginArtifactId, PluginError, PluginJobId},
    PluginStore,
    convert::{
        hash_array, parse_timestamp, parse_u64_text, path_bytes, path_from_bytes, timestamp_parts,
        u64_text,
    },
    store_error,
};

impl PluginStore {
    pub async fn cache_artifact_download_dir(&self, path: &Path) -> Result<(), PluginError> {
        if !path.is_absolute() || path.as_os_str().is_empty() {
            return Err(PluginError::InvalidArgument(
                "artifact download directory must be absolute and nonempty".to_owned(),
            ));
        }
        let updated =
            sqlx::query("UPDATE plugin_meta SET artifact_download_dir = $1 WHERE singleton = 1")
                .bind(path_bytes(path))
                .execute(&self.pool)
                .await
                .map_err(store_error)?;
        if updated.rows_affected() != 1 {
            return Err(PluginError::CorruptStore(
                "plugin metadata singleton row is missing".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn artifact_download_dir(&self) -> Result<Option<std::path::PathBuf>, PluginError> {
        let bytes: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT artifact_download_dir FROM plugin_meta WHERE singleton = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(store_error)?;
        bytes.map(path_from_bytes).transpose()
    }

    pub async fn prepare_artifact_publish(
        &self,
        intent: &ArtifactPublishIntent,
    ) -> Result<(), PluginError> {
        validate_artifact_intent(intent)?;
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        // Removal and artifact admission are mutually exclusive transitions.
        // A no-op update provides one portable serialization point for the
        // plugin row on both SQLite and PostgreSQL; prepare_removal takes the
        // same lock before publishing its durable intent.
        let installed = sqlx::query(
            "UPDATE plugins SET restart_sequence = restart_sequence WHERE plugin_id = $1",
        )
        .bind(intent.plugin_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if installed.rows_affected() != 1 {
            return Err(PluginError::FailedPrecondition(
                "artifact plugin is not installed".to_owned(),
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
                "plugin removal has already begun".to_owned(),
            ));
        }
        let job = sqlx::query(
            "SELECT plugin_id, correlation_id FROM plugin_jobs
             WHERE job_id = $1 AND state IN ('dispatching', 'running', 'cancelling')",
        )
        .bind(intent.job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        let Some(job) = job else {
            return Err(PluginError::FailedPrecondition(
                "artifact job is not active".to_owned(),
            ));
        };
        let job_plugin: String = job.try_get("plugin_id").map_err(store_error)?;
        let job_correlation: String = job.try_get("correlation_id").map_err(store_error)?;
        if job_plugin != intent.plugin_id.as_str() {
            return Err(PluginError::FailedPrecondition(
                "artifact job does not belong to the declaring plugin".to_owned(),
            ));
        }
        if job_correlation != intent.correlation_id {
            return Err(PluginError::FailedPrecondition(
                "artifact publication correlation differs from its job".to_owned(),
            ));
        }
        let duplicate: i64 = sqlx::query_scalar(
            "SELECT (
                (SELECT COUNT(*) FROM plugin_artifacts WHERE artifact_id = $1) +
                (SELECT COUNT(*) FROM plugin_artifact_publish_intents WHERE artifact_id = $1)
             )",
        )
        .bind(intent.artifact_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if duplicate != 0 {
            return Err(PluginError::AlreadyExists(format!(
                "plugin artifact {} already exists",
                intent.artifact_id
            )));
        }
        sqlx::query(
            "INSERT INTO plugin_artifact_publish_intents (
                artifact_id, job_id, plugin_id, file_name, media_type,
                size_bytes, sha256, staging_path, destination, correlation_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(intent.artifact_id.to_string())
        .bind(intent.job_id.to_string())
        .bind(intent.plugin_id.as_str())
        .bind(&intent.file_name)
        .bind(&intent.media_type)
        .bind(u64_text(intent.size_bytes))
        .bind(intent.sha256.as_slice())
        .bind(path_bytes(&intent.staging_path))
        .bind(path_bytes(&intent.destination))
        .bind(&intent.correlation_id)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn artifact_publish_intent(
        &self,
        artifact_id: PluginArtifactId,
    ) -> Result<Option<ArtifactPublishIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_artifact_publish_intents WHERE artifact_id = $1")
            .bind(artifact_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(parse_artifact_intent)
            .transpose()
    }

    pub async fn artifact_publish_intents(
        &self,
    ) -> Result<Vec<ArtifactPublishIntent>, PluginError> {
        sqlx::query("SELECT * FROM plugin_artifact_publish_intents ORDER BY artifact_id")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?
            .iter()
            .map(parse_artifact_intent)
            .collect()
    }

    pub async fn discard_artifact_publish_intent(
        &self,
        artifact_id: PluginArtifactId,
    ) -> Result<bool, PluginError> {
        let result =
            sqlx::query("DELETE FROM plugin_artifact_publish_intents WHERE artifact_id = $1")
                .bind(artifact_id.to_string())
                .execute(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn finalize_artifact_publish(
        &self,
        artifact_id: PluginArtifactId,
        stored_at: OffsetDateTime,
    ) -> Result<PluginArtifact, PluginError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let Some(row) =
            sqlx::query("SELECT * FROM plugin_artifact_publish_intents WHERE artifact_id = $1")
                .bind(artifact_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?
        else {
            let row = sqlx::query("SELECT * FROM plugin_artifacts WHERE artifact_id = $1")
                .bind(artifact_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?
                .ok_or_else(|| {
                    PluginError::NotFound(format!("plugin artifact {artifact_id} was not found"))
                })?;
            return parse_artifact(&row);
        };
        let intent = parse_artifact_intent(&row)?;
        let (seconds, nanos) = timestamp_parts(stored_at);
        sqlx::query(
            "INSERT INTO plugin_artifacts (
                artifact_id, job_id, plugin_id, file_name, media_type,
                size_bytes, sha256, destination, stored_at_seconds, stored_at_nanos
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(intent.artifact_id.to_string())
        .bind(intent.job_id.to_string())
        .bind(intent.plugin_id.as_str())
        .bind(&intent.file_name)
        .bind(&intent.media_type)
        .bind(u64_text(intent.size_bytes))
        .bind(intent.sha256.as_slice())
        .bind(path_bytes(&intent.destination))
        .bind(seconds)
        .bind(nanos)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        sqlx::query("DELETE FROM plugin_artifact_publish_intents WHERE artifact_id = $1")
            .bind(artifact_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        let row = sqlx::query("SELECT * FROM plugin_artifacts WHERE artifact_id = $1")
            .bind(artifact_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        let artifact = parse_artifact(&row)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(artifact)
    }

    pub async fn get_artifact(
        &self,
        artifact_id: PluginArtifactId,
    ) -> Result<PluginArtifact, PluginError> {
        let row = sqlx::query("SELECT * FROM plugin_artifacts WHERE artifact_id = $1")
            .bind(artifact_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?
            .ok_or_else(|| {
                PluginError::NotFound(format!("plugin artifact {artifact_id} was not found"))
            })?;
        parse_artifact(&row)
    }

    pub async fn artifacts_for_job(
        &self,
        job_id: PluginJobId,
    ) -> Result<Vec<PluginArtifact>, PluginError> {
        sqlx::query("SELECT * FROM plugin_artifacts WHERE job_id = $1 ORDER BY artifact_id")
            .bind(job_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?
            .iter()
            .map(parse_artifact)
            .collect()
    }
}

fn validate_artifact_intent(intent: &ArtifactPublishIntent) -> Result<(), PluginError> {
    let file_name = intent.file_name.as_bytes();
    if file_name.is_empty()
        || file_name.len() > 191
        || intent.file_name == "."
        || intent.file_name == ".."
        || file_name.contains(&0)
        || intent.file_name.contains('/')
    {
        return Err(PluginError::InvalidArgument(
            "artifact file name must be one safe UTF-8 basename of at most 191 bytes".to_owned(),
        ));
    }
    if intent.media_type.is_empty() || intent.correlation_id.is_empty() {
        return Err(PluginError::InvalidArgument(
            "artifact media type and correlation ID must not be empty".to_owned(),
        ));
    }
    if !intent.staging_path.is_absolute() || !intent.destination.is_absolute() {
        return Err(PluginError::InvalidArgument(
            "artifact staging and destination paths must be absolute".to_owned(),
        ));
    }
    Ok(())
}

fn parse_artifact_intent(row: &AnyRow) -> Result<ArtifactPublishIntent, PluginError> {
    Ok(ArtifactPublishIntent {
        artifact_id: row
            .try_get::<String, _>("artifact_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        job_id: row
            .try_get::<String, _>("job_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        plugin_id: row
            .try_get::<String, _>("plugin_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        file_name: row.try_get("file_name").map_err(store_error)?,
        media_type: row.try_get("media_type").map_err(store_error)?,
        size_bytes: parse_u64_text(
            &row.try_get::<String, _>("size_bytes")
                .map_err(store_error)?,
            "plugin artifact size",
        )?,
        sha256: hash_array(
            row.try_get("sha256").map_err(store_error)?,
            "plugin artifact SHA-256",
        )?,
        staging_path: path_from_bytes(row.try_get("staging_path").map_err(store_error)?)?,
        destination: path_from_bytes(row.try_get("destination").map_err(store_error)?)?,
        correlation_id: row.try_get("correlation_id").map_err(store_error)?,
    })
}

fn parse_artifact(row: &AnyRow) -> Result<PluginArtifact, PluginError> {
    Ok(PluginArtifact {
        artifact_id: row
            .try_get::<String, _>("artifact_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        job_id: row
            .try_get::<String, _>("job_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        plugin_id: row
            .try_get::<String, _>("plugin_id")
            .map_err(store_error)?
            .parse()
            .map_err(PluginError::CorruptStore)?,
        file_name: row.try_get("file_name").map_err(store_error)?,
        media_type: row.try_get("media_type").map_err(store_error)?,
        size_bytes: parse_u64_text(
            &row.try_get::<String, _>("size_bytes")
                .map_err(store_error)?,
            "plugin artifact size",
        )?,
        sha256: hash_array(
            row.try_get("sha256").map_err(store_error)?,
            "plugin artifact SHA-256",
        )?,
        destination: path_from_bytes(row.try_get("destination").map_err(store_error)?)?,
        stored_at: parse_timestamp(
            row.try_get("stored_at_seconds").map_err(store_error)?,
            row.try_get("stored_at_nanos").map_err(store_error)?,
            "plugin artifact storage time",
        )?,
    })
}
