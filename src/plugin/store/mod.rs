mod artifacts;
mod convert;
mod intents;
mod jobs;
mod plugins;
mod schema;

#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use sqlx::AnyPool;

use super::PluginError;
use jobs::terminal::JobTerminalNotifications;

#[derive(Clone, Debug)]
pub struct PluginStore {
    pool: AnyPool,
    terminal_jobs: Arc<JobTerminalNotifications>,
}

impl PluginStore {
    pub async fn initialize(pool: AnyPool) -> Result<Self, PluginError> {
        for statement in schema::SCHEMA {
            sqlx::query(*statement)
                .execute(&pool)
                .await
                .map_err(store_error)?;
        }
        schema::migrate_existing_tables(&pool)
            .await
            .map_err(store_error)?;
        sqlx::query(
            "INSERT INTO plugin_meta (singleton, artifact_download_dir)
             VALUES (1, NULL)
             ON CONFLICT (singleton) DO NOTHING",
        )
        .execute(&pool)
        .await
        .map_err(store_error)?;
        Ok(Self {
            pool,
            terminal_jobs: Arc::new(JobTerminalNotifications::default()),
        })
    }
}

pub(super) fn store_error(error: impl std::fmt::Display) -> PluginError {
    PluginError::Store(format!("plugin-store operation failed: {error}"))
}
