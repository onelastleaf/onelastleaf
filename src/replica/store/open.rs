use std::{
    fs::{self, OpenOptions},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    str::FromStr,
};

use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use url::Url;

use crate::configuration::ReplicaStoreConfig;

use super::{super::ReplicaError, ReplicaStore, schema::SCHEMA, support::store_error};

impl ReplicaStore {
    pub async fn open(config: &ReplicaStoreConfig) -> Result<Self, ReplicaError> {
        sqlx::any::install_default_drivers();
        let (database_url, sqlite_path) = match config {
            ReplicaStoreConfig::Sqlite { path } => {
                prepare_sqlite_path(path)?;
                (sqlite_url(path)?, Some(path.clone()))
            }
            ReplicaStoreConfig::Postgres { url } => (url.expose().to_owned(), None),
        };
        let options = AnyConnectOptions::from_str(&database_url)
            .map_err(|_| ReplicaError::Store("invalid replica-store connection".to_owned()))?;
        let pool = AnyPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|_| {
                ReplicaError::Store("cannot connect to the configured replica store".to_owned())
            })?;
        for statement in SCHEMA {
            sqlx::query(*statement)
                .execute(&pool)
                .await
                .map_err(store_error)?;
        }
        sqlx::query(
            "INSERT INTO oll_meta (singleton, active_generation, projection_pending)
             VALUES (1, NULL, 0)
             ON CONFLICT (singleton) DO NOTHING",
        )
        .execute(&pool)
        .await
        .map_err(store_error)?;
        if let Some(path) = &sqlite_path {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .map_err(|error| ReplicaError::io("set SQLite store permissions", error))?;
        }
        Ok(Self { pool })
    }
}

fn prepare_sqlite_path(path: &Path) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::InvalidArgument("SQLite replica-store path has no parent".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create SQLite store directory", error))?;
    if !path.exists() {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| ReplicaError::io("create SQLite replica store", error))?;
    }
    Ok(())
}

fn sqlite_url(path: &Path) -> Result<String, ReplicaError> {
    let file = Url::from_file_path(path).map_err(|()| {
        ReplicaError::InvalidArgument("SQLite replica-store path must be absolute".to_owned())
    })?;
    Ok(file.as_str().replacen("file:", "sqlite:", 1))
}
