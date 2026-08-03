use sqlx::{Any, Transaction};
use uuid::Uuid;

use super::{
    super::ReplicaError, ReplicaStore, support::store_error, write::require_active_generation,
};

impl ReplicaStore {
    pub async fn projection_pending(&self) -> Result<bool, ReplicaError> {
        let value: i64 =
            sqlx::query_scalar("SELECT projection_pending FROM oll_meta WHERE singleton = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(store_error)?;
        match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReplicaError::CorruptStore(
                "projection_pending is not boolean".to_owned(),
            )),
        }
    }

    pub async fn clear_projection_pending(&self, generation_id: Uuid) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, generation_id).await?;
        sqlx::query(
            "UPDATE oll_meta SET projection_pending = 0
             WHERE singleton = 1",
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        sqlx::query("DELETE FROM projection_paths WHERE generation_id = $1")
            .bind(generation_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub async fn projection_paths(&self, generation_id: Uuid) -> Result<Vec<String>, ReplicaError> {
        sqlx::query_scalar(
            "SELECT path FROM projection_paths
             WHERE generation_id = $1 ORDER BY path",
        )
        .bind(generation_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)
    }

    pub async fn clear_projection_path(
        &self,
        generation_id: Uuid,
        path: &str,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, generation_id).await?;
        sqlx::query(
            "DELETE FROM projection_paths
             WHERE generation_id = $1 AND path = $2",
        )
        .bind(generation_id.to_string())
        .bind(path)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }
}

pub(super) async fn write_projection_paths(
    transaction: &mut Transaction<'_, Any>,
    generation_id: Uuid,
    paths: &[String],
) -> Result<(), ReplicaError> {
    for path in paths {
        sqlx::query(
            "INSERT INTO projection_paths (generation_id, path)
             VALUES ($1, $2)
             ON CONFLICT (generation_id, path) DO NOTHING",
        )
        .bind(generation_id.to_string())
        .bind(path)
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}
