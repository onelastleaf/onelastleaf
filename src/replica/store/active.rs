use sqlx::Row;
use uuid::Uuid;

use super::{
    super::{ReplicaError, types::ActiveReplica, types::parse_uuid_v4},
    ReplicaStore,
    state::state_token,
    support::store_error,
};

impl ReplicaStore {
    pub async fn load_active(&self) -> Result<Option<ActiveReplica>, ReplicaError> {
        let row = sqlx::query("SELECT active_generation FROM oll_meta WHERE singleton = 1")
            .fetch_one(&self.pool)
            .await
            .map_err(store_error)?;
        let Some(generation_id) = row
            .try_get::<Option<String>, _>("active_generation")
            .map_err(store_error)?
        else {
            return Ok(None);
        };
        self.load_generation(&generation_id).await.map(Some)
    }

    pub(crate) async fn active_generation_id(&self) -> Result<Option<Uuid>, ReplicaError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(store_error)?;
        value
            .map(|value| parse_uuid_v4(&value, "active_generation"))
            .transpose()
    }

    #[cfg(test)]
    pub(crate) async fn generation_exists(
        &self,
        generation_id: Uuid,
    ) -> Result<bool, ReplicaError> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM replica_generations WHERE generation_id = $1")
                .bind(generation_id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(count != 0)
    }

    #[cfg(test)]
    pub(crate) async fn blob_exists(&self, sha256: &str) -> Result<bool, ReplicaError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs WHERE sha256 = $1")
            .bind(sha256)
            .fetch_one(&self.pool)
            .await
            .map_err(store_error)?;
        Ok(count != 0)
    }

    pub(crate) async fn ensure_active_state_guard(
        &self,
        active: Option<&ActiveReplica>,
    ) -> Result<(), ReplicaError> {
        let row = sqlx::query(
            "SELECT generation_id, state_token FROM active_state_guard WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        match (active, row) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(ReplicaError::CorruptStore(
                "active-state guard exists without an active replica".to_owned(),
            )),
            (Some(replica), None) => {
                let token = state_token(replica);
                sqlx::query(
                    "INSERT INTO generation_state_tokens (generation_id, state_token)
                     VALUES ($1, $2)
                     ON CONFLICT (generation_id) DO UPDATE SET state_token = excluded.state_token",
                )
                .bind(replica.generation_id.to_string())
                .bind(token.as_slice())
                .execute(&self.pool)
                .await
                .map_err(store_error)?;
                sqlx::query(
                    "INSERT INTO active_state_guard (singleton, generation_id, state_token)
                     VALUES (1, $1, $2)",
                )
                .bind(replica.generation_id.to_string())
                .bind(token.as_slice())
                .execute(&self.pool)
                .await
                .map_err(store_error)?;
                Ok(())
            }
            (Some(replica), Some(row)) => {
                let generation = row
                    .try_get::<String, _>("generation_id")
                    .map_err(store_error)?;
                let token = row
                    .try_get::<Vec<u8>, _>("state_token")
                    .map_err(store_error)?;
                if generation != replica.generation_id.to_string()
                    || token.as_slice() != state_token(replica)
                {
                    return Err(ReplicaError::CorruptStore(
                        "active-state guard differs from the active replica".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }

    pub(crate) async fn active_state_token(
        &self,
        generation_id: Uuid,
    ) -> Result<[u8; 32], ReplicaError> {
        let token: Vec<u8> = sqlx::query_scalar(
            "SELECT state_token FROM active_state_guard
             WHERE singleton = 1 AND generation_id = $1",
        )
        .bind(generation_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?
        .ok_or_else(|| ReplicaError::CorruptStore("active-state guard is missing".to_owned()))?;
        token.try_into().map_err(|_| {
            ReplicaError::CorruptStore("active-state guard token has an invalid length".to_owned())
        })
    }
}
