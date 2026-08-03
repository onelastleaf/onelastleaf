use sqlx::Row;
use uuid::Uuid;

use super::{
    super::{ReplicaError, types::ActiveReplica, types::OperationRecord},
    NewBlob, ReplicaStore,
    projection::write_projection_paths,
    state::state_token,
    support::store_error,
    write::{require_active_generation, write_generation},
};

#[derive(Debug)]
pub struct RetainedCommit {
    pub operation_id: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
}

impl ReplicaStore {
    pub async fn save_active(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
    ) -> Result<(), ReplicaError> {
        self.save_active_inner(replica, blobs, operations, projection_paths, None)
            .await
    }

    pub async fn save_active_commit(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
        commit: &RetainedCommit,
    ) -> Result<(), ReplicaError> {
        self.save_active_inner(replica, blobs, operations, projection_paths, Some(commit))
            .await
    }

    async fn save_active_inner(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
        commit: Option<&RetainedCommit>,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, replica.generation_id).await?;
        write_generation(&mut transaction, replica, blobs, operations).await?;
        write_projection_paths(&mut transaction, replica.generation_id, projection_paths).await?;
        let guard = sqlx::query(
            "UPDATE active_state_guard SET state_token = $1
             WHERE singleton = 1 AND generation_id = $2",
        )
        .bind(state_token(replica).as_slice())
        .bind(replica.generation_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if guard.rows_affected() != 1 {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed during local commit".to_owned(),
            ));
        }
        if let Some(commit) = commit {
            sqlx::query(
                "INSERT INTO retained_commits (
                    generation_id, operation_id, request, response
                 ) VALUES ($1, $2, $3, $4)",
            )
            .bind(replica.generation_id.to_string())
            .bind(&commit.operation_id)
            .bind(&commit.request)
            .bind(&commit.response)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        }
        transaction.commit().await.map_err(store_error)
    }

    pub async fn retained_commit(
        &self,
        generation_id: Uuid,
        operation_id: &str,
    ) -> Result<Option<RetainedCommit>, ReplicaError> {
        let row = sqlx::query(
            "SELECT request, response
             FROM retained_commits
             WHERE generation_id = $1 AND operation_id = $2",
        )
        .bind(generation_id.to_string())
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| {
            Ok(RetainedCommit {
                operation_id: operation_id.to_owned(),
                request: row.try_get::<Vec<u8>, _>("request").map_err(store_error)?,
                response: row.try_get::<Vec<u8>, _>("response").map_err(store_error)?,
            })
        })
        .transpose()
    }
}
