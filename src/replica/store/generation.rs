use uuid::Uuid;

use super::{
    super::{ReplicaError, types::ActiveReplica, types::OperationRecord},
    NewBlob, ReplicaStore,
    projection::write_projection_paths,
    state::state_token,
    support::store_error,
    write::write_generation,
};

impl ReplicaStore {
    pub async fn build_inactive_generation(
        &self,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
        projection_paths: &[String],
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        write_generation(&mut transaction, replica, blobs, operations).await?;
        write_projection_paths(&mut transaction, replica.generation_id, projection_paths).await?;
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn build_sync_generation(
        &self,
        expected_generation: Uuid,
        replica: &ActiveReplica,
        blobs: &[NewBlob],
        operations: &[OperationRecord],
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        sqlx::query(
            "INSERT INTO replica_operations (
                generation_id, timestamp, operation_id, source, kind,
                catalog_node_id, document_id, path_before, path_after,
                correlation_id
             )
             SELECT $1, timestamp, operation_id, source, kind,
                    catalog_node_id, document_id, path_before, path_after,
                    correlation_id
             FROM replica_operations WHERE generation_id = $2",
        )
        .bind(replica.generation_id.to_string())
        .bind(expected_generation.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        sqlx::query(
            "INSERT INTO retained_commits (generation_id, operation_id, request, response)
             SELECT $1, operation_id, request, response
             FROM retained_commits WHERE generation_id = $2",
        )
        .bind(replica.generation_id.to_string())
        .bind(expected_generation.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        write_generation(&mut transaction, replica, blobs, operations).await?;
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn activate_sync_generation(
        &self,
        expected_generation: Uuid,
        expected_state_token: [u8; 32],
        candidate: &ActiveReplica,
    ) -> Result<(), ReplicaError> {
        let candidate_token = state_token(candidate);
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let guard = sqlx::query(
            "UPDATE active_state_guard
             SET generation_id = $1, state_token = $2
             WHERE singleton = 1 AND generation_id = $3 AND state_token = $4",
        )
        .bind(candidate.generation_id.to_string())
        .bind(candidate_token.as_slice())
        .bind(expected_generation.to_string())
        .bind(expected_state_token.as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if guard.rows_affected() != 1 {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed during synchronization".to_owned(),
            ));
        }
        let active = sqlx::query(
            "UPDATE oll_meta SET active_generation = $1, projection_pending = 1
             WHERE singleton = 1 AND active_generation = $2",
        )
        .bind(candidate.generation_id.to_string())
        .bind(expected_generation.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if active.rows_affected() != 1 {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed during synchronization".to_owned(),
            ));
        }
        transaction.commit().await.map_err(store_error)
    }
}
