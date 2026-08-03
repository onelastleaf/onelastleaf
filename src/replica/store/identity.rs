use sqlx::Row;
use uuid::Uuid;

use super::{
    super::{ReplicaError, types::parse_uuid_v4},
    ReplicaStore,
    support::{parse_bool, store_error},
    write::{delete_generation_rows, require_active_generation},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdentityTransitionKind {
    Initialize,
    SnapshotImport,
    Bootstrap,
}

impl IdentityTransitionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initialize => "initialize",
            Self::SnapshotImport => "snapshot_import",
            Self::Bootstrap => "bootstrap",
        }
    }

    fn parse(value: &str) -> Result<Self, ReplicaError> {
        match value {
            "initialize" => Ok(Self::Initialize),
            "snapshot_import" => Ok(Self::SnapshotImport),
            "bootstrap" => Ok(Self::Bootstrap),
            _ => Err(ReplicaError::CorruptStore(
                "replica identity transition has an unknown kind".to_owned(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IdentityTransition {
    pub kind: IdentityTransitionKind,
    pub expected_active_generation: Option<Uuid>,
    pub candidate_generation: Uuid,
    pub old_replica_id: Option<Uuid>,
    pub new_replica_id: Uuid,
    pub old_identity_file: Option<Vec<u8>>,
    pub new_identity_file: Vec<u8>,
    pub projection_pending: bool,
    pub committed: bool,
}

impl ReplicaStore {
    pub(crate) async fn identity_transition(
        &self,
    ) -> Result<Option<IdentityTransition>, ReplicaError> {
        let row = sqlx::query(
            "SELECT transition_kind, expected_active_generation,
                    candidate_generation, old_replica_id, new_replica_id,
                    old_identity_file, new_identity_file, projection_pending,
                    committed
             FROM replica_identity_transition
             WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| {
            let expected_active_generation = row
                .try_get::<Option<String>, _>("expected_active_generation")
                .map_err(store_error)?
                .map(|value| parse_uuid_v4(&value, "expected_active_generation"))
                .transpose()?;
            let old_replica_id = row
                .try_get::<Option<String>, _>("old_replica_id")
                .map_err(store_error)?
                .map(|value| parse_uuid_v4(&value, "old_replica_id"))
                .transpose()?;
            Ok(IdentityTransition {
                kind: IdentityTransitionKind::parse(
                    &row.try_get::<String, _>("transition_kind")
                        .map_err(store_error)?,
                )?,
                expected_active_generation,
                candidate_generation: parse_uuid_v4(
                    &row.try_get::<String, _>("candidate_generation")
                        .map_err(store_error)?,
                    "candidate_generation",
                )?,
                old_replica_id,
                new_replica_id: parse_uuid_v4(
                    &row.try_get::<String, _>("new_replica_id")
                        .map_err(store_error)?,
                    "new_replica_id",
                )?,
                old_identity_file: row
                    .try_get::<Option<Vec<u8>>, _>("old_identity_file")
                    .map_err(store_error)?,
                new_identity_file: row
                    .try_get::<Vec<u8>, _>("new_identity_file")
                    .map_err(store_error)?,
                projection_pending: parse_bool(
                    row.try_get::<i64, _>("projection_pending")
                        .map_err(store_error)?,
                    "identity transition projection_pending",
                )?,
                committed: parse_bool(
                    row.try_get::<i64, _>("committed").map_err(store_error)?,
                    "identity transition committed",
                )?,
            })
        })
        .transpose()
    }

    pub(crate) async fn prepare_identity_transition(
        &self,
        transition: &IdentityTransition,
    ) -> Result<(), ReplicaError> {
        if transition.committed {
            return Err(ReplicaError::Internal(
                "cannot prepare an already committed identity transition".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM replica_identity_transition")
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        if existing != 0 {
            return Err(ReplicaError::RevisionConflict(
                "another replica identity transition is already prepared".to_owned(),
            ));
        }
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        let expected = transition
            .expected_active_generation
            .map(|generation| generation.to_string());
        if active != expected {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed while identity transition was prepared".to_owned(),
            ));
        }
        let candidate_replica_id: Option<String> = sqlx::query_scalar(
            "SELECT replica_id FROM replica_generations WHERE generation_id = $1",
        )
        .bind(transition.candidate_generation.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        if candidate_replica_id.as_deref() != Some(transition.new_replica_id.to_string().as_str()) {
            return Err(ReplicaError::CorruptStore(
                "identity transition candidate is missing or has another ReplicaId".to_owned(),
            ));
        }
        match (
            transition.expected_active_generation,
            transition.old_replica_id,
        ) {
            (None, None) => {}
            (Some(generation), Some(old_replica_id)) => {
                let active_replica_id: Option<String> = sqlx::query_scalar(
                    "SELECT replica_id FROM replica_generations WHERE generation_id = $1",
                )
                .bind(generation.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
                if active_replica_id.as_deref() != Some(old_replica_id.to_string().as_str()) {
                    return Err(ReplicaError::CorruptStore(
                        "identity transition old ReplicaId differs from active state".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(ReplicaError::Internal(
                    "identity transition old identity is incomplete".to_owned(),
                ));
            }
        }
        sqlx::query(
            "INSERT INTO replica_identity_transition (
                singleton, transition_kind, expected_active_generation,
                candidate_generation, old_replica_id, new_replica_id,
                old_identity_file, new_identity_file, projection_pending,
                committed
             ) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, 0)",
        )
        .bind(transition.kind.as_str())
        .bind(expected)
        .bind(transition.candidate_generation.to_string())
        .bind(transition.old_replica_id.map(|value| value.to_string()))
        .bind(transition.new_replica_id.to_string())
        .bind(&transition.old_identity_file)
        .bind(&transition.new_identity_file)
        .bind(i64::from(transition.projection_pending))
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn activate_identity_transition(
        &self,
        candidate_generation: Uuid,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let row = sqlx::query(
            "SELECT expected_active_generation, projection_pending, committed
             FROM replica_identity_transition
             WHERE singleton = 1 AND candidate_generation = $1",
        )
        .bind(candidate_generation.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ReplicaError::CorruptStore("prepared identity transition is missing".to_owned())
        })?;
        if parse_bool(
            row.try_get::<i64, _>("committed").map_err(store_error)?,
            "identity transition committed",
        )? {
            return Err(ReplicaError::RevisionConflict(
                "replica identity transition is already committed".to_owned(),
            ));
        }
        let expected = row
            .try_get::<Option<String>, _>("expected_active_generation")
            .map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if active != expected {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed before identity transition activation".to_owned(),
            ));
        }
        let projection_pending = row
            .try_get::<i64, _>("projection_pending")
            .map_err(store_error)?;
        let candidate_token: Vec<u8> = sqlx::query_scalar(
            "SELECT state_token FROM generation_state_tokens WHERE generation_id = $1",
        )
        .bind(candidate_generation.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ReplicaError::CorruptStore("identity transition candidate token is missing".to_owned())
        })?;
        let result = sqlx::query(
            "UPDATE oll_meta
             SET active_generation = $1, projection_pending = $2
             WHERE singleton = 1 AND
                   ((active_generation IS NULL AND $3 IS NULL) OR active_generation = $3)",
        )
        .bind(candidate_generation.to_string())
        .bind(projection_pending)
        .bind(expected)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(ReplicaError::RevisionConflict(
                "active replica changed during identity transition activation".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO active_state_guard (singleton, generation_id, state_token)
             VALUES (1, $1, $2)
             ON CONFLICT (singleton) DO UPDATE SET
                 generation_id = excluded.generation_id,
                 state_token = excluded.state_token",
        )
        .bind(candidate_generation.to_string())
        .bind(candidate_token)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        let result = sqlx::query(
            "UPDATE replica_identity_transition SET committed = 1
             WHERE singleton = 1 AND candidate_generation = $1 AND committed = 0",
        )
        .bind(candidate_generation.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(ReplicaError::CorruptStore(
                "identity transition commit marker changed unexpectedly".to_owned(),
            ));
        }
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn clear_identity_transition(
        &self,
        candidate_generation: Uuid,
    ) -> Result<(), ReplicaError> {
        let result = sqlx::query(
            "DELETE FROM replica_identity_transition
             WHERE singleton = 1 AND candidate_generation = $1 AND committed = 1",
        )
        .bind(candidate_generation.to_string())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(ReplicaError::CorruptStore(
                "committed identity transition is missing during cleanup".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn rollback_identity_transition(
        &self,
        candidate_generation: Uuid,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let row = sqlx::query(
            "SELECT expected_active_generation, committed
             FROM replica_identity_transition
             WHERE singleton = 1 AND candidate_generation = $1",
        )
        .bind(candidate_generation.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ReplicaError::CorruptStore("prepared identity transition is missing".to_owned())
        })?;
        if parse_bool(
            row.try_get::<i64, _>("committed").map_err(store_error)?,
            "identity transition committed",
        )? {
            return Err(ReplicaError::CorruptStore(
                "cannot roll back a committed identity transition".to_owned(),
            ));
        }
        let expected = row
            .try_get::<Option<String>, _>("expected_active_generation")
            .map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if active != expected {
            return Err(ReplicaError::CorruptStore(
                "active generation contradicts prepared identity transition".to_owned(),
            ));
        }
        delete_generation_rows(&mut transaction, candidate_generation).await?;
        sqlx::query("DELETE FROM replica_identity_transition WHERE singleton = 1")
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn update_active_replica_id(
        &self,
        generation_id: Uuid,
        expected_replica_id: Uuid,
        replacement_replica_id: Uuid,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        require_active_generation(&mut transaction, generation_id).await?;
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM replica_identity_transition")
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
        if pending != 0 {
            return Err(ReplicaError::RevisionConflict(
                "replica identity transition is in progress".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE replica_generations SET replica_id = $1
             WHERE generation_id = $2 AND replica_id = $3",
        )
        .bind(replacement_replica_id.to_string())
        .bind(generation_id.to_string())
        .bind(expected_replica_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        if result.rows_affected() != 1 {
            return Err(ReplicaError::RevisionConflict(
                "active ReplicaId changed before identity update".to_owned(),
            ));
        }
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn discard_inactive_generation(
        &self,
        generation_id: Uuid,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if active.as_deref() == Some(generation_id.to_string().as_str()) {
            return Err(ReplicaError::CorruptStore(
                "cannot discard the active replica generation".to_owned(),
            ));
        }
        let referenced: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM replica_identity_transition
             WHERE candidate_generation = $1",
        )
        .bind(generation_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_error)?;
        if referenced != 0 {
            return Err(ReplicaError::RevisionConflict(
                "cannot discard a generation with a prepared identity transition".to_owned(),
            ));
        }
        delete_generation_rows(&mut transaction, generation_id).await?;
        transaction.commit().await.map_err(store_error)
    }
}
