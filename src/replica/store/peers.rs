use sqlx::Row;
use uuid::Uuid;

use crate::{cli::NodeName, node::identity::NodeIdentity};

use super::{
    super::{ReplicaError, types::parse_uuid_v4},
    ReplicaStore,
    support::store_error,
    write::delete_generation_rows,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PeerBinding {
    pub identity: NodeIdentity,
    pub connect_targets: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BootstrapClaim {
    pub claim_id: Uuid,
    pub source_node_id: Uuid,
    pub correlation_id: String,
}

impl ReplicaStore {
    pub(crate) async fn bind_sync_peer(
        &self,
        identity: &NodeIdentity,
        connect_target: Option<&str>,
    ) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let node_id = identity.node_id().to_string();
        let node_name = identity.node_name().as_str();
        let name_for_id: Option<String> =
            sqlx::query_scalar("SELECT node_name FROM sync_peer_bindings WHERE node_id = $1")
                .bind(&node_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if name_for_id
            .as_deref()
            .is_some_and(|known| known != node_name)
        {
            return Err(ReplicaError::RevisionConflict(
                "remote NodeId is already bound to another NodeName".to_owned(),
            ));
        }
        let id_for_name: Option<String> =
            sqlx::query_scalar("SELECT node_id FROM sync_peer_bindings WHERE node_name = $1")
                .bind(node_name)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if id_for_name.as_deref().is_some_and(|known| known != node_id) {
            return Err(ReplicaError::RevisionConflict(
                "remote NodeName is already bound to another NodeId".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO sync_peer_bindings (node_id, node_name)
             VALUES ($1, $2)
             ON CONFLICT (node_id) DO NOTHING",
        )
        .bind(&node_id)
        .bind(node_name)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;

        if let Some(connect_target) = connect_target {
            let bound_node: Option<String> = sqlx::query_scalar(
                "SELECT node_id FROM sync_connect_bindings WHERE connect_target = $1",
            )
            .bind(connect_target)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?;
            if bound_node.as_deref().is_some_and(|known| known != node_id) {
                return Err(ReplicaError::RevisionConflict(
                    "connect target is already bound to another NodeId".to_owned(),
                ));
            }
            sqlx::query(
                "INSERT INTO sync_connect_bindings (connect_target, node_id)
                 VALUES ($1, $2)
                 ON CONFLICT (connect_target) DO NOTHING",
            )
            .bind(connect_target)
            .bind(&node_id)
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
        }
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn sync_peer_bindings(&self) -> Result<Vec<PeerBinding>, ReplicaError> {
        let rows = sqlx::query(
            "SELECT node_id, node_name FROM sync_peer_bindings ORDER BY node_name, node_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        let mut peers = Vec::with_capacity(rows.len());
        for row in rows {
            let node_id = parse_uuid_v4(
                &row.try_get::<String, _>("node_id").map_err(store_error)?,
                "sync peer node_id",
            )?;
            let node_name = row
                .try_get::<String, _>("node_name")
                .map_err(store_error)?
                .parse::<NodeName>()
                .map_err(|_| {
                    ReplicaError::CorruptStore(
                        "sync peer node_name is not a lowercase DNS label".to_owned(),
                    )
                })?;
            let connect_targets = sqlx::query_scalar(
                "SELECT connect_target FROM sync_connect_bindings
                 WHERE node_id = $1 ORDER BY connect_target",
            )
            .bind(node_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
            peers.push(PeerBinding {
                identity: NodeIdentity::new(node_id, node_name),
                connect_targets,
            });
        }
        Ok(peers)
    }

    pub(crate) async fn acquire_bootstrap_claim(
        &self,
        claim: &BootstrapClaim,
    ) -> Result<bool, ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        if active.is_some() {
            return Ok(false);
        }
        let result = sqlx::query(
            "INSERT INTO bootstrap_claim (singleton, claim_id, source_node_id, correlation_id)
             VALUES (1, $1, $2, $3)
             ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(claim.claim_id.to_string())
        .bind(claim.source_node_id.to_string())
        .bind(&claim.correlation_id)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn release_bootstrap_claim(&self, claim_id: Uuid) -> Result<(), ReplicaError> {
        sqlx::query("DELETE FROM bootstrap_claim WHERE singleton = 1 AND claim_id = $1")
            .bind(claim_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(store_error)?;
        Ok(())
    }

    pub(crate) async fn clear_bootstrap_claim_on_startup(&self) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let claim: Option<String> =
            sqlx::query_scalar("SELECT claim_id FROM bootstrap_claim WHERE singleton = 1")
                .fetch_optional(&mut *transaction)
                .await
                .map_err(store_error)?;
        if let Some(claim_id) = claim {
            let claim_id = parse_uuid_v4(&claim_id, "bootstrap claim_id")?;
            let active: Option<String> =
                sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(store_error)?;
            if active.as_deref() != Some(claim_id.to_string().as_str()) {
                delete_generation_rows(&mut transaction, claim_id).await?;
            }
            sqlx::query("DELETE FROM bootstrap_claim WHERE singleton = 1")
                .execute(&mut *transaction)
                .await
                .map_err(store_error)?;
        }
        transaction.commit().await.map_err(store_error)
    }

    pub(crate) async fn discard_orphaned_generations_on_startup(&self) -> Result<(), ReplicaError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT active_generation FROM oll_meta WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(store_error)?;
        let prepared: Option<String> = sqlx::query_scalar(
            "SELECT candidate_generation FROM replica_identity_transition WHERE singleton = 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        let generations = sqlx::query_scalar::<_, String>(
            "SELECT generation_id FROM replica_generations ORDER BY generation_id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(store_error)?;
        for generation in generations {
            if active.as_deref() == Some(generation.as_str())
                || prepared.as_deref() == Some(generation.as_str())
            {
                continue;
            }
            delete_generation_rows(
                &mut transaction,
                parse_uuid_v4(&generation, "generation_id")?,
            )
            .await?;
        }
        sqlx::query(
            "DELETE FROM blob_chunks
             WHERE sha256 NOT IN (SELECT DISTINCT sha256 FROM binary_versions)",
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        sqlx::query(
            "DELETE FROM blobs
             WHERE sha256 NOT IN (SELECT DISTINCT sha256 FROM binary_versions)",
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }
}
