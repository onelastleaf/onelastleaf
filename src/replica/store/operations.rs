use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use super::{
    super::{
        ReplicaError,
        types::{OperationKind, OperationRecord, OperationSource, parse_uuid_v4},
    },
    ReplicaStore,
    support::store_error,
};

impl ReplicaStore {
    pub async fn list_operations(
        &self,
        generation_id: Uuid,
        document_id: Uuid,
        limit: usize,
    ) -> Result<Vec<OperationRecord>, ReplicaError> {
        let limit = i64::try_from(limit).map_err(|_| {
            ReplicaError::InvalidArgument("operation limit is too large".to_owned())
        })?;
        let rows = sqlx::query(
            "SELECT timestamp, operation_id, source, kind, catalog_node_id,
                    document_id, path_before, path_after, correlation_id
             FROM replica_operations
             WHERE generation_id = $1 AND document_id = $2
             ORDER BY timestamp DESC, operation_id DESC
             LIMIT $3",
        )
        .bind(generation_id.to_string())
        .bind(document_id.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(decode_operation).collect()
    }
}

fn decode_operation(row: sqlx::any::AnyRow) -> Result<OperationRecord, ReplicaError> {
    let timestamp = row.try_get::<String, _>("timestamp").map_err(store_error)?;
    Ok(OperationRecord {
        timestamp: time::OffsetDateTime::parse(&timestamp, &Rfc3339).map_err(|_| {
            ReplicaError::CorruptStore("operation timestamp is not RFC 3339".to_owned())
        })?,
        operation_id: row
            .try_get::<String, _>("operation_id")
            .map_err(store_error)?,
        source: OperationSource::parse(&row.try_get::<String, _>("source").map_err(store_error)?)?,
        kind: OperationKind::parse(&row.try_get::<String, _>("kind").map_err(store_error)?)?,
        catalog_node_id: parse_uuid_v4(
            &row.try_get::<String, _>("catalog_node_id")
                .map_err(store_error)?,
            "catalog_node_id",
        )?,
        document_id: parse_uuid_v4(
            &row.try_get::<String, _>("document_id")
                .map_err(store_error)?,
            "document_id",
        )?,
        path_before: row
            .try_get::<Option<String>, _>("path_before")
            .map_err(store_error)?,
        path_after: row
            .try_get::<Option<String>, _>("path_after")
            .map_err(store_error)?,
        correlation_id: row
            .try_get::<String, _>("correlation_id")
            .map_err(store_error)?,
    })
}
