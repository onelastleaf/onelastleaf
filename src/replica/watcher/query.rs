use std::path::Path;

use uuid::Uuid;

use crate::node::identity::NodeIdentity;

use super::{
    super::{
        BootstrapClaim, PeerBinding, ReplicaError,
        model::absolute_to_namespace,
        snapshot,
        types::{ActiveReplica, OperationRecord, ReplicaStatus},
    },
    types::{DocumentInspection, ReplicaRuntime},
};

impl ReplicaRuntime {
    pub async fn status(&self) -> ReplicaStatus {
        self.state
            .read()
            .await
            .as_ref()
            .map_or(ReplicaStatus::Uninitialized, ActiveReplica::status)
    }

    pub(crate) async fn bind_sync_peer(
        &self,
        identity: &NodeIdentity,
        connect_target: Option<&str>,
    ) -> Result<(), ReplicaError> {
        self.store.bind_sync_peer(identity, connect_target).await
    }

    pub(crate) async fn sync_peer_bindings(&self) -> Result<Vec<PeerBinding>, ReplicaError> {
        self.store.sync_peer_bindings().await
    }

    pub(crate) async fn acquire_bootstrap_claim(
        &self,
        claim: &BootstrapClaim,
    ) -> Result<bool, ReplicaError> {
        self.store.acquire_bootstrap_claim(claim).await
    }

    pub(crate) async fn release_bootstrap_claim(&self, claim_id: Uuid) -> Result<(), ReplicaError> {
        self.store.release_bootstrap_claim(claim_id).await
    }

    pub async fn inspect_document(
        &self,
        native_path: &Path,
    ) -> Result<DocumentInspection, ReplicaError> {
        let namespace = absolute_to_namespace(&self.root, native_path)?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let entry = replica
            .entry_at_path(&namespace)?
            .ok_or_else(|| ReplicaError::NotFound("managed document was not found".to_owned()))?;
        let document = entry.document().ok_or_else(|| {
            ReplicaError::InvalidArgument(
                "replica inspect path must name a text document".to_owned(),
            )
        })?;
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("document Loro object is missing".to_owned())
            })?;
        Ok(DocumentInspection {
            catalog_node_id: entry.catalog_node_id,
            catalog_revision: entry.catalog_revision,
            document_id: document.document_id,
            document_revision: object.revision,
            path: namespace,
            media_type: document.media_type.clone(),
            encoding: document.encoding.clone(),
            has_byte_order_mark: document.has_byte_order_mark,
            size_bytes: document.size_bytes,
        })
    }

    pub async fn list_operations(
        &self,
        native_path: &Path,
        limit: usize,
    ) -> Result<Vec<OperationRecord>, ReplicaError> {
        if limit == 0 {
            return Err(ReplicaError::InvalidArgument(
                "operation limit must be greater than zero".to_owned(),
            ));
        }
        let inspection = self.inspect_document(native_path).await?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        self.store
            .list_operations(replica.generation_id, inspection.document_id, limit)
            .await
    }

    pub async fn export_snapshot(
        &self,
        destination: &Path,
        correlation_id: &str,
    ) -> Result<(Uuid, Uuid), ReplicaError> {
        snapshot::export_runtime(self, destination, correlation_id).await
    }

    pub async fn import_snapshot(
        &self,
        source: &Path,
        correlation_id: &str,
    ) -> Result<(Uuid, Uuid), ReplicaError> {
        snapshot::import_runtime(self, source, correlation_id).await
    }
}
