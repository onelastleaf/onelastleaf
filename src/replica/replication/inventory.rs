use std::collections::BTreeMap;

use loro::VersionVector;

use super::{
    super::{ReplicaError, types::CatalogEntry, watcher::ReplicaRuntime},
    candidate_build::object_summary,
    export::export_object,
    types::{ExportedReplicaObject, ReplicaInventory, ReplicaObject},
};

impl ReplicaRuntime {
    pub(crate) async fn capture_replica_inventory(&self) -> Result<ReplicaInventory, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let replica = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        let state_token = self.store.active_state_token(replica.generation_id).await?;
        let mut objects = Vec::with_capacity(1 + replica.documents.len());
        objects.push(object_summary(
            ReplicaObject::Catalog,
            &replica.catalog_loro,
        )?);
        for (document_id, document) in &replica.documents {
            objects.push(object_summary(
                ReplicaObject::Document(*document_id),
                &document.loro,
            )?);
        }
        let mut blobs = BTreeMap::new();
        for version in replica
            .entries
            .values()
            .filter_map(CatalogEntry::binary)
            .flat_map(|binary| binary.versions.values())
        {
            match blobs.insert(version.sha256.clone(), version.size_bytes) {
                Some(size) if size != version.size_bytes => {
                    return Err(ReplicaError::CorruptStore(
                        "one retained blob hash has contradictory sizes".to_owned(),
                    ));
                }
                _ => {}
            }
        }
        Ok(ReplicaInventory {
            generation_id: replica.generation_id,
            state_token,
            replica_id: replica.replica_id,
            objects,
            blobs,
        })
    }

    pub(crate) async fn export_replica_updates(
        &self,
        object: ReplicaObject,
        from: &VersionVector,
    ) -> Result<ExportedReplicaObject, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        export_object(replica, object, from)
    }
}
