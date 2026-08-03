use std::collections::BTreeMap;

use loro::VersionVector;

use super::{
    super::{ReplicaError, model::import_loro_doc, types::CatalogEntry, watcher::ReplicaRuntime},
    export::export_object,
    types::{BootstrapSource, ReplicaInventory, ReplicaObject, ReplicaObjectSummary},
};

impl ReplicaRuntime {
    pub(crate) async fn capture_bootstrap_source(&self) -> Result<BootstrapSource, ReplicaError> {
        let _coordinator = self.identities.commit_guard().await;
        let replica = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        let state_token = self.store.active_state_token(replica.generation_id).await?;
        let mut objects = Vec::with_capacity(1 + replica.documents.len());
        let mut payloads = BTreeMap::new();
        for object in std::iter::once(ReplicaObject::Catalog).chain(
            replica
                .documents
                .keys()
                .copied()
                .map(ReplicaObject::Document),
        ) {
            let exported = export_object(&replica, object, &VersionVector::default())?;
            let document = match object {
                ReplicaObject::Catalog => import_loro_doc(&replica.catalog_loro, 0)?,
                ReplicaObject::Document(document_id) => import_loro_doc(
                    &replica
                        .documents
                        .get(&document_id)
                        .expect("bootstrap object came from retained document keys")
                        .loro,
                    0,
                )?,
            };
            objects.push(ReplicaObjectSummary {
                object,
                version_vector: document.oplog_vv(),
                frontier: document.oplog_frontiers(),
            });
            payloads.insert(object, exported);
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
        Ok(BootstrapSource {
            inventory: ReplicaInventory {
                generation_id: replica.generation_id,
                state_token,
                replica_id: replica.replica_id,
                objects,
                blobs,
            },
            objects: payloads,
        })
    }
}
