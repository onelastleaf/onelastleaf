use loro::Container;

use crate::protocol::oll;

use super::{
    super::{ReplicaError, model::import_loro_doc, types::EntryData, watcher::ReplicaRuntime},
    catalog::{
        directory_metadata, directory_tree_node, invalid, node_metadata, required_document_path,
        required_entry,
    },
    crdt_read::{
        container_to_proto, resolve_value, validate_object_path, value_or_container_to_proto,
    },
};

impl ReplicaRuntime {
    pub async fn read_document(
        &self,
        request: oll::ReadDocumentRequest,
    ) -> Result<oll::ReadDocumentResponse, ReplicaError> {
        let path = required_document_path(request.path, "path")?;
        let projection = oll::DocumentProjection::try_from(request.projection)
            .unwrap_or(oll::DocumentProjection::Unspecified);
        if projection == oll::DocumentProjection::Unspecified {
            return Err(invalid("document projection must be specified"));
        }
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let entry = required_entry(replica, &path)?;
        let document = entry
            .document()
            .ok_or_else(|| invalid("read document path must name a managed text document"))?;
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
            })?;
        let doc = import_loro_doc(&object.loro, replica.loro_peer_id)?;
        let representation = match projection {
            oll::DocumentProjection::Content => {
                oll::document_snapshot::Representation::Content(doc.get_text("content").to_string())
            }
            oll::DocumentProjection::Crdt => oll::document_snapshot::Representation::Crdt(
                container_to_proto(Container::Map(doc.get_map("data")))?,
            ),
            oll::DocumentProjection::Unspecified => unreachable!(),
        };
        Ok(oll::ReadDocumentResponse {
            document: Some(oll::DocumentSnapshot {
                metadata: Some(node_metadata(replica, entry, &path)?),
                representation: Some(representation),
            }),
        })
    }

    pub async fn list_directory(
        &self,
        request: oll::ListDirectoryRequest,
    ) -> Result<oll::ListDirectoryResponse, ReplicaError> {
        let path = required_document_path(request.path, "path")?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let directory = directory_metadata(replica, &path)?;
        let paths = replica.projected_paths()?;
        let directory_id = if path == "/" {
            replica.root_catalog_node_id
        } else {
            required_entry(replica, &path)?.catalog_node_id
        };
        let mut entries = replica
            .entries
            .values()
            .filter(|entry| {
                !entry.deleted
                    && if request.recursive {
                        paths.get(&entry.catalog_node_id).is_some_and(|candidate| {
                            candidate.starts_with(&format!("{path}/"))
                                || (path == "/" && candidate.starts_with('/'))
                        })
                    } else {
                        entry.parent_catalog_node_id == directory_id
                    }
            })
            .map(|entry| {
                let path = paths.get(&entry.catalog_node_id).ok_or_else(|| {
                    ReplicaError::CorruptStore("visible catalog path is missing".to_owned())
                })?;
                node_metadata(replica, entry, path)
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by(|left, right| {
            left.path
                .as_ref()
                .map(|path| &path.value)
                .cmp(&right.path.as_ref().map(|path| &path.value))
        });
        Ok(oll::ListDirectoryResponse {
            directory: Some(directory),
            entries,
        })
    }

    pub async fn get_directory_tree(
        &self,
        request: oll::GetDirectoryTreeRequest,
    ) -> Result<oll::GetDirectoryTreeResponse, ReplicaError> {
        let path = required_document_path(request.root, "root")?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let root_id = if path == "/" {
            replica.root_catalog_node_id
        } else {
            let entry = required_entry(replica, &path)?;
            if !matches!(entry.data, EntryData::Directory) {
                return Err(invalid("directory-tree root must name a directory"));
            }
            entry.catalog_node_id
        };
        let paths = replica.projected_paths()?;
        Ok(oll::GetDirectoryTreeResponse {
            root: Some(directory_tree_node(replica, root_id, &path, &paths)?),
        })
    }

    pub async fn read_crdt(
        &self,
        request: oll::ReadCrdtRequest,
    ) -> Result<oll::ReadCrdtResponse, ReplicaError> {
        let path = required_document_path(request.document, "document")?;
        let object_path = request.object.unwrap_or_default();
        validate_object_path(&object_path)?;
        let state = self.state.read().await;
        let replica = state.as_ref().ok_or(ReplicaError::Uninitialized)?;
        let entry = required_entry(replica, &path)?;
        let document = entry
            .document()
            .ok_or_else(|| invalid("read CRDT path must name a managed text document"))?;
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
            })?;
        let doc = import_loro_doc(&object.loro, replica.loro_peer_id)?;
        let value = resolve_value(&doc, &object_path)?;
        Ok(oll::ReadCrdtResponse {
            revision: Some(oll::DocumentRevision {
                token: object.revision.to_vec(),
            }),
            value: Some(value_or_container_to_proto(value)?),
        })
    }
}
