use std::collections::{BTreeMap, BTreeSet, HashMap};

use loro::{
    Container, ExportMode, LoroCounter, LoroDoc, LoroList, LoroMap, LoroMovableList, LoroText,
    LoroTree, LoroValue, TextDelta, TreeID, TreeParentId, UpdateOptions, ValueOrContainer,
};
use prost::Message;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{node::logging::LogLevel, protocol::oll};

use super::{
    ReplicaError,
    classification::encode_text,
    model::{
        get_entry_record, import_loro_doc, new_loro_doc, parent_namespace_path, parse_tree_id,
        recompute_live_catalog_revisions, validate_name, write_entry_record,
    },
    store::RetainedCommit,
    types::{
        ActiveReplica, CatalogEntry, DocumentEntry, DocumentObject, EntryData, OperationKind,
        OperationRecord, OperationSource,
    },
    watcher::ReplicaRuntime,
};

const TREE_NODE_ID_KEY: &str = "\0oll_node_id";

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

    pub async fn commit_documents(
        &self,
        request: oll::CommitDocumentsRequest,
        source: OperationSource,
        correlation_id: &str,
    ) -> Result<oll::CommitDocumentsResponse, ReplicaError> {
        let started = std::time::Instant::now();
        let operation_id = request.operation_id.clone();
        let mutation_count = request.mutations.len();
        self.logger
            .emit(
                LogLevel::Info,
                "oll::replica",
                "document_commit_started",
                correlation_id,
                serde_json::json!({
                    "operation_id": &operation_id,
                    "source": source.as_str(),
                    "mutation_count": mutation_count,
                }),
            )
            .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        let result = self
            .commit_documents_inner(request, source, correlation_id)
            .await;
        let log_result = match &result {
            Ok(response) => self.logger.emit(
                LogLevel::Info,
                "oll::replica",
                "document_commit_completed",
                correlation_id,
                serde_json::json!({
                    "operation_id": &operation_id,
                    "source": source.as_str(),
                    "mutation_count": mutation_count,
                    "updated_node_count": response.updated_nodes.len(),
                    "duration_ms": api_elapsed_ms(started),
                }),
            ),
            Err(error) => self.logger.emit(
                if matches!(
                    error,
                    ReplicaError::InvalidArgument(_)
                        | ReplicaError::NotFound(_)
                        | ReplicaError::AlreadyExists(_)
                        | ReplicaError::RevisionConflict(_)
                ) {
                    LogLevel::Warn
                } else {
                    LogLevel::Error
                },
                "oll::replica",
                "document_commit_failed",
                correlation_id,
                serde_json::json!({
                    "operation_id": &operation_id,
                    "source": source.as_str(),
                    "mutation_count": mutation_count,
                    "error_code": error.code(),
                    "duration_ms": api_elapsed_ms(started),
                }),
            ),
        };
        if result.is_ok() {
            log_result.map_err(|error| ReplicaError::Internal(error.to_string()))?;
        }
        result
    }

    async fn commit_documents_inner(
        &self,
        request: oll::CommitDocumentsRequest,
        source: OperationSource,
        correlation_id: &str,
    ) -> Result<oll::CommitDocumentsResponse, ReplicaError> {
        validate_operation_id(&request.operation_id)?;
        if request.mutations.is_empty() {
            return Err(invalid(
                "document commit must contain at least one mutation",
            ));
        }

        let _coordinator = self.coordinator.lock().await;
        let current = self
            .state
            .read()
            .await
            .clone()
            .ok_or(ReplicaError::Uninitialized)?;
        if let Some(retained) = self
            .store
            .retained_commit(current.generation_id, &request.operation_id)
            .await?
        {
            let original = oll::CommitDocumentsRequest::decode(retained.request.as_slice())
                .map_err(|_| {
                    ReplicaError::CorruptStore(
                        "retained document commit request cannot be decoded".to_owned(),
                    )
                })?;
            if original != request {
                return Err(invalid(
                    "operation_id was already used for a different document commit",
                ));
            }
            self.recover_projection(Some(correlation_id)).await?;
            return oll::CommitDocumentsResponse::decode(retained.response.as_slice()).map_err(
                |_| {
                    ReplicaError::CorruptStore(
                        "retained document commit response cannot be decoded".to_owned(),
                    )
                },
            );
        }
        check_preconditions(&current, &request.preconditions)?;
        let retained_request = request.encode_to_vec();

        let before = current.clone();
        let before_paths = before.projected_paths()?;
        let mut replica = current;
        let catalog = import_loro_doc(&replica.catalog_loro, replica.loro_peer_id)?;
        catalog.set_next_commit_origin(source.as_str());
        let tree = catalog.get_tree("tree");
        let entries = catalog.get_map("entries");
        let mut documents = BTreeMap::<Uuid, LoroDoc>::new();
        let mut touched = BTreeSet::new();
        let mut body_touched = BTreeSet::new();
        let mut catalog_changed = false;
        let mut encoding_promotions = Vec::new();

        for mutation in request.mutations {
            let mutation = mutation
                .mutation
                .ok_or_else(|| invalid("document mutation must be specified"))?;
            apply_mutation(
                &mut replica,
                &catalog,
                &tree,
                &entries,
                &mut documents,
                &mut touched,
                &mut body_touched,
                &mut catalog_changed,
                mutation,
                source,
            )?;
        }

        for (document_id, doc) in documents {
            doc.commit();
            let loro = doc.export(ExportMode::Snapshot).map_err(|error| {
                ReplicaError::Internal(format!("cannot encode document Loro snapshot: {error}"))
            })?;
            replica
                .documents
                .insert(document_id, DocumentObject::new(document_id, loro));
            let text = doc.get_text("content").to_string();
            let entry = replica
                .entries
                .values_mut()
                .find(|entry| {
                    entry
                        .document()
                        .is_some_and(|document| document.document_id == document_id)
                })
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("edited document has no catalog entry".to_owned())
                })?;
            let EntryData::Document(document) = &mut entry.data else {
                unreachable!()
            };
            let old_size = document.size_bytes;
            let (bytes, promoted) =
                encode_text(&text, &document.encoding, document.has_byte_order_mark)?;
            if promoted {
                document.encoding = encoding_rs::UTF_8.name().to_owned();
                document.has_byte_order_mark = false;
                body_touched.insert(entry.catalog_node_id);
                encoding_promotions.push((entry.catalog_node_id, document.document_id));
            }
            let size_bytes = u64::try_from(bytes.len())
                .map_err(|_| invalid("encoded document size is too large"))?;
            document.size_bytes = size_bytes;
            if promoted || size_bytes != old_size {
                write_entry_record(&get_entry_record(&entries, entry.catalog_node_id)?, entry)?;
                catalog_changed = true;
            }
        }

        if catalog_changed {
            catalog.commit();
            replica.catalog_loro = catalog.export(ExportMode::Snapshot).map_err(|error| {
                ReplicaError::Internal(format!("cannot encode catalog Loro snapshot: {error}"))
            })?;
        }
        let after_paths = replica.projected_paths()?;
        recompute_live_catalog_revisions(&mut replica, &after_paths);
        let projection_paths = changed_projection_paths(
            &before,
            &replica,
            &before_paths,
            &after_paths,
            &mut touched,
            &body_touched,
        );
        if !projection_paths.is_empty() {
            replica.projection_generation = replica
                .projection_generation
                .checked_add(1)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("projection generation overflow".to_owned())
                })?;
        }
        let operations = document_operations(
            &before,
            &replica,
            &before_paths,
            &after_paths,
            &request.operation_id,
            source,
            correlation_id,
        );
        let response = oll::CommitDocumentsResponse {
            operation_id: request.operation_id.clone(),
            updated_nodes: touched
                .into_iter()
                .map(|id| updated_node(&replica, &before_paths, &after_paths, id))
                .collect::<Result<Vec<_>, _>>()?,
        };
        let retained = RetainedCommit {
            operation_id: request.operation_id,
            request: retained_request,
            response: response.encode_to_vec(),
        };
        self.store
            .save_active_commit(&replica, &[], &operations, &projection_paths, &retained)
            .await?;
        *self.state.write().await = Some(replica.clone());
        for (catalog_node_id, document_id) in encoding_promotions {
            self.logger
                .emit(
                    LogLevel::Info,
                    "oll::replica",
                    "document_encoding_promoted",
                    correlation_id,
                    serde_json::json!({
                        "operation_id": &retained.operation_id,
                        "catalog_node_id": catalog_node_id.to_string(),
                        "document_id": document_id.to_string(),
                        "encoding": "UTF-8",
                    }),
                )
                .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        }
        if !projection_paths.is_empty() {
            self.project_targeted(&replica, &projection_paths, correlation_id)
                .await?;
        }
        Ok(response)
    }
}

fn api_elapsed_ms(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn required_document_path(
    path: Option<oll::DocumentPath>,
    field: &'static str,
) -> Result<String, ReplicaError> {
    let path = path
        .ok_or_else(|| invalid(&format!("{field} must be specified")))?
        .value;
    validate_namespace_path(&path)?;
    Ok(path)
}

fn validate_namespace_path(path: &str) -> Result<(), ReplicaError> {
    if path == "/" {
        return Ok(());
    }
    if !path.starts_with('/') || path.ends_with('/') {
        return Err(invalid(
            "document path must be absolute without a trailing slash",
        ));
    }
    for segment in path[1..].split('/') {
        validate_name(segment)?;
    }
    Ok(())
}

fn required_entry<'a>(
    replica: &'a ActiveReplica,
    path: &str,
) -> Result<&'a CatalogEntry, ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root is not an ordinary catalog entry"));
    }
    replica
        .entry_at_path(path)?
        .ok_or_else(|| ReplicaError::NotFound("document path was not found".to_owned()))
}

fn directory_metadata(
    replica: &ActiveReplica,
    path: &str,
) -> Result<oll::NodeMetadata, ReplicaError> {
    if path == "/" {
        let mut hash = Sha256::new();
        hash.update(b"oll.catalog.root");
        hash.update(replica.root_catalog_node_id.as_bytes());
        return Ok(oll::NodeMetadata {
            path: Some(oll::DocumentPath {
                value: "/".to_owned(),
            }),
            kind: oll::NodeKind::Directory as i32,
            catalog_revision: Some(oll::CatalogRevision {
                token: hash.finalize().to_vec(),
            }),
            media_type: None,
            size_bytes: 0,
            node_id: Some(oll::CatalogNodeId {
                value: replica.root_catalog_node_id.to_string(),
            }),
            document_id: None,
            binary_id: None,
            document_revision: None,
            encoding: None,
            has_byte_order_mark: false,
        });
    }
    let entry = required_entry(replica, path)?;
    if !matches!(entry.data, EntryData::Directory) {
        return Err(invalid("path must name a directory"));
    }
    node_metadata(replica, entry, path)
}

fn node_metadata(
    replica: &ActiveReplica,
    entry: &CatalogEntry,
    path: &str,
) -> Result<oll::NodeMetadata, ReplicaError> {
    let mut metadata = oll::NodeMetadata {
        path: Some(oll::DocumentPath {
            value: path.to_owned(),
        }),
        kind: oll::NodeKind::Unspecified as i32,
        catalog_revision: Some(oll::CatalogRevision {
            token: entry.catalog_revision.to_vec(),
        }),
        media_type: None,
        size_bytes: 0,
        node_id: Some(oll::CatalogNodeId {
            value: entry.catalog_node_id.to_string(),
        }),
        document_id: None,
        binary_id: None,
        document_revision: None,
        encoding: None,
        has_byte_order_mark: false,
    };
    match &entry.data {
        EntryData::Directory => metadata.kind = oll::NodeKind::Directory as i32,
        EntryData::Document(document) => {
            let object = replica
                .documents
                .get(&document.document_id)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
                })?;
            metadata.kind = oll::NodeKind::Document as i32;
            metadata.media_type = Some(document.media_type.clone());
            metadata.size_bytes = document.size_bytes;
            metadata.document_id = Some(oll::DocumentId {
                value: document.document_id.to_string(),
            });
            metadata.document_revision = Some(oll::DocumentRevision {
                token: object.revision.to_vec(),
            });
            metadata.encoding = Some(document.encoding.clone());
            metadata.has_byte_order_mark = document.has_byte_order_mark;
        }
        EntryData::Binary(binary) => {
            let (_, version) = binary.winning_version().ok_or_else(|| {
                ReplicaError::CorruptStore("catalog binary has no winning version".to_owned())
            })?;
            metadata.kind = oll::NodeKind::Binary as i32;
            metadata.media_type = Some(binary.media_type.clone());
            metadata.size_bytes = version.size_bytes;
            metadata.binary_id = Some(oll::BinaryId {
                value: binary.binary_id.to_string(),
            });
        }
    }
    Ok(metadata)
}

fn directory_tree_node(
    replica: &ActiveReplica,
    id: Uuid,
    path: &str,
    paths: &HashMap<Uuid, String>,
) -> Result<oll::DirectoryTreeNode, ReplicaError> {
    let metadata = if id == replica.root_catalog_node_id {
        directory_metadata(replica, "/")?
    } else {
        let entry = replica.entries.get(&id).ok_or_else(|| {
            ReplicaError::CorruptStore("directory-tree entry is missing".to_owned())
        })?;
        node_metadata(replica, entry, path)?
    };
    let mut child_entries = replica
        .entries
        .values()
        .filter(|entry| !entry.deleted && entry.parent_catalog_node_id == id)
        .collect::<Vec<_>>();
    child_entries.sort_by_key(|entry| paths.get(&entry.catalog_node_id).cloned());
    let children = child_entries
        .into_iter()
        .map(|entry| {
            let child_path = paths.get(&entry.catalog_node_id).ok_or_else(|| {
                ReplicaError::CorruptStore("directory-tree child path is missing".to_owned())
            })?;
            if matches!(entry.data, EntryData::Directory) {
                directory_tree_node(replica, entry.catalog_node_id, child_path, paths)
            } else {
                Ok(oll::DirectoryTreeNode {
                    metadata: Some(node_metadata(replica, entry, child_path)?),
                    children: Vec::new(),
                })
            }
        })
        .collect::<Result<Vec<_>, ReplicaError>>()?;
    Ok(oll::DirectoryTreeNode {
        metadata: Some(metadata),
        children,
    })
}

fn invalid(message: &str) -> ReplicaError {
    ReplicaError::InvalidArgument(message.to_owned())
}

fn validate_operation_id(operation_id: &str) -> Result<(), ReplicaError> {
    if operation_id.is_empty() || operation_id.len() > 512 || operation_id.contains('\0') {
        Err(invalid(
            "operation_id must be non-empty, NUL-free, and at most 512 bytes",
        ))
    } else {
        Ok(())
    }
}

fn check_preconditions(
    replica: &ActiveReplica,
    preconditions: &[oll::CommitPrecondition],
) -> Result<(), ReplicaError> {
    for precondition in preconditions {
        let condition = precondition
            .condition
            .as_ref()
            .ok_or_else(|| invalid("commit precondition must be specified"))?;
        match condition {
            oll::commit_precondition::Condition::CatalogUnchanged(precondition) => {
                let id = api_uuid(
                    precondition
                        .catalog_node_id
                        .as_ref()
                        .map(|id| id.value.as_str()),
                    "catalog_node_id",
                )?;
                let expected = precondition
                    .unchanged_since
                    .as_ref()
                    .ok_or_else(|| invalid("catalog revision must be specified"))?;
                let actual = replica.entries.get(&id).filter(|entry| !entry.deleted);
                if actual.is_none_or(|entry| entry.catalog_revision.as_slice() != expected.token) {
                    return Err(ReplicaError::RevisionConflict(
                        "catalog revision precondition failed".to_owned(),
                    ));
                }
            }
            oll::commit_precondition::Condition::DocumentUnchanged(precondition) => {
                let id = api_uuid(
                    precondition
                        .document_id
                        .as_ref()
                        .map(|id| id.value.as_str()),
                    "document_id",
                )?;
                let expected = precondition
                    .unchanged_since
                    .as_ref()
                    .ok_or_else(|| invalid("document revision must be specified"))?;
                if replica
                    .documents
                    .get(&id)
                    .is_none_or(|document| document.revision.as_slice() != expected.token)
                {
                    return Err(ReplicaError::RevisionConflict(
                        "document revision precondition failed".to_owned(),
                    ));
                }
            }
            oll::commit_precondition::Condition::MustExist(path) => {
                validate_namespace_path(&path.value)?;
                if path.value != "/"
                    && replica
                        .entry_at_path(&path.value)?
                        .is_none_or(|entry| entry.deleted)
                {
                    return Err(ReplicaError::RevisionConflict(
                        "existence precondition failed".to_owned(),
                    ));
                }
            }
            oll::commit_precondition::Condition::MustNotExist(path) => {
                validate_namespace_path(&path.value)?;
                if path.value == "/"
                    || replica
                        .entry_at_path(&path.value)?
                        .is_some_and(|entry| !entry.deleted)
                {
                    return Err(ReplicaError::RevisionConflict(
                        "non-existence precondition failed".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_mutation(
    replica: &mut ActiveReplica,
    catalog: &LoroDoc,
    tree: &LoroTree,
    entries: &LoroMap,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    touched: &mut BTreeSet<Uuid>,
    body_touched: &mut BTreeSet<Uuid>,
    catalog_changed: &mut bool,
    mutation: oll::document_mutation::Mutation,
    source: OperationSource,
) -> Result<(), ReplicaError> {
    match mutation {
        oll::document_mutation::Mutation::CreateDirectory(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            create_entry(
                replica, tree, entries, &path, None, source, documents, touched,
            )?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::CreateDocument(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            if mutation.media_type.is_empty() || mutation.media_type.contains('\0') {
                return Err(invalid(
                    "document media_type must be non-empty and NUL-free",
                ));
            }
            create_entry(
                replica,
                tree,
                entries,
                &path,
                Some((mutation.content, mutation.media_type)),
                source,
                documents,
                touched,
            )?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::ReplaceDocument(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            let (catalog_node_id, document_id, doc) = editable_document(replica, documents, &path)?;
            doc.set_next_commit_origin(source.as_str());
            doc.get_text("content")
                .update(&mutation.content, UpdateOptions::default())
                .map_err(|_| invalid("document text replacement exceeded its update budget"))?;
            if let Some(media_type) = mutation.media_type {
                if media_type.is_empty() || media_type.contains('\0') {
                    return Err(invalid(
                        "document media_type must be non-empty and NUL-free",
                    ));
                }
                let entry = replica.entries.get_mut(&catalog_node_id).ok_or_else(|| {
                    ReplicaError::CorruptStore("edited catalog entry is missing".to_owned())
                })?;
                let EntryData::Document(document) = &mut entry.data else {
                    unreachable!()
                };
                document.media_type = media_type;
                write_entry_record(&get_entry_record(entries, catalog_node_id)?, entry)?;
                *catalog_changed = true;
            }
            let _ = document_id;
            touched.insert(catalog_node_id);
            body_touched.insert(catalog_node_id);
        }
        oll::document_mutation::Mutation::SpliceDocumentText(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            let (catalog_node_id, _, doc) = editable_document(replica, documents, &path)?;
            let index = usize_index(mutation.scalar_index, "scalar_index")?;
            let count = usize_index(mutation.delete_scalar_count, "delete_scalar_count")?;
            let content = doc.get_text("content");
            validate_range(index, count, content.len_unicode(), "document text range")?;
            doc.set_next_commit_origin(source.as_str());
            content
                .splice(index, count, &mutation.insert_text)
                .map_err(|_| invalid("document text splice range is invalid"))?;
            touched.insert(catalog_node_id);
            body_touched.insert(catalog_node_id);
        }
        oll::document_mutation::Mutation::DeleteNode(mutation) => {
            let path = required_document_path(mutation.path, "path")?;
            delete_entry(replica, tree, entries, &path, mutation.recursive, touched)?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::MoveNode(mutation) => {
            let source_path = required_document_path(mutation.source, "source")?;
            let destination = required_document_path(mutation.destination, "destination")?;
            move_entry(replica, tree, entries, &source_path, &destination, touched)?;
            *catalog_changed = true;
        }
        oll::document_mutation::Mutation::ApplyCrdtOperations(mutation) => {
            let path = required_document_path(mutation.document, "document")?;
            if mutation.operations.is_empty() {
                return Err(invalid("CRDT mutation must contain at least one operation"));
            }
            let (catalog_node_id, _, doc) = editable_document(replica, documents, &path)?;
            doc.set_next_commit_origin(source.as_str());
            for operation in mutation.operations {
                apply_crdt_operation(
                    &doc,
                    operation
                        .operation
                        .ok_or_else(|| invalid("CRDT operation must be specified"))?,
                )?;
            }
            touched.insert(catalog_node_id);
        }
    }
    let _ = catalog;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    path: &str,
    document: Option<(String, String)>,
    source: OperationSource,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root cannot be created"));
    }
    if replica.entry_at_path(path)?.is_some() {
        return Err(ReplicaError::AlreadyExists(
            "document path already exists".to_owned(),
        ));
    }
    let parent_path = parent_namespace_path(path)?;
    let parent_id = if parent_path == "/" {
        replica.root_catalog_node_id
    } else {
        let parent = required_entry(replica, parent_path)?;
        if !matches!(parent.data, EntryData::Directory) {
            return Err(invalid("new entry parent must be a directory"));
        }
        parent.catalog_node_id
    };
    let name = path
        .rsplit('/')
        .next()
        .ok_or_else(|| invalid("invalid path"))?;
    validate_name(name)?;
    reject_local_name_collision(replica, parent_id, name, None)?;
    let tree_id = if parent_id == replica.root_catalog_node_id {
        tree.create(TreeParentId::Root)
    } else {
        let parent = replica
            .entries
            .get(&parent_id)
            .ok_or_else(|| ReplicaError::CorruptStore("new entry parent is missing".to_owned()))?;
        tree.create(parse_tree_id(&parent.loro_tree_id)?)
    }
    .map_err(loro_failure)?;
    let catalog_node_id = Uuid::new_v4();
    tree.get_meta(tree_id)
        .map_err(loro_failure)?
        .insert("catalog_node_id", catalog_node_id.to_string())
        .map_err(loro_failure)?;
    let data = if let Some((content, media_type)) = document {
        let document_id = Uuid::new_v4();
        let doc = new_loro_doc(replica.loro_peer_id)?;
        doc.set_next_commit_origin(source.as_str());
        let _data = doc.get_map("data");
        doc.get_text("content")
            .update(&content, UpdateOptions::default())
            .map_err(|_| invalid("cannot initialize document text"))?;
        documents.insert(document_id, doc);
        EntryData::Document(DocumentEntry {
            document_id,
            media_type,
            encoding: encoding_rs::UTF_8.name().to_owned(),
            has_byte_order_mark: false,
            size_bytes: u64::try_from(content.len())
                .map_err(|_| invalid("document content is too large"))?,
        })
    } else {
        EntryData::Directory
    };
    let mut entry = CatalogEntry {
        catalog_node_id,
        parent_catalog_node_id: parent_id,
        loro_tree_id: tree_id.to_string(),
        name: name.to_owned(),
        deleted: false,
        catalog_revision: [0; 32],
        data,
    };
    entry.recompute_revision();
    let record = entries
        .insert_container(&catalog_node_id.to_string(), LoroMap::new())
        .map_err(loro_failure)?;
    write_entry_record(&record, &entry)?;
    replica.entries.insert(catalog_node_id, entry);
    touched.insert(catalog_node_id);
    Ok(())
}

fn editable_document(
    replica: &ActiveReplica,
    documents: &mut BTreeMap<Uuid, LoroDoc>,
    path: &str,
) -> Result<(Uuid, Uuid, LoroDoc), ReplicaError> {
    let entry = required_entry(replica, path)?;
    let document = entry
        .document()
        .ok_or_else(|| invalid("document mutation path must name a text document"))?;
    if let std::collections::btree_map::Entry::Vacant(slot) = documents.entry(document.document_id)
    {
        let object = replica
            .documents
            .get(&document.document_id)
            .ok_or_else(|| {
                ReplicaError::CorruptStore("catalog document has no Loro object".to_owned())
            })?;
        slot.insert(import_loro_doc(&object.loro, replica.loro_peer_id)?);
    }
    let doc = documents
        .get(&document.document_id)
        .cloned()
        .ok_or_else(|| ReplicaError::Internal("editable document is missing".to_owned()))?;
    Ok((entry.catalog_node_id, document.document_id, doc))
}

fn delete_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    path: &str,
    recursive: bool,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if path == "/" {
        return Err(invalid("replica root cannot be deleted"));
    }
    let target = required_entry(replica, path)?;
    let target_id = target.catalog_node_id;
    let paths = replica.projected_paths()?;
    let mut targets = paths
        .iter()
        .filter_map(|(id, candidate)| {
            (candidate == path || candidate.starts_with(&format!("{path}/"))).then_some(*id)
        })
        .collect::<Vec<_>>();
    if targets.len() > 1 && !recursive {
        return Err(invalid(
            "non-empty directory deletion requires recursive=true",
        ));
    }
    targets.sort_by_key(|id| {
        std::cmp::Reverse(
            paths
                .get(id)
                .map_or(0, |candidate| candidate.matches('/').count()),
        )
    });
    for id in targets {
        let entry = replica
            .entries
            .get_mut(&id)
            .ok_or_else(|| ReplicaError::CorruptStore("deletion target is missing".to_owned()))?;
        tree.delete(parse_tree_id(&entry.loro_tree_id)?)
            .map_err(loro_failure)?;
        entry.deleted = true;
        entry.recompute_revision();
        write_entry_record(&get_entry_record(entries, id)?, entry)?;
        touched.insert(id);
    }
    if !touched.contains(&target_id) {
        return Err(ReplicaError::Internal(
            "catalog deletion did not include its target".to_owned(),
        ));
    }
    Ok(())
}

fn move_entry(
    replica: &mut ActiveReplica,
    tree: &LoroTree,
    entries: &LoroMap,
    source: &str,
    destination: &str,
    touched: &mut BTreeSet<Uuid>,
) -> Result<(), ReplicaError> {
    if source == "/" || destination == "/" {
        return Err(invalid("replica root cannot be moved or replaced"));
    }
    if source == destination {
        return Ok(());
    }
    if replica.entry_at_path(destination)?.is_some() {
        return Err(ReplicaError::AlreadyExists(
            "move destination already exists".to_owned(),
        ));
    }
    if destination.starts_with(&format!("{source}/")) {
        return Err(invalid("catalog entry cannot be moved beneath itself"));
    }
    let target_id = required_entry(replica, source)?.catalog_node_id;
    let parent_path = parent_namespace_path(destination)?;
    let parent_id = if parent_path == "/" {
        replica.root_catalog_node_id
    } else {
        let parent = required_entry(replica, parent_path)?;
        if !matches!(parent.data, EntryData::Directory) {
            return Err(invalid("move destination parent must be a directory"));
        }
        parent.catalog_node_id
    };
    let name = destination
        .rsplit('/')
        .next()
        .ok_or_else(|| invalid("move destination is invalid"))?;
    validate_name(name)?;
    reject_local_name_collision(replica, parent_id, name, Some(target_id))?;
    let parent = if parent_id == replica.root_catalog_node_id {
        TreeParentId::Root
    } else {
        TreeParentId::Node(parse_tree_id(
            &replica
                .entries
                .get(&parent_id)
                .ok_or_else(|| ReplicaError::CorruptStore("move parent is missing".to_owned()))?
                .loro_tree_id,
        )?)
    };
    let entry = replica
        .entries
        .get_mut(&target_id)
        .ok_or_else(|| ReplicaError::CorruptStore("move target is missing".to_owned()))?;
    tree.mov(parse_tree_id(&entry.loro_tree_id)?, parent)
        .map_err(loro_failure)?;
    entry.parent_catalog_node_id = parent_id;
    entry.name = name.to_owned();
    entry.recompute_revision();
    write_entry_record(&get_entry_record(entries, target_id)?, entry)?;
    touched.insert(target_id);
    Ok(())
}

fn reject_local_name_collision(
    replica: &ActiveReplica,
    parent_id: Uuid,
    name: &str,
    except: Option<Uuid>,
) -> Result<(), ReplicaError> {
    let key = super::types::portable_name_key(name);
    if replica.entries.values().any(|entry| {
        !entry.deleted
            && entry.parent_catalog_node_id == parent_id
            && Some(entry.catalog_node_id) != except
            && super::types::portable_name_key(&entry.name) == key
    }) {
        Err(ReplicaError::AlreadyExists(
            "a sibling already has the same portable name".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn api_uuid(value: Option<&str>, field: &str) -> Result<Uuid, ReplicaError> {
    let value = value.ok_or_else(|| invalid(&format!("{field} must be specified")))?;
    let id = Uuid::parse_str(value)
        .map_err(|_| invalid(&format!("{field} must be a canonical UUID v4")))?;
    if id.get_version_num() != 4 || id.to_string() != value {
        return Err(invalid(&format!("{field} must be a canonical UUID v4")));
    }
    Ok(id)
}

fn usize_index(value: u64, field: &str) -> Result<usize, ReplicaError> {
    usize::try_from(value).map_err(|_| invalid(&format!("{field} is too large")))
}

fn validate_range(
    index: usize,
    count: usize,
    length: usize,
    field: &str,
) -> Result<(), ReplicaError> {
    if index > length || count > length.saturating_sub(index) {
        Err(invalid(&format!("{field} is out of bounds")))
    } else {
        Ok(())
    }
}

fn loro_failure(error: impl std::fmt::Display) -> ReplicaError {
    ReplicaError::InvalidArgument(format!(
        "Loro rejected the validated CRDT operation: {error}"
    ))
}

fn validate_object_path(path: &oll::CrdtObjectPath) -> Result<(), ReplicaError> {
    for segment in &path.segments {
        match segment
            .kind
            .as_ref()
            .ok_or_else(|| invalid("CRDT path segment must be specified"))?
        {
            oll::crdt_path_segment::Kind::MapKey(key) => validate_user_key(key)?,
            oll::crdt_path_segment::Kind::ListIndex(_) => {}
            oll::crdt_path_segment::Kind::TreeNodeId(node_id) => {
                validate_tree_node_id(node_id)?;
            }
        }
    }
    Ok(())
}

fn resolve_value(
    doc: &LoroDoc,
    path: &oll::CrdtObjectPath,
) -> Result<ValueOrContainer, ReplicaError> {
    validate_object_path(path)?;
    let mut value = ValueOrContainer::Container(Container::Map(doc.get_map("data")));
    for segment in &path.segments {
        let kind = segment.kind.as_ref().expect("validated above");
        value = match (value, kind) {
            (
                ValueOrContainer::Container(Container::Map(map)),
                oll::crdt_path_segment::Kind::MapKey(key),
            ) => map
                .get(key)
                .ok_or_else(|| invalid("CRDT map path key does not exist"))?,
            (
                ValueOrContainer::Container(Container::List(list)),
                oll::crdt_path_segment::Kind::ListIndex(index),
            ) => list
                .get(usize_index(*index, "list_index")?)
                .ok_or_else(|| invalid("CRDT list path index is out of bounds"))?,
            (
                ValueOrContainer::Container(Container::MovableList(list)),
                oll::crdt_path_segment::Kind::ListIndex(index),
            ) => list
                .get(usize_index(*index, "list_index")?)
                .ok_or_else(|| invalid("CRDT list path index is out of bounds"))?,
            (
                ValueOrContainer::Container(Container::Tree(tree)),
                oll::crdt_path_segment::Kind::TreeNodeId(node_id),
            ) => {
                let tree_id = tree_node_by_api_id(&tree, node_id)?;
                ValueOrContainer::Container(Container::Map(
                    tree.get_meta(tree_id).map_err(loro_failure)?,
                ))
            }
            _ => return Err(invalid("CRDT object path has a container-kind mismatch")),
        };
    }
    Ok(value)
}

fn resolve_container(
    doc: &LoroDoc,
    path: Option<oll::CrdtObjectPath>,
) -> Result<Container, ReplicaError> {
    let path = path.unwrap_or_default();
    match resolve_value(doc, &path)? {
        ValueOrContainer::Container(container) => Ok(container),
        ValueOrContainer::Value(_) => Err(invalid("CRDT operation target is a scalar")),
    }
}

fn value_or_container_to_proto(value: ValueOrContainer) -> Result<oll::CrdtValue, ReplicaError> {
    match value {
        ValueOrContainer::Value(value) => Ok(oll::CrdtValue {
            kind: Some(oll::crdt_value::Kind::Scalar(loro_scalar_to_proto(&value)?)),
        }),
        ValueOrContainer::Container(container) => container_to_proto(container),
    }
}

fn container_to_proto(container: Container) -> Result<oll::CrdtValue, ReplicaError> {
    let kind = match container {
        Container::Map(map) => {
            let mut entries = HashMap::new();
            let mut failure = None;
            map.for_each(|key, value| {
                if failure.is_some() || key == TREE_NODE_ID_KEY {
                    return;
                }
                match value_or_container_to_proto(value) {
                    Ok(value) => {
                        entries.insert(key.to_owned(), value);
                    }
                    Err(error) => failure = Some(error),
                }
            });
            if let Some(error) = failure {
                return Err(error);
            }
            oll::crdt_value::Kind::Map(oll::CrdtMap { entries })
        }
        Container::List(list) => {
            let mut values = Vec::with_capacity(list.len());
            for index in 0..list.len() {
                values.push(value_or_container_to_proto(list.get(index).ok_or_else(
                    || ReplicaError::CorruptStore("Loro list index disappeared".to_owned()),
                )?)?);
            }
            oll::crdt_value::Kind::List(oll::CrdtList {
                values,
                movable: false,
            })
        }
        Container::MovableList(list) => {
            let mut values = Vec::with_capacity(list.len());
            for index in 0..list.len() {
                values.push(value_or_container_to_proto(list.get(index).ok_or_else(
                    || ReplicaError::CorruptStore("Loro movable-list index disappeared".to_owned()),
                )?)?);
            }
            oll::crdt_value::Kind::List(oll::CrdtList {
                values,
                movable: true,
            })
        }
        Container::Text(text) => oll::crdt_value::Kind::Text(text_to_proto(&text)?),
        Container::Tree(tree) => oll::crdt_value::Kind::Tree(tree_to_proto(&tree)?),
        Container::Counter(counter) => oll::crdt_value::Kind::Counter(oll::CrdtCounter {
            value: counter.get(),
        }),
        Container::Unknown(_) => {
            return Err(ReplicaError::CorruptStore(
                "document contains an unknown Loro container".to_owned(),
            ));
        }
    };
    Ok(oll::CrdtValue { kind: Some(kind) })
}

fn text_to_proto(text: &LoroText) -> Result<oll::CrdtText, ReplicaError> {
    let mut offset = 0_u64;
    let mut marks = Vec::new();
    for delta in text.to_delta() {
        let TextDelta::Insert { insert, attributes } = delta else {
            return Err(ReplicaError::CorruptStore(
                "materialized Loro text contains a non-insert delta".to_owned(),
            ));
        };
        let length = u64::try_from(insert.chars().count())
            .map_err(|_| ReplicaError::Internal("text length overflow".to_owned()))?;
        if let Some(attributes) = attributes {
            let mut attributes = attributes.into_iter().collect::<Vec<_>>();
            attributes.sort_by(|left, right| left.0.cmp(&right.0));
            for (name, value) in attributes {
                marks.push(oll::CrdtTextMark {
                    start_scalar: offset,
                    end_scalar: offset
                        .checked_add(length)
                        .ok_or_else(|| ReplicaError::Internal("text range overflow".to_owned()))?,
                    name,
                    value: Some(loro_scalar_to_proto(&value)?),
                });
            }
        }
        offset = offset
            .checked_add(length)
            .ok_or_else(|| ReplicaError::Internal("text length overflow".to_owned()))?;
    }
    Ok(oll::CrdtText {
        text: text.to_string(),
        marks,
    })
}

fn tree_to_proto(tree: &LoroTree) -> Result<oll::CrdtTree, ReplicaError> {
    let ids = tree_api_ids(tree)?;
    let mut nodes = Vec::new();
    for node in tree.get_nodes(false) {
        let node_id = ids.get(&node.id).ok_or_else(|| {
            ReplicaError::CorruptStore("tree node has no stable API identity".to_owned())
        })?;
        let parent_id = match node.parent {
            TreeParentId::Root => None,
            TreeParentId::Node(parent) => Some(
                ids.get(&parent)
                    .ok_or_else(|| {
                        ReplicaError::CorruptStore(
                            "tree node parent has no stable API identity".to_owned(),
                        )
                    })?
                    .clone(),
            ),
            TreeParentId::Deleted | TreeParentId::Unexist => {
                return Err(ReplicaError::CorruptStore(
                    "live tree node has an invalid parent".to_owned(),
                ));
            }
        };
        let metadata = tree.get_meta(node.id).map_err(loro_failure)?;
        let mut values = HashMap::new();
        let mut failure = None;
        metadata.for_each(|key, value| {
            if key == TREE_NODE_ID_KEY || failure.is_some() {
                return;
            }
            let ValueOrContainer::Value(value) = value else {
                failure = Some(ReplicaError::CorruptStore(
                    "tree metadata contains a child container".to_owned(),
                ));
                return;
            };
            match loro_scalar_to_proto(&value) {
                Ok(value) => {
                    values.insert(key.to_owned(), value);
                }
                Err(error) => failure = Some(error),
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        nodes.push(oll::CrdtTreeNode {
            node_id: node_id.clone(),
            parent_id,
            index_in_parent: Some(
                u64::try_from(node.index)
                    .map_err(|_| ReplicaError::Internal("tree index overflow".to_owned()))?,
            ),
            metadata: values,
        });
    }
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(oll::CrdtTree { nodes })
}

fn loro_scalar_to_proto(value: &LoroValue) -> Result<oll::CrdtScalar, ReplicaError> {
    let kind = match value {
        LoroValue::Null => {
            oll::crdt_scalar::Kind::NullValue(prost_types::NullValue::NullValue as i32)
        }
        LoroValue::Bool(value) => oll::crdt_scalar::Kind::BoolValue(*value),
        LoroValue::I64(value) => oll::crdt_scalar::Kind::IntegerValue(*value),
        LoroValue::Double(value) => oll::crdt_scalar::Kind::NumberValue(*value),
        LoroValue::String(value) => oll::crdt_scalar::Kind::StringValue(value.as_ref().to_owned()),
        LoroValue::Binary(value) => oll::crdt_scalar::Kind::BytesValue(value.as_slice().to_vec()),
        LoroValue::List(_) | LoroValue::Map(_) | LoroValue::Container(_) => {
            return Err(ReplicaError::CorruptStore(
                "CRDT scalar position contains a non-scalar Loro value".to_owned(),
            ));
        }
    };
    Ok(oll::CrdtScalar { kind: Some(kind) })
}

fn proto_scalar_to_loro(value: oll::CrdtScalar) -> Result<LoroValue, ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT scalar kind must be specified"))?
    {
        oll::crdt_scalar::Kind::BoolValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::IntegerValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::NumberValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::StringValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::BytesValue(value) => Ok(value.into()),
        oll::crdt_scalar::Kind::NullValue(value)
            if prost_types::NullValue::try_from(value).ok()
                == Some(prost_types::NullValue::NullValue) =>
        {
            Ok(LoroValue::Null)
        }
        oll::crdt_scalar::Kind::NullValue(_) => {
            Err(invalid("CRDT null scalar has an invalid enum value"))
        }
    }
}

fn apply_crdt_operation(
    doc: &LoroDoc,
    operation: oll::crdt_operation::Operation,
) -> Result<(), ReplicaError> {
    match operation {
        oll::crdt_operation::Operation::MapSet(operation) => {
            validate_user_key(&operation.key)?;
            let value = operation
                .value
                .ok_or_else(|| invalid("map-set value must be specified"))?;
            let Container::Map(map) = resolve_container(doc, operation.target)? else {
                return Err(invalid("map-set target is not a map"));
            };
            set_map_value(&map, &operation.key, value)
        }
        oll::crdt_operation::Operation::MapDelete(operation) => {
            validate_user_key(&operation.key)?;
            let Container::Map(map) = resolve_container(doc, operation.target)? else {
                return Err(invalid("map-delete target is not a map"));
            };
            map.delete(&operation.key).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::ListInsert(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let target = resolve_container(doc, operation.target)?;
            match target {
                Container::List(list) => {
                    if index > list.len() {
                        return Err(invalid("list-insert index is out of bounds"));
                    }
                    for (offset, value) in operation.values.into_iter().enumerate() {
                        insert_list_value(&list, index + offset, value)?;
                    }
                    Ok(())
                }
                Container::MovableList(list) => {
                    if index > list.len() {
                        return Err(invalid("list-insert index is out of bounds"));
                    }
                    for (offset, value) in operation.values.into_iter().enumerate() {
                        insert_movable_value(&list, index + offset, value)?;
                    }
                    Ok(())
                }
                _ => Err(invalid("list-insert target is not a list")),
            }
        }
        oll::crdt_operation::Operation::ListDelete(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let count = usize_index(operation.count, "list count")?;
            match resolve_container(doc, operation.target)? {
                Container::List(list) => {
                    validate_range(index, count, list.len(), "list-delete range")?;
                    list.delete(index, count).map_err(loro_failure)
                }
                Container::MovableList(list) => {
                    validate_range(index, count, list.len(), "list-delete range")?;
                    list.delete(index, count).map_err(loro_failure)
                }
                _ => Err(invalid("list-delete target is not a list")),
            }
        }
        oll::crdt_operation::Operation::ListMove(operation) => {
            let index = usize_index(operation.index, "list index")?;
            let count = usize_index(operation.count, "list count")?;
            let destination = usize_index(operation.destination, "list destination")?;
            let Container::MovableList(list) = resolve_container(doc, operation.target)? else {
                return Err(invalid("list-move target is not a movable list"));
            };
            validate_range(index, count, list.len(), "list-move range")?;
            if destination > list.len().saturating_sub(count) {
                return Err(invalid("list-move destination is out of bounds"));
            }
            if destination < index {
                for offset in 0..count {
                    list.mov(index + offset, destination + offset)
                        .map_err(loro_failure)?;
                }
            } else if destination > index {
                for offset in (0..count).rev() {
                    list.mov(index + offset, destination + offset)
                        .map_err(loro_failure)?;
                }
            }
            Ok(())
        }
        oll::crdt_operation::Operation::TextInsert(operation) => {
            let index = usize_index(operation.scalar_index, "text scalar index")?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-insert target is not text"));
            };
            if index > text.len_unicode() {
                return Err(invalid("text-insert index is out of bounds"));
            }
            text.insert(index, &operation.text).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextDelete(operation) => {
            let index = usize_index(operation.scalar_index, "text scalar index")?;
            let count = usize_index(operation.scalar_count, "text scalar count")?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-delete target is not text"));
            };
            validate_range(index, count, text.len_unicode(), "text-delete range")?;
            text.delete(index, count).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextMark(operation) => {
            let start = usize_index(operation.start_scalar, "text mark start")?;
            let end = usize_index(operation.end_scalar, "text mark end")?;
            validate_user_key(&operation.name)?;
            let value = proto_scalar_to_loro(
                operation
                    .value
                    .ok_or_else(|| invalid("text-mark value must be specified"))?,
            )?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-mark target is not text"));
            };
            if end < start {
                return Err(invalid("text-mark range is reversed"));
            }
            validate_range(start, end - start, text.len_unicode(), "text-mark range")?;
            text.mark(start..end, &operation.name, value)
                .map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TextUnmark(operation) => {
            let start = usize_index(operation.start_scalar, "text unmark start")?;
            let end = usize_index(operation.end_scalar, "text unmark end")?;
            validate_user_key(&operation.name)?;
            let Container::Text(text) = resolve_container(doc, operation.target)? else {
                return Err(invalid("text-unmark target is not text"));
            };
            if end < start {
                return Err(invalid("text-unmark range is reversed"));
            }
            validate_range(start, end - start, text.len_unicode(), "text-unmark range")?;
            text.unmark(start..end, &operation.name)
                .map_err(loro_failure)
        }
        oll::crdt_operation::Operation::CounterIncrement(operation) => {
            if !operation.delta.is_finite() {
                return Err(invalid("counter increment must be finite"));
            }
            let Container::Counter(counter) = resolve_container(doc, operation.target)? else {
                return Err(invalid("counter-increment target is not a counter"));
            };
            counter.increment(operation.delta).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeCreateNode(operation) => {
            apply_tree_create(doc, operation)
        }
        oll::crdt_operation::Operation::TreeDeleteNode(operation) => {
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-delete target is not a tree"));
            };
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            tree.delete(node).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeMoveNode(operation) => {
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-move target is not a tree"));
            };
            tree.enable_fractional_index(0);
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            let parent = operation
                .parent_id
                .as_deref()
                .map(|id| tree_node_by_api_id(&tree, id).map(TreeParentId::Node))
                .transpose()?
                .unwrap_or(TreeParentId::Root);
            let index = usize_index(operation.index, "tree index")?;
            let children = tree_child_count(&tree, parent, "tree-move parent")?;
            if index > children {
                return Err(invalid("tree-move index is out of bounds"));
            }
            tree.mov_to(node, parent, index).map_err(loro_failure)
        }
        oll::crdt_operation::Operation::TreeSetMetadata(operation) => {
            validate_user_key(&operation.key)?;
            let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
                return Err(invalid("tree-metadata target is not a tree"));
            };
            let node = tree_node_by_api_id(&tree, &operation.node_id)?;
            let metadata = tree.get_meta(node).map_err(loro_failure)?;
            match operation.value {
                Some(value) => metadata
                    .insert(&operation.key, proto_scalar_to_loro(value)?)
                    .map_err(loro_failure),
                None => metadata.delete(&operation.key).map_err(loro_failure),
            }
        }
    }
}

fn set_map_value(map: &LoroMap, key: &str, value: oll::CrdtValue) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => map
            .insert(key, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = map
                .insert_container(key, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = map
                .insert_container(key, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = map
                .insert_container(key, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = map
                .insert_container(key, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = map
                .insert_container(key, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = map
                .insert_container(key, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}

fn insert_list_value(
    list: &LoroList,
    index: usize,
    value: oll::CrdtValue,
) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => list
            .insert(index, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = list
                .insert_container(index, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = list
                .insert_container(index, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = list
                .insert_container(index, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = list
                .insert_container(index, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = list
                .insert_container(index, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = list
                .insert_container(index, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}

fn insert_movable_value(
    list: &LoroMovableList,
    index: usize,
    value: oll::CrdtValue,
) -> Result<(), ReplicaError> {
    match value
        .kind
        .ok_or_else(|| invalid("CRDT value kind must be specified"))?
    {
        oll::crdt_value::Kind::Scalar(value) => list
            .insert(index, proto_scalar_to_loro(value)?)
            .map_err(loro_failure),
        oll::crdt_value::Kind::Text(value) => {
            let child = list
                .insert_container(index, LoroText::new())
                .map_err(loro_failure)?;
            populate_text(&child, value)
        }
        oll::crdt_value::Kind::List(value) if value.movable => {
            let child = list
                .insert_container(index, LoroMovableList::new())
                .map_err(loro_failure)?;
            populate_movable_list(&child, value.values)
        }
        oll::crdt_value::Kind::List(value) => {
            let child = list
                .insert_container(index, LoroList::new())
                .map_err(loro_failure)?;
            populate_list(&child, value.values)
        }
        oll::crdt_value::Kind::Map(value) => {
            let child = list
                .insert_container(index, LoroMap::new())
                .map_err(loro_failure)?;
            populate_map(&child, value)
        }
        oll::crdt_value::Kind::Tree(value) => {
            let child = list
                .insert_container(index, LoroTree::new())
                .map_err(loro_failure)?;
            populate_tree(&child, value)
        }
        oll::crdt_value::Kind::Counter(value) => {
            if !value.value.is_finite() {
                return Err(invalid("counter value must be finite"));
            }
            let child = list
                .insert_container(index, LoroCounter::new())
                .map_err(loro_failure)?;
            child.increment(value.value).map_err(loro_failure)
        }
    }
}

fn populate_map(map: &LoroMap, value: oll::CrdtMap) -> Result<(), ReplicaError> {
    let mut entries = value.entries.into_iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in entries {
        validate_user_key(&key)?;
        set_map_value(map, &key, value)?;
    }
    Ok(())
}

fn populate_list(list: &LoroList, values: Vec<oll::CrdtValue>) -> Result<(), ReplicaError> {
    for (index, value) in values.into_iter().enumerate() {
        insert_list_value(list, index, value)?;
    }
    Ok(())
}

fn populate_movable_list(
    list: &LoroMovableList,
    values: Vec<oll::CrdtValue>,
) -> Result<(), ReplicaError> {
    for (index, value) in values.into_iter().enumerate() {
        insert_movable_value(list, index, value)?;
    }
    Ok(())
}

fn populate_text(text: &LoroText, value: oll::CrdtText) -> Result<(), ReplicaError> {
    text.insert(0, &value.text).map_err(loro_failure)?;
    let length = value.text.chars().count();
    let mut marks = value.marks;
    marks.sort_by(|left, right| {
        (left.start_scalar, left.end_scalar, &left.name).cmp(&(
            right.start_scalar,
            right.end_scalar,
            &right.name,
        ))
    });
    for mark in marks {
        validate_user_key(&mark.name)?;
        let start = usize_index(mark.start_scalar, "text mark start")?;
        let end = usize_index(mark.end_scalar, "text mark end")?;
        if end < start {
            return Err(invalid("text mark range is reversed"));
        }
        validate_range(start, end - start, length, "text mark range")?;
        text.mark(
            start..end,
            &mark.name,
            proto_scalar_to_loro(
                mark.value
                    .ok_or_else(|| invalid("text mark value must be specified"))?,
            )?,
        )
        .map_err(loro_failure)?;
    }
    Ok(())
}

fn populate_tree(tree: &LoroTree, value: oll::CrdtTree) -> Result<(), ReplicaError> {
    tree.enable_fractional_index(0);
    let mut pending = value.nodes;
    let mut seen = BTreeSet::new();
    for node in &pending {
        validate_tree_node_id(&node.node_id)?;
        if !seen.insert(node.node_id.clone()) {
            return Err(invalid("CRDT tree repeats a node_id"));
        }
    }
    let mut created = HashMap::<String, TreeID>::new();
    while !pending.is_empty() {
        pending.sort_by(|left, right| {
            (
                left.parent_id.as_deref(),
                left.index_in_parent,
                left.node_id.as_str(),
            )
                .cmp(&(
                    right.parent_id.as_deref(),
                    right.index_in_parent,
                    right.node_id.as_str(),
                ))
        });
        let mut progress = false;
        let mut remaining = Vec::new();
        for node in pending {
            let parent = match node.parent_id.as_deref() {
                None => Some(TreeParentId::Root),
                Some(parent) => created.get(parent).copied().map(TreeParentId::Node),
            };
            let Some(parent) = parent else {
                remaining.push(node);
                continue;
            };
            let index = usize_index(
                node.index_in_parent
                    .ok_or_else(|| invalid("tree node index_in_parent must be specified"))?,
                "tree node index",
            )?;
            let child_count = tree_child_count(tree, parent, "tree node parent")?;
            if index > child_count {
                return Err(invalid("tree node index is out of bounds"));
            }
            let tree_id = tree.create_at(parent, index).map_err(loro_failure)?;
            write_tree_node_metadata(tree, tree_id, &node.node_id, node.metadata)?;
            created.insert(node.node_id, tree_id);
            progress = true;
        }
        if !progress {
            return Err(invalid(
                "CRDT tree contains a missing parent or parent cycle",
            ));
        }
        pending = remaining;
    }
    Ok(())
}

fn apply_tree_create(doc: &LoroDoc, operation: oll::TreeCreateNode) -> Result<(), ReplicaError> {
    validate_tree_node_id(&operation.node_id)?;
    let Container::Tree(tree) = resolve_container(doc, operation.target)? else {
        return Err(invalid("tree-create target is not a tree"));
    };
    tree.enable_fractional_index(0);
    if tree_api_ids(&tree)?
        .values()
        .any(|node_id| node_id == &operation.node_id)
    {
        return Err(ReplicaError::AlreadyExists(
            "tree node_id already exists".to_owned(),
        ));
    }
    let parent = operation
        .parent_id
        .as_deref()
        .map(|id| tree_node_by_api_id(&tree, id).map(TreeParentId::Node))
        .transpose()?
        .unwrap_or(TreeParentId::Root);
    let index = usize_index(operation.index, "tree index")?;
    let child_count = tree_child_count(&tree, parent, "tree-create parent")?;
    if index > child_count {
        return Err(invalid("tree-create index is out of bounds"));
    }
    let tree_id = tree.create_at(parent, index).map_err(loro_failure)?;
    write_tree_node_metadata(&tree, tree_id, &operation.node_id, operation.metadata)
}

fn write_tree_node_metadata(
    tree: &LoroTree,
    tree_id: TreeID,
    node_id: &str,
    metadata: HashMap<String, oll::CrdtScalar>,
) -> Result<(), ReplicaError> {
    let map = tree.get_meta(tree_id).map_err(loro_failure)?;
    map.insert(TREE_NODE_ID_KEY, node_id)
        .map_err(loro_failure)?;
    let mut metadata = metadata.into_iter().collect::<Vec<_>>();
    metadata.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in metadata {
        validate_user_key(&key)?;
        map.insert(&key, proto_scalar_to_loro(value)?)
            .map_err(loro_failure)?;
    }
    Ok(())
}

fn tree_api_ids(tree: &LoroTree) -> Result<HashMap<TreeID, String>, ReplicaError> {
    let mut ids = HashMap::new();
    let mut unique = BTreeSet::new();
    for tree_id in tree.nodes() {
        if tree.is_node_deleted(&tree_id).map_err(loro_failure)? {
            continue;
        }
        let metadata = tree.get_meta(tree_id).map_err(loro_failure)?;
        let Some(ValueOrContainer::Value(LoroValue::String(node_id))) =
            metadata.get(TREE_NODE_ID_KEY)
        else {
            return Err(ReplicaError::CorruptStore(
                "tree node has no stable API identity".to_owned(),
            ));
        };
        let node_id = node_id.as_ref().to_owned();
        validate_tree_node_id(&node_id).map_err(|_| {
            ReplicaError::CorruptStore("tree node has an invalid stable API identity".to_owned())
        })?;
        if !unique.insert(node_id.clone()) {
            return Err(ReplicaError::CorruptStore(
                "tree repeats a stable API node identity".to_owned(),
            ));
        }
        ids.insert(tree_id, node_id);
    }
    Ok(ids)
}

fn tree_node_by_api_id(tree: &LoroTree, node_id: &str) -> Result<TreeID, ReplicaError> {
    validate_tree_node_id(node_id)?;
    tree_api_ids(tree)?
        .into_iter()
        .find_map(|(tree_id, candidate)| (candidate == node_id).then_some(tree_id))
        .ok_or_else(|| invalid("tree node_id does not exist"))
}

fn tree_child_count(
    tree: &LoroTree,
    parent: TreeParentId,
    field: &str,
) -> Result<usize, ReplicaError> {
    match parent {
        TreeParentId::Root => Ok(tree.children_num(parent).unwrap_or(0)),
        TreeParentId::Node(node) if tree.contains(node) => {
            if tree.is_node_deleted(&node).map_err(loro_failure)? {
                Err(invalid(&format!("{field} is deleted")))
            } else {
                Ok(tree.children_num(parent).unwrap_or(0))
            }
        }
        TreeParentId::Node(_) | TreeParentId::Deleted | TreeParentId::Unexist => {
            Err(invalid(&format!("{field} does not exist")))
        }
    }
}

fn validate_tree_node_id(node_id: &str) -> Result<(), ReplicaError> {
    if node_id.is_empty() || node_id.len() > 512 || node_id.contains('\0') {
        Err(invalid(
            "tree node_id must be non-empty, NUL-free, and at most 512 bytes",
        ))
    } else {
        Ok(())
    }
}

fn validate_user_key(key: &str) -> Result<(), ReplicaError> {
    if key == TREE_NODE_ID_KEY || key.contains('\0') {
        Err(invalid("CRDT map and metadata keys must be NUL-free"))
    } else {
        Ok(())
    }
}

fn changed_projection_paths(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    touched: &mut BTreeSet<Uuid>,
    body_touched: &BTreeSet<Uuid>,
) -> Vec<String> {
    let ids = before
        .entries
        .keys()
        .chain(after.entries.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut paths = BTreeSet::new();
    for id in ids {
        if before_paths.get(&id) != after_paths.get(&id) {
            touched.insert(id);
            if let Some(path) = before_paths.get(&id) {
                paths.insert(path.clone());
            }
            if let Some(path) = after_paths.get(&id) {
                paths.insert(path.clone());
            }
        }
    }
    for id in body_touched {
        if let Some(path) = before_paths.get(id) {
            paths.insert(path.clone());
        }
        if let Some(path) = after_paths.get(id) {
            paths.insert(path.clone());
        }
    }
    paths.into_iter().collect()
}

fn document_operations(
    before: &ActiveReplica,
    after: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    operation_id: &str,
    source: OperationSource,
    correlation_id: &str,
) -> Vec<OperationRecord> {
    let document_ids = before
        .documents
        .keys()
        .chain(after.documents.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let timestamp = time::OffsetDateTime::now_utc();
    let mut records = Vec::new();
    for document_id in document_ids {
        let before_entry = before.entries.values().find(|entry| {
            entry
                .document()
                .is_some_and(|document| document.document_id == document_id)
        });
        let after_entry = after.entries.values().find(|entry| {
            entry
                .document()
                .is_some_and(|document| document.document_id == document_id)
        });
        let before_live = before_entry.filter(|entry| !entry.deleted);
        let after_live = after_entry.filter(|entry| !entry.deleted);
        let path_before =
            before_live.and_then(|entry| before_paths.get(&entry.catalog_node_id).cloned());
        let path_after =
            after_live.and_then(|entry| after_paths.get(&entry.catalog_node_id).cloned());
        let revision_changed = before.documents.get(&document_id).map(|doc| doc.revision)
            != after.documents.get(&document_id).map(|doc| doc.revision);
        let catalog_changed = before_entry.map(|entry| entry.catalog_revision)
            != after_entry.map(|entry| entry.catalog_revision);
        let kind = match (before_live, after_live) {
            (None, Some(_)) => Some(OperationKind::Create),
            (Some(_), None) => Some(OperationKind::Delete),
            (Some(_), Some(_)) if path_before != path_after => Some(OperationKind::Move),
            (Some(_), Some(_)) if revision_changed || catalog_changed => {
                Some(OperationKind::Update)
            }
            _ => None,
        };
        let Some(kind) = kind else {
            continue;
        };
        let catalog_node_id = after_entry
            .or(before_entry)
            .map(|entry| entry.catalog_node_id)
            .expect("a changed document has a catalog entry");
        records.push(OperationRecord {
            timestamp,
            operation_id: operation_id.to_owned(),
            source,
            kind,
            catalog_node_id,
            document_id,
            path_before,
            path_after,
            correlation_id: correlation_id.to_owned(),
        });
    }
    records
}

fn updated_node(
    replica: &ActiveReplica,
    before_paths: &HashMap<Uuid, String>,
    after_paths: &HashMap<Uuid, String>,
    id: Uuid,
) -> Result<oll::UpdatedNode, ReplicaError> {
    let entry = replica
        .entries
        .get(&id)
        .ok_or_else(|| ReplicaError::CorruptStore("updated catalog entry is missing".to_owned()))?;
    let (document_id, document_revision, binary_id) = match &entry.data {
        EntryData::Directory => (None, None, None),
        EntryData::Document(document) => {
            let object = replica
                .documents
                .get(&document.document_id)
                .ok_or_else(|| {
                    ReplicaError::CorruptStore("updated document object is missing".to_owned())
                })?;
            (
                Some(oll::DocumentId {
                    value: document.document_id.to_string(),
                }),
                Some(oll::DocumentRevision {
                    token: object.revision.to_vec(),
                }),
                None,
            )
        }
        EntryData::Binary(binary) => (
            None,
            None,
            Some(oll::BinaryId {
                value: binary.binary_id.to_string(),
            }),
        ),
    };
    Ok(oll::UpdatedNode {
        path: after_paths
            .get(&id)
            .or_else(|| before_paths.get(&id))
            .map(|path| oll::DocumentPath {
                value: path.clone(),
            }),
        catalog_node_id: Some(oll::CatalogNodeId {
            value: id.to_string(),
        }),
        catalog_revision: Some(oll::CatalogRevision {
            token: entry.catalog_revision.to_vec(),
        }),
        document_id,
        document_revision,
        binary_id,
        deleted: entry.deleted,
    })
}
