use std::collections::{BTreeMap, BTreeSet};

use loro::{ExportMode, LoroDoc};
use prost::Message;
use uuid::Uuid;

use crate::{node::logging::LogLevel, protocol::oll};

use super::{
    super::{
        ReplicaError,
        classification::encode_text,
        model::{
            get_entry_record, import_loro_doc, recompute_live_catalog_revisions, write_entry_record,
        },
        store::RetainedCommit,
        types::{DocumentObject, EntryData, OperationSource},
        watcher::ReplicaRuntime,
    },
    catalog::{api_elapsed_ms, invalid},
    change::{changed_projection_paths, document_operations, updated_node},
    mutation::apply_mutation,
    precondition::{check_preconditions, validate_operation_id},
};

impl ReplicaRuntime {
    pub async fn commit_documents(
        &self,
        request: oll::CommitDocumentsRequest,
        source: OperationSource,
        correlation_id: &str,
    ) -> Result<oll::CommitDocumentsResponse, ReplicaError> {
        let started = std::time::Instant::now();
        let operation_id = request.operation_id.clone();
        let mutation_count = request.mutations.len();
        self.logger.emit(
            LogLevel::Info,
            "oll::replica",
            "document_commit_started",
            correlation_id,
            serde_json::json!({
                "operation_id": &operation_id,
                "source": source.as_str(),
                "mutation_count": mutation_count,
            }),
        );
        let result = self
            .commit_documents_inner(request, source, correlation_id)
            .await;
        match &result {
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

        let _coordinator = self.identities.commit_guard().await;
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
        self.replace_state(replica.clone()).await;
        for (catalog_node_id, document_id) in encoding_promotions {
            self.logger.emit(
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
            );
        }
        if !projection_paths.is_empty() {
            self.project_targeted(&replica, &projection_paths, correlation_id)
                .await?;
        }
        Ok(response)
    }
}
