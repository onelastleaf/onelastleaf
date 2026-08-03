use crate::protocol::oll;

use super::{
    super::{ReplicaError, types::ActiveReplica},
    catalog::{invalid, validate_namespace_path},
    mutation::api_uuid,
};

pub(super) fn validate_operation_id(operation_id: &str) -> Result<(), ReplicaError> {
    if operation_id.is_empty() || operation_id.len() > 512 || operation_id.contains('\0') {
        Err(invalid(
            "operation_id must be non-empty, NUL-free, and at most 512 bytes",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn check_preconditions(
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
