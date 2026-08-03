use std::collections::HashMap;

use uuid::Uuid;

use super::super::types::ActiveReplica;

pub(crate) fn recompute_live_catalog_revisions(
    replica: &mut ActiveReplica,
    paths: &HashMap<Uuid, String>,
) {
    for entry in replica.entries.values_mut() {
        if let Some(path) = paths.get(&entry.catalog_node_id) {
            entry.recompute_revision_at_path(path);
        } else {
            entry.recompute_revision();
        }
    }
}
