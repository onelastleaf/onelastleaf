mod bootstrap;
mod catalog;
mod catalog_record;
mod catalog_schema;
mod initialize;
mod loro;
mod namespace;
mod reconcile;
mod rename;
mod revisions;
mod scan;
mod support;
mod validation;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use super::{
    classification::{BinaryFile, DecodedText},
    store::NewBlob,
    types::{ActiveReplica, OperationRecord},
};

pub(crate) use bootstrap::merge_local_only_disk;
pub use catalog::decode_catalog_snapshot;
pub(crate) use catalog_record::write_entry_record;
pub use initialize::initialize_from_disk;
pub use loro::generate_loro_peer_id;
pub(crate) use loro::{get_entry_record, import_loro_doc, new_loro_doc, parse_tree_id};
pub use namespace::absolute_to_namespace;
pub(crate) use namespace::{parent_namespace_path, validate_name};
pub use reconcile::reconcile_disk;
pub use rename::apply_reliable_rename;
pub(crate) use revisions::recompute_live_catalog_revisions;
pub use scan::scan_working_tree;
pub use validation::validate_document_snapshot;
pub(crate) use validation::validate_loaded_replica;

#[derive(Clone, Debug)]
pub struct DiskSnapshot {
    pub entries: BTreeMap<String, DiskEntry>,
}

impl DiskSnapshot {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct DiskEntry {
    pub namespace_path: String,
    pub name: String,
    pub data: DiskEntryData,
}

#[derive(Clone, Debug)]
pub enum DiskEntryData {
    Directory,
    Text(DecodedText),
    Binary(BinaryFile),
}

impl DiskEntryData {
    pub(super) fn kind_name(&self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::Text(_) => "document",
            Self::Binary(_) => "binary",
        }
    }
}

#[derive(Debug)]
pub struct ModelChange {
    pub replica: ActiveReplica,
    pub blobs: Vec<NewBlob>,
    pub operations: Vec<OperationRecord>,
    pub projection_paths: Vec<String>,
    pub changed: bool,
}
