use std::path::{Component, Path};

use super::{
    super::{ReplicaError, types::EntryData},
    DiskEntry, DiskSnapshot,
};

pub(super) fn sorted_disk_entries(disk: &DiskSnapshot) -> Vec<&DiskEntry> {
    let mut entries = disk.entries.values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        path_depth(&left.namespace_path)
            .cmp(&path_depth(&right.namespace_path))
            .then_with(|| left.namespace_path.cmp(&right.namespace_path))
    });
    entries
}

pub(super) fn namespace_path(relative: &Path) -> Result<String, ReplicaError> {
    let mut segments = Vec::new();
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Err(ReplicaError::InvalidArgument(
                "working-tree path contains a non-normal segment".to_owned(),
            ));
        };
        let segment = segment.to_str().ok_or_else(|| {
            ReplicaError::InvalidArgument(
                "working-tree path contains a non-UTF-8 segment".to_owned(),
            )
        })?;
        validate_name(segment)?;
        segments.push(segment);
    }
    if segments.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

pub fn absolute_to_namespace(root: &Path, path: &Path) -> Result<String, ReplicaError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        ReplicaError::InvalidArgument("working-tree path lies outside replica_root".to_owned())
    })?;
    namespace_path(relative)
}

pub(crate) fn validate_name(name: &str) -> Result<(), ReplicaError> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains('/') || name.contains('\0') {
        return Err(ReplicaError::InvalidArgument(
            "catalog name is not a valid document-path segment".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn parent_namespace_path(path: &str) -> Result<&str, ReplicaError> {
    let index = path.rfind('/').ok_or_else(|| {
        ReplicaError::Internal("namespace path has no slash separator".to_owned())
    })?;
    if index == 0 {
        Ok("/")
    } else {
        Ok(&path[..index])
    }
}

pub(super) fn path_depth(path: &str) -> usize {
    path.bytes().filter(|byte| *byte == b'/').count()
}

pub(super) fn entry_kind_name(data: &EntryData) -> &'static str {
    match data {
        EntryData::Directory => "directory",
        EntryData::Document(_) => "document",
        EntryData::Binary(_) => "binary",
    }
}
