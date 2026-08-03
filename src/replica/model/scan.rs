use std::{collections::BTreeMap, path::Path};

use walkdir::WalkDir;

use super::{
    super::{ReplicaError, classification::ClassifiedFile, classification::classify_path},
    DiskEntry, DiskEntryData, DiskSnapshot,
    namespace::namespace_path,
};

pub fn scan_working_tree(root: &Path) -> Result<DiskSnapshot, ReplicaError> {
    let mut entries = BTreeMap::new();
    for item in WalkDir::new(root)
        .follow_links(false)
        .sort_by_file_name()
        .min_depth(1)
    {
        let item = item.map_err(|error| {
            ReplicaError::InvalidArgument(format!("cannot scan working-tree entry: {error}"))
        })?;
        let relative = item.path().strip_prefix(root).map_err(|_| {
            ReplicaError::Internal("working-tree walker escaped its root".to_owned())
        })?;
        let namespace_path = namespace_path(relative)?;
        let name = relative
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ReplicaError::InvalidArgument(
                    "working-tree entry name is not valid UTF-8".to_owned(),
                )
            })?
            .to_owned();
        let file_type = item.file_type();
        let data = if file_type.is_dir() {
            DiskEntryData::Directory
        } else if file_type.is_file() {
            match classify_path(item.path())? {
                ClassifiedFile::Text(text) => DiskEntryData::Text(text),
                ClassifiedFile::Binary(binary) => DiskEntryData::Binary(binary),
            }
        } else {
            return Err(ReplicaError::InvalidArgument(format!(
                "unsupported special working-tree entry at {namespace_path}"
            )));
        };
        let entry = DiskEntry {
            namespace_path: namespace_path.clone(),
            name,
            data,
        };
        if entries.insert(namespace_path, entry).is_some() {
            return Err(ReplicaError::InvalidArgument(
                "working-tree scan produced a duplicate path".to_owned(),
            ));
        }
    }
    Ok(DiskSnapshot { entries })
}
