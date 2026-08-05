use std::{
    env,
    ffi::OsString,
    fmt, fs, io,
    path::{Component, Path, PathBuf},
};

use super::ReplicaStoreConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StorageLayoutError {
    problem: &'static str,
}

impl fmt::Display for StorageLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.problem)
    }
}

pub(crate) fn validate_storage_layout(
    config_root: &Path,
    replica_root: &Path,
    log_dir: &Path,
    artifact_download_dir: &Path,
    plugin_data_root: &Path,
    replica_store: &ReplicaStoreConfig,
) -> Result<(), StorageLayoutError> {
    validate_working_tree_roots(config_root, replica_root, log_dir, artifact_download_dir)?;

    let replica_root = comparable_location(replica_root, "cannot inspect node.replica_root")?;
    let plugin_data_root = comparable_location(
        plugin_data_root,
        "cannot inspect the derived plugin data root",
    )?;
    reject_overlap(
        &replica_root,
        &plugin_data_root,
        "node.replica_root must not overlap the derived plugin data root",
    )?;
    if let ReplicaStoreConfig::Sqlite { path } = replica_store {
        let parent = path.parent().ok_or(StorageLayoutError {
            problem: "node.replica_store.path must have a management directory",
        })?;
        let parent = comparable_location(
            parent,
            "cannot inspect node.replica_store.path management directory",
        )?;
        reject_overlap(
            &replica_root,
            &parent,
            "node.replica_root must not overlap node.replica_store.path management directory",
        )?;
    }

    Ok(())
}

pub(crate) fn validate_working_tree_roots(
    config_root: &Path,
    replica_root: &Path,
    log_dir: &Path,
    artifact_download_dir: &Path,
) -> Result<(), StorageLayoutError> {
    let config_root = comparable_location(config_root, "cannot inspect config_root")?;
    let replica_root = comparable_location(replica_root, "cannot inspect node.replica_root")?;
    let log_dir = comparable_location(log_dir, "cannot inspect node.log_dir")?;
    let artifact_download_dir = comparable_location(
        artifact_download_dir,
        "cannot inspect node.artifact_download_dir",
    )?;

    reject_overlap(
        &replica_root,
        &config_root,
        "node.replica_root must not overlap config_root",
    )?;
    reject_overlap(
        &replica_root,
        &log_dir,
        "node.replica_root must not overlap node.log_dir",
    )?;
    reject_overlap(
        &replica_root,
        &artifact_download_dir,
        "node.replica_root must not overlap node.artifact_download_dir",
    )?;
    Ok(())
}

fn comparable_location(
    path: &Path,
    inspection_problem: &'static str,
) -> Result<PathBuf, StorageLayoutError> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        env::current_dir()
            .map_err(|_| StorageLayoutError {
                problem: inspection_problem,
            })?
            .join(path)
    };
    let mut candidate = absolute.clone();
    let mut suffix = Vec::<OsString>::new();

    loop {
        match fs::canonicalize(&candidate) {
            Ok(mut canonical) => {
                for component in suffix.iter().rev() {
                    canonical.push(component);
                }
                return Ok(normalize_location(&canonical));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                let Some(component) = candidate.components().next_back() else {
                    return Err(StorageLayoutError {
                        problem: inspection_problem,
                    });
                };
                if matches!(component, Component::RootDir | Component::Prefix(_)) {
                    return Err(StorageLayoutError {
                        problem: inspection_problem,
                    });
                }
                suffix.push(component.as_os_str().to_owned());
                if !candidate.pop() {
                    return Err(StorageLayoutError {
                        problem: inspection_problem,
                    });
                }
            }
            Err(_) => {
                return Err(StorageLayoutError {
                    problem: inspection_problem,
                });
            }
        }
    }
}

fn normalize_location(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
        }
    }
    normalized
}

fn reject_overlap(
    left: &Path,
    right: &Path,
    problem: &'static str,
) -> Result<(), StorageLayoutError> {
    if left.starts_with(right) || right.starts_with(left) {
        Err(StorageLayoutError { problem })
    } else {
        Ok(())
    }
}
