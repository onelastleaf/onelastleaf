use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use notify_debouncer_full::notify::{
    EventKind,
    event::{AccessKind, AccessMode, MetadataKind, ModifyKind},
};
use uuid::Uuid;

use super::super::{ReplicaError, store::ReplicaStore};

pub(super) fn ensure_projection_ancestors(root: &Path, path: &Path) -> Result<(), ReplicaError> {
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|error| ReplicaError::io("inspect replica root before projection", error))?;
    if !root_metadata.is_dir() {
        return Err(ReplicaError::InvalidArgument(
            "replica_root is not a real directory".to_owned(),
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        ReplicaError::CorruptStore("projected path escaped replica_root".to_owned())
    })?;
    let Some(parent) = relative.parent() else {
        return Ok(());
    };
    let mut current = root.to_owned();
    for component in parent.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(ReplicaError::CorruptStore(
                "projected path has a non-normal ancestor".to_owned(),
            ));
        };
        current.push(segment);
        ensure_projected_directory(&current)?;
    }
    Ok(())
}

pub(super) fn event_requires_reconciliation(event: &notify_debouncer_full::DebouncedEvent) -> bool {
    event.need_rescan()
        || matches!(
            event.kind,
            EventKind::Any
                | EventKind::Create(_)
                | EventKind::Remove(_)
                | EventKind::Modify(
                    ModifyKind::Any
                        | ModifyKind::Data(_)
                        | ModifyKind::Name(_)
                        | ModifyKind::Metadata(
                            MetadataKind::Any | MetadataKind::WriteTime | MetadataKind::Other
                        )
                        | ModifyKind::Other
                )
                | EventKind::Access(AccessKind::Close(AccessMode::Write))
        )
}

pub(super) fn ensure_projected_directory(path: &Path) -> Result<(), ReplicaError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => remove_path(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ReplicaError::io("inspect projected directory", error)),
    }
    fs::create_dir_all(path)
        .map_err(|error| ReplicaError::io("create projected directory", error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| ReplicaError::io("set projected directory permissions", error))?;
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io("sync projected directory", error))?;
    sync_parent(path, "sync projected directory parent")
}

pub(super) fn atomic_project_file(path: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected file has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create projected file parent", error))?;
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
        fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("replace projected directory with file", error))?;
    }
    let temporary = parent.join(format!(".oll-project-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| ReplicaError::io("create projected temporary file", error))?;
        output
            .write_all(bytes)
            .map_err(|error| ReplicaError::io("write projected temporary file", error))?;
        output
            .sync_all()
            .map_err(|error| ReplicaError::io("sync projected temporary file", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| ReplicaError::io("publish projected file", error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReplicaError::io("sync projected directory", error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) async fn atomic_project_blob(
    store: &ReplicaStore,
    path: &Path,
    sha256: &str,
) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected file has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ReplicaError::io("create projected file parent", error))?;
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
        fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("replace projected directory with file", error))?;
    }
    let temporary = parent.join(format!(".oll-project-{}.tmp", Uuid::new_v4()));
    let result = async {
        store.write_blob_to_path(sha256, &temporary).await?;
        fs::rename(&temporary, path)
            .map_err(|error| ReplicaError::io("publish projected file", error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReplicaError::io("sync projected directory", error))
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn remove_path(path: &Path) -> Result<(), ReplicaError> {
    let removed = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path)
            .map_err(|error| ReplicaError::io("remove stale projected directory", error)),
        Ok(_) => fs::remove_file(path)
            .map_err(|error| ReplicaError::io("remove stale projected file", error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => Err(ReplicaError::io("inspect stale projected path", error)),
    };
    removed?;
    sync_parent(path, "sync stale projection parent")
}

fn sync_parent(path: &Path, operation: &'static str) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::CorruptStore("projected path has no parent directory".to_owned())
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io(operation, error))
}
