use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::watch;
use walkdir::WalkDir;

use super::PackageError;

pub(super) async fn copy_workspace(
    source: &Path,
    destination: &Path,
    shutdown: watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), PackageError> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelCopyOnDrop(Arc::clone(&cancelled));
    tokio::task::spawn_blocking(move || {
        copy_workspace_blocking(&source, &destination, || {
            cancelled.load(Ordering::Acquire) || shutdown.borrow().is_some()
        })
    })
    .await
    .map_err(|_| {
        PackageError::new(
            "local_path_invalid",
            "local_source",
            "local workspace copy task failed unexpectedly",
        )
    })?
}

fn copy_workspace_blocking(
    source: &Path,
    destination: &Path,
    cancelled: impl Fn() -> bool,
) -> Result<(), PackageError> {
    let mut directory_permissions = Vec::new();
    for entry in WalkDir::new(source).follow_links(false) {
        check_cancelled(&cancelled)?;
        let entry = entry.map_err(|error| {
            PackageError::new(
                "local_path_invalid",
                "local_source",
                format!("cannot walk local plugin workspace: {error}"),
            )
        })?;
        let relative = entry.path().strip_prefix(source).map_err(|_| {
            PackageError::new(
                "local_path_invalid",
                "local_source",
                "local workspace entry escaped its root",
            )
        })?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        let file_type = entry.file_type();
        if file_type.is_dir() {
            fs::create_dir(&target)
                .map_err(|error| local_copy_io("cannot create local workspace directory", error))?;
            directory_permissions.push((target, permission_mode(entry.path())?));
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)
                .map_err(|error| local_copy_io("cannot copy local workspace file", error))?;
            copy_permissions(entry.path(), &target)?;
        } else if file_type.is_symlink() {
            let link = fs::read_link(entry.path()).map_err(|error| {
                local_copy_io("cannot read local workspace symbolic link", error)
            })?;
            symlink(link, &target).map_err(|error| {
                local_copy_io("cannot copy local workspace symbolic link", error)
            })?;
        } else {
            return Err(PackageError::new(
                "local_path_invalid",
                "local_source",
                format!(
                    "local workspace contains unsupported file type at {}",
                    relative.display()
                ),
            ));
        }
    }
    for (directory, mode) in directory_permissions.into_iter().rev() {
        fs::set_permissions(&directory, fs::Permissions::from_mode(mode)).map_err(|error| {
            local_copy_io("cannot copy local workspace directory permissions", error)
        })?;
    }
    check_cancelled(&cancelled)
}

fn copy_permissions(source: &Path, destination: &Path) -> Result<(), PackageError> {
    let mode = permission_mode(source)?;
    fs::set_permissions(destination, fs::Permissions::from_mode(mode))
        .map_err(|error| local_copy_io("cannot copy local workspace permissions", error))
}

fn permission_mode(path: &Path) -> Result<u32, PackageError> {
    fs::symlink_metadata(path)
        .map_err(|error| local_copy_io("cannot inspect local workspace permissions", error))
        .map(|metadata| metadata.permissions().mode())
}

fn check_cancelled(cancelled: &impl Fn() -> bool) -> Result<(), PackageError> {
    if cancelled() {
        Err(PackageError::new(
            "operation_cancelled",
            "local_source",
            "local workspace copy was cancelled by daemon shutdown",
        ))
    } else {
        Ok(())
    }
}

fn local_copy_io(message: &'static str, error: std::io::Error) -> PackageError {
    PackageError::io("local_path_invalid", "local_source", message, error)
}

struct CancelCopyOnDrop(Arc<AtomicBool>);

impl Drop for CancelCopyOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn copies_regular_files_directories_and_symbolic_links() {
        let source = tempfile::TempDir::new().unwrap();
        let destination = tempfile::TempDir::new().unwrap();
        fs::create_dir(source.path().join("nested")).unwrap();
        fs::write(source.path().join("nested/file"), b"contents").unwrap();
        symlink("nested/file", source.path().join("link")).unwrap();
        fs::set_permissions(
            source.path().join("nested"),
            fs::Permissions::from_mode(0o500),
        )
        .unwrap();

        copy_workspace_blocking(source.path(), destination.path(), || false).unwrap();

        assert_eq!(
            fs::read(destination.path().join("nested/file")).unwrap(),
            b"contents"
        );
        assert_eq!(
            fs::read_link(destination.path().join("link")).unwrap(),
            PathBuf::from("nested/file")
        );
        assert_eq!(
            fs::metadata(destination.path().join("nested"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
        for directory in [source.path(), destination.path()] {
            fs::set_permissions(directory.join("nested"), fs::Permissions::from_mode(0o700))
                .unwrap();
        }
    }

    #[test]
    fn cancellation_stops_before_copying_entries() {
        let source = tempfile::TempDir::new().unwrap();
        let destination = tempfile::TempDir::new().unwrap();
        fs::write(source.path().join("file"), b"contents").unwrap();

        let error =
            copy_workspace_blocking(source.path(), destination.path(), || true).unwrap_err();

        assert_eq!(error.code(), "operation_cancelled");
        assert!(!destination.path().join("file").exists());
    }
}
