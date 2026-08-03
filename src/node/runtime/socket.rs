use std::{
    io,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
};

use tokio::net::UnixListener;

use crate::node::lock::{admin_socket_path, ensure_runtime_directory};

use super::NodeError;

pub(super) struct AdminSocketGuard {
    pub(super) path: PathBuf,
}

impl Drop for AdminSocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) fn bind_admin_socket(
    config_root: &Path,
) -> Result<(UnixListener, AdminSocketGuard), NodeError> {
    ensure_runtime_directory(config_root)?;
    let path = admin_socket_path(config_root);
    recover_stale_socket(&path)?;
    let listener =
        UnixListener::bind(&path).map_err(|error| NodeError::io("bind Admin UDS", error))?;
    let guard = AdminSocketGuard { path };
    if let Err(error) =
        std::fs::set_permissions(&guard.path, std::fs::Permissions::from_mode(0o600))
    {
        drop(guard);
        return Err(NodeError::io("set Admin UDS permissions", error));
    }
    Ok((listener, guard))
}

fn recover_stale_socket(path: &Path) -> Result<(), NodeError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(NodeError::io("inspect Admin UDS path", error)),
    };
    if !metadata.file_type().is_socket() {
        return Err(NodeError::Config(format!(
            "Admin UDS path {} is not a socket",
            path.display()
        )));
    }
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        return Err(NodeError::Unavailable(
            "an Admin endpoint already answers for this deployment".to_owned(),
        ));
    }
    std::fs::remove_file(path).map_err(|error| NodeError::io("remove stale Admin UDS", error))
}
