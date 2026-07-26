use std::{
    env,
    fs::{self, File, OpenOptions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        io::AsRawFd,
    },
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use super::runtime::NodeError;

const LOCK_DIRECTORY_NAME: &str = "oll";
const LOCK_FILENAME: &str = "node.lock";

/// An exclusive deployment lock held for the lifetime of a bootstrap or daemon.
pub struct DeploymentLock {
    _file: File,
}

impl DeploymentLock {
    pub fn acquire_for_init(config_root: &Path) -> Result<Self, NodeError> {
        Self::acquire(config_root, true)
    }

    pub fn acquire_for_runtime(config_root: &Path) -> Result<Self, NodeError> {
        Self::acquire(config_root, false)
    }

    pub fn preflight(config_root: &Path) -> Result<(), NodeError> {
        drop(Self::acquire_for_runtime(config_root)?);
        Ok(())
    }

    fn acquire(config_root: &Path, bootstrap: bool) -> Result<Self, NodeError> {
        let path = lock_path(config_root, bootstrap)?;
        let parent = path.parent().ok_or_else(|| {
            NodeError::Internal("deployment lock path has no parent directory".to_owned())
        })?;
        ensure_lock_directory(parent)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|error| NodeError::io("open deployment lock", error))?;

        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == -1 {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(libc::EWOULDBLOCK)) {
                return Err(NodeError::Unavailable(
                    "another oll daemon already owns this deployment".to_owned(),
                ));
            }
            return Err(NodeError::io("acquire deployment lock", error));
        }

        Ok(Self { _file: file })
    }
}

pub fn admin_socket_path(config_root: &Path) -> PathBuf {
    config_root.join("run").join("admin.sock")
}

pub fn ensure_runtime_directory(config_root: &Path) -> Result<PathBuf, NodeError> {
    let path = config_root.join("run");
    ensure_lock_directory(&path)?;
    Ok(path)
}

fn lock_path(config_root: &Path, bootstrap: bool) -> Result<PathBuf, NodeError> {
    if let Ok(canonical_root) = fs::canonicalize(config_root) {
        if local_bootstrap_lock_is_held(config_root)? {
            return Err(NodeError::Unavailable(
                "another oll daemon already owns this deployment".to_owned(),
            ));
        }
        let key = deployment_key(&canonical_root);
        for runtime_root in external_runtime_roots() {
            let directory = runtime_root.join(LOCK_DIRECTORY_NAME);
            if is_usable_lock_directory(&directory) {
                return Ok(directory.join(format!("{key}.lock")));
            }
        }
    } else if !bootstrap {
        return Err(NodeError::Config(format!(
            "cannot resolve configuration root {} before acquiring its deployment lock",
            config_root.display()
        )));
    }

    Ok(config_root.join("run").join(LOCK_FILENAME))
}

/// An init that started before its config root existed holds this local lock.
/// Once it has created the root, concurrent callers must still see that lock
/// rather than switching to an external runtime-directory lock.
fn local_bootstrap_lock_is_held(config_root: &Path) -> Result<bool, NodeError> {
    let path = config_root.join("run").join(LOCK_FILENAME);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(NodeError::Config(format!(
                "deployment lock path {} is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(NodeError::io("inspect bootstrap deployment lock", error)),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| NodeError::io("open bootstrap deployment lock", error))?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(false);
    }
    let error = io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::EWOULDBLOCK)) {
        Ok(true)
    } else {
        Err(NodeError::io("probe bootstrap deployment lock", error))
    }
}

fn is_usable_lock_directory(path: &Path) -> bool {
    if ensure_lock_directory(path).is_err() {
        return false;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let probe = path.join(format!(".lock-write-probe-{}-{nonce}", std::process::id()));
    let opened = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&probe);
    match opened {
        Ok(file) => {
            drop(file);
            fs::remove_file(probe).is_ok()
        }
        Err(_) => false,
    }
}

fn deployment_key(path: &Path) -> String {
    let bytes = path.as_os_str().as_bytes();
    let hash = Sha256::digest(bytes);
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn external_runtime_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(xdg) = env::var_os("XDG_RUNTIME_DIR") {
        let xdg = PathBuf::from(xdg);
        if is_usable_runtime_root(&xdg) {
            roots.push(xdg);
        }
    }

    let fallback = PathBuf::from(format!("/run/user/{}", effective_uid()));
    if is_usable_runtime_root(&fallback) && !roots.contains(&fallback) {
        roots.push(fallback);
    }
    roots
}

fn is_usable_runtime_root(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    metadata.is_dir() && metadata.uid() == effective_uid()
}

fn ensure_lock_directory(path: &Path) -> Result<(), NodeError> {
    fs::create_dir_all(path).map_err(|error| NodeError::io("create runtime directory", error))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| NodeError::io("inspect runtime directory", error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(NodeError::Config(format!(
            "runtime path {} is not a directory",
            path.display()
        )));
    }
    if metadata.uid() != effective_uid() {
        return Err(NodeError::Config(format!(
            "runtime directory {} is not owned by the deployment user",
            path.display()
        )));
    }
    if metadata.mode() & 0o077 != 0 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| NodeError::io("restrict runtime directory permissions", error))?;
    }
    Ok(())
}

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn bootstrap_uses_a_local_lock_before_the_config_root_exists() {
        let directory = TempDir::new().unwrap();
        let config_root = directory.path().join("new-config");
        let _lock = DeploymentLock::acquire_for_init(&config_root).unwrap();

        assert!(config_root.join("run/node.lock").exists());
    }

    #[test]
    fn a_held_lock_rejects_a_second_owner() {
        let directory = TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        fs::create_dir_all(&config_root).unwrap();
        let first = DeploymentLock::acquire_for_init(&config_root).unwrap();

        assert!(matches!(
            DeploymentLock::acquire_for_init(&config_root),
            Err(NodeError::Unavailable(_))
        ));
        drop(first);
        DeploymentLock::acquire_for_init(&config_root).unwrap();
    }

    #[test]
    fn bootstrap_lock_remains_authoritative_after_it_creates_the_root() {
        let directory = TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let _bootstrap = DeploymentLock::acquire_for_init(&config_root).unwrap();

        assert!(matches!(
            DeploymentLock::acquire_for_runtime(&config_root),
            Err(NodeError::Unavailable(_))
        ));
    }
}
