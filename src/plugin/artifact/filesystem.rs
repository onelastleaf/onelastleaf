use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::plugin::{ArtifactPublishIntent, PluginArtifactId, PluginError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishOutcome {
    Published,
    AlreadyMatching,
    Collision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VerifiedFile {
    Missing,
    Matching,
    Contradictory,
}

pub(super) fn prepare_download_directory(path: &Path) -> Result<PathBuf, PluginError> {
    if path.as_os_str().is_empty() || !path.is_absolute() {
        return Err(PluginError::InvalidArgument(
            "artifact download directory must be absolute and nonempty".to_owned(),
        ));
    }
    let created = match fs::symlink_metadata(path) {
        Ok(_) => false,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .map_err(|error| PluginError::io("create artifact download directory", error))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|error| PluginError::io("restrict artifact download directory", error))?;
            true
        }
        Err(error) => {
            return Err(PluginError::io(
                "inspect artifact download directory",
                error,
            ));
        }
    };
    let canonical = fs::canonicalize(path)
        .map_err(|error| PluginError::io("resolve artifact download directory", error))?;
    if canonical.to_str().is_none() {
        return Err(PluginError::InvalidArgument(
            "artifact download directory must resolve to a UTF-8 path".to_owned(),
        ));
    }
    if !fs::metadata(&canonical)
        .map_err(|error| PluginError::io("inspect artifact download directory", error))?
        .is_dir()
    {
        return Err(PluginError::InvalidArgument(
            "artifact download path does not resolve to a directory".to_owned(),
        ));
    }
    verify_directory_writable(&canonical)?;
    if created {
        sync_directory(&canonical)?;
    }
    Ok(canonical)
}

pub(super) fn unchanged_cached_directory(path: &Path) -> Result<bool, PluginError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(PluginError::io(
                "inspect the previous artifact download directory",
                error,
            ));
        }
    };
    if !metadata.file_type().is_dir() {
        return Ok(false);
    }
    let resolved = fs::canonicalize(path).map_err(|error| {
        PluginError::io("resolve the previous artifact download directory", error)
    })?;
    Ok(resolved == path)
}

pub(super) fn validate_file_name(file_name: &str) -> Result<(), PluginError> {
    let bytes = file_name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 191
        || file_name == "."
        || file_name == ".."
        || bytes.contains(&0)
        || file_name.contains('/')
    {
        return Err(PluginError::InvalidArgument(
            "artifact file name must be one safe UTF-8 basename of at most 191 bytes".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn choose_destination(
    root: &Path,
    file_name: &str,
    artifact_id: PluginArtifactId,
) -> Result<PathBuf, PluginError> {
    let ordinary = root.join(file_name);
    if !path_exists(&ordinary)? {
        return Ok(ordinary);
    }
    let qualified = root.join(collision_name(file_name, artifact_id));
    if path_exists(&qualified)? {
        return Err(PluginError::AlreadyExists(
            "artifact filename and identity-qualified collision path both exist".to_owned(),
        ));
    }
    Ok(qualified)
}

pub(super) async fn publish_staging_async(
    intent: ArtifactPublishIntent,
) -> Result<PublishOutcome, PluginError> {
    tokio::task::spawn_blocking(move || publish_staging(&intent))
        .await
        .map_err(|error| PluginError::Store(format!("artifact publication task failed: {error}")))?
}

pub(super) fn publish_staging(
    intent: &ArtifactPublishIntent,
) -> Result<PublishOutcome, PluginError> {
    if !intent_paths_are_valid(intent) {
        return Err(PluginError::CorruptStore(
            "artifact publication intent contains invalid paths".to_owned(),
        ));
    }
    let parent = intent.destination.parent().ok_or_else(|| {
        PluginError::CorruptStore("artifact destination has no parent".to_owned())
    })?;
    if !unchanged_cached_directory(parent)? {
        return Err(PluginError::FailedPrecondition(
            "artifact download directory changed before publication".to_owned(),
        ));
    }
    run_publish_hook(&intent.destination);
    match fs::hard_link(&intent.staging_path, &intent.destination) {
        Ok(()) => {
            remove_staging_required(&intent.staging_path)?;
            sync_directory(intent.destination.parent().ok_or_else(|| {
                PluginError::CorruptStore("artifact destination has no parent".to_owned())
            })?)?;
            Ok(PublishOutcome::Published)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // A previous attempt can have linked the staging inode and then
            // failed before removing its private name. Content equality alone
            // is not enough to identify that case: a user may have won the
            // no-replace race with an unrelated file containing the same
            // bytes. Only the same inode is an idempotent oll publication.
            if same_file(&intent.staging_path, &intent.destination)?
                && file_matches(&intent.destination, intent.size_bytes, &intent.sha256)?
            {
                remove_staging_required(&intent.staging_path)?;
                sync_directory(intent.destination.parent().ok_or_else(|| {
                    PluginError::CorruptStore("artifact destination has no parent".to_owned())
                })?)?;
                Ok(PublishOutcome::AlreadyMatching)
            } else {
                Ok(PublishOutcome::Collision)
            }
        }
        Err(error) => Err(PluginError::io(
            "publish artifact without replacement",
            error,
        )),
    }
}

fn same_file(first: &Path, second: &Path) -> Result<bool, PluginError> {
    use std::os::unix::fs::MetadataExt as _;

    let first = fs::symlink_metadata(first)
        .map_err(|error| PluginError::io("inspect artifact staging inode", error))?;
    let second = fs::symlink_metadata(second)
        .map_err(|error| PluginError::io("inspect artifact destination inode", error))?;
    Ok(first.file_type().is_file()
        && second.file_type().is_file()
        && first.dev() == second.dev()
        && first.ino() == second.ino())
}

pub(super) fn file_matches(
    path: &Path,
    size_bytes: u64,
    sha256: &[u8; 32],
) -> Result<bool, PluginError> {
    Ok(matches!(
        inspect_file(path, size_bytes, sha256)?,
        VerifiedFile::Matching
    ))
}

pub(super) fn inspect_file(
    path: &Path,
    size_bytes: u64,
    sha256: &[u8; 32],
) -> Result<VerifiedFile, PluginError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(VerifiedFile::Missing);
        }
        Err(error) => return Err(PluginError::io("inspect artifact file", error)),
    };
    if !metadata.file_type().is_file() || metadata.len() != size_bytes {
        return Ok(VerifiedFile::Contradictory);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| PluginError::io("open artifact for verification", error))?;
    let mut hasher = Sha256::new();
    let mut observed_size = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| PluginError::io("read artifact for verification", error))?;
        if count == 0 {
            break;
        }
        observed_size = observed_size
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| PluginError::CorruptStore("artifact size overflowed".to_owned()))?;
        hasher.update(&buffer[..count]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    Ok(if observed_size == size_bytes && actual == *sha256 {
        VerifiedFile::Matching
    } else {
        VerifiedFile::Contradictory
    })
}

pub(super) fn remove_staging_if_present(path: &Path) -> Result<(), PluginError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PluginError::io("remove artifact staging file", error)),
    }
}

pub(super) fn cleanup_orphan_staging(roots: &[PathBuf]) -> Result<(), PluginError> {
    for root in roots {
        if !unchanged_cached_directory(root)? {
            continue;
        }
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(PluginError::io("scan artifact staging files", error)),
        };
        for entry in entries {
            let entry =
                entry.map_err(|error| PluginError::io("scan artifact staging file", error))?;
            let path = entry.path();
            if is_private_staging_name(&entry.file_name()) {
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(PluginError::io(
                            "remove orphan artifact staging file",
                            error,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn intent_paths_are_valid(intent: &ArtifactPublishIntent) -> bool {
    if validate_file_name(&intent.file_name).is_err()
        || !intent.destination.is_absolute()
        || !owned_staging_path(&intent.staging_path, intent.artifact_id)
        || intent.destination.parent() != intent.staging_path.parent()
    {
        return false;
    }
    let Some(parent) = intent.destination.parent() else {
        return false;
    };
    intent.destination == parent.join(&intent.file_name)
        || intent.destination == parent.join(collision_name(&intent.file_name, intent.artifact_id))
}

pub(super) fn owned_staging_path(path: &Path, artifact_id: PluginArtifactId) -> bool {
    path.is_absolute()
        && path
            .file_name()
            .is_some_and(|name| staging_name_matches(name, artifact_id))
}

fn collision_name(file_name: &str, artifact_id: PluginArtifactId) -> String {
    let suffix = format!(".artifact-{artifact_id}");
    match file_name.rfind('.') {
        Some(index) if index > 0 => {
            format!("{}{}{}", &file_name[..index], suffix, &file_name[index..])
        }
        _ => format!("{file_name}{suffix}"),
    }
}

fn path_exists(path: &Path) -> Result<bool, PluginError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(PluginError::io("inspect artifact destination", error)),
    }
}

fn verify_directory_writable(path: &Path) -> Result<(), PluginError> {
    let probe = path.join(format!(".oll-artifact-probe-{}", Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&probe)
        .map_err(|error| PluginError::io("verify artifact directory access", error))?;
    drop(file);
    fs::remove_file(&probe)
        .map_err(|error| PluginError::io("remove artifact directory probe", error))
}

fn remove_staging_required(path: &Path) -> Result<(), PluginError> {
    fs::remove_file(path).map_err(|error| PluginError::io("remove artifact staging file", error))
}

fn sync_directory(path: &Path) -> Result<(), PluginError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| PluginError::io("sync artifact directory", error))
}

fn is_private_staging_name(name: &std::ffi::OsStr) -> bool {
    parse_private_staging_name(name).is_some()
}

fn staging_name_matches(name: &std::ffi::OsStr, artifact_id: PluginArtifactId) -> bool {
    parse_private_staging_name(name).is_some_and(|(observed, _)| observed == artifact_id.as_uuid())
}

fn parse_private_staging_name(name: &std::ffi::OsStr) -> Option<(Uuid, Uuid)> {
    let body = name
        .to_str()
        .and_then(|name| name.strip_prefix(".oll-artifact-"))
        .and_then(|name| name.strip_suffix(".part"))?;
    if body.len() != 73 || body.as_bytes().get(36) != Some(&b'-') {
        return None;
    }
    let first = Uuid::parse_str(&body[..36]).ok()?;
    let second = Uuid::parse_str(&body[37..]).ok()?;
    if [(&body[..36], first), (&body[37..], second)]
        .into_iter()
        .all(|(source, uuid)| uuid.get_version_num() == 4 && uuid.to_string() == source)
    {
        Some((first, second))
    } else {
        None
    }
}

#[cfg(test)]
use std::sync::{Mutex, mpsc};

#[cfg(test)]
pub(crate) struct PublishTestHook {
    pub destination: PathBuf,
    pub started: mpsc::Sender<()>,
    pub release: mpsc::Receiver<()>,
}

#[cfg(test)]
static PUBLISH_TEST_HOOK: Mutex<Option<PublishTestHook>> = Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_publish_test_hook(hook: PublishTestHook) {
    *PUBLISH_TEST_HOOK.lock().unwrap() = Some(hook);
}

#[cfg(test)]
fn run_publish_hook(destination: &Path) {
    let hook = {
        let mut guard = PUBLISH_TEST_HOOK.lock().unwrap();
        if guard
            .as_ref()
            .is_some_and(|hook| hook.destination == destination)
        {
            guard.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        let _ = hook.started.send(());
        let _ = hook.release.recv();
    }
}

#[cfg(not(test))]
fn run_publish_hook(_destination: &Path) {}
