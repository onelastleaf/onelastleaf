use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::{OpenOptionsExt, PermissionsExt, symlink},
    },
    path::{Component, Path, PathBuf},
};

use flate2::read::GzDecoder;
use futures_util::StreamExt;
use reqwest::redirect::{Action, Attempt, Policy};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use super::{ArchiveKind, PackageError, PublisherManifest, ReleasePackage};

const MAX_HTTP_REDIRECTS: usize = 10;
const MAX_SYMLINK_TARGET_BYTES: u64 = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArchiveEntryKind {
    Directory,
    File,
    Symlink(PathBuf),
}

#[derive(Default)]
struct ArchivePlan {
    entries: BTreeMap<PathBuf, ArchiveEntryKind>,
}

impl ArchivePlan {
    fn insert(
        &mut self,
        path: PathBuf,
        kind: ArchiveEntryKind,
        format: &'static str,
    ) -> Result<(), PackageError> {
        if self.entries.insert(path, kind).is_some() {
            return Err(unsafe_archive(format!(
                "{format} archive contains duplicate normalized paths"
            )));
        }
        Ok(())
    }
}

pub async fn stage_release_download(
    package: &ReleasePackage,
    destination: &Path,
) -> Result<u64, PackageError> {
    let url = package.parsed_url()?;
    let (expected_size, expected_sha256) = package.remote_integrity()?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .await
        .map_err(|error| download_io("cannot create release staging file", error))?;
    let result = match url.scheme() {
        "file" => stage_file_url(&url, expected_size, &mut file).await,
        "http" | "https" => stage_http_url(url, expected_size, &mut file).await,
        _ => Err(PackageError::new(
            "release_package_download_failed",
            "download",
            "release URL scheme is not allowed",
        )),
    };
    let (size, digest) = match result {
        Ok(result) => result,
        Err(error) => {
            drop(file);
            let _ = tokio::fs::remove_file(destination).await;
            return Err(error);
        }
    };
    if let Err(error) = file.flush().await {
        drop(file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(download_io("cannot flush release staging file", error));
    }
    if let Err(error) = file.sync_all().await {
        drop(file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(download_io(
            "cannot synchronize release staging file",
            error,
        ));
    }
    if size != expected_size || crate::replica::lower_hex(&digest) != expected_sha256 {
        drop(file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(PackageError::new(
            "release_package_checksum_mismatch",
            "download",
            "release package size or SHA-256 differs from oll-release.json",
        ));
    }
    Ok(size)
}

pub(crate) async fn stage_local_release_package(
    package: &ReleasePackage,
    workspace: &Path,
    destination: &Path,
) -> Result<u64, PackageError> {
    let source = package.local_package_path(workspace)?;
    let mut source = tokio::fs::File::open(source).await.map_err(|error| {
        PackageError::io(
            "release_package_path_invalid",
            "release_index",
            "cannot open local release package",
            error,
        )
    })?;
    let mut destination_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .await
        .map_err(|error| local_package_io("cannot create release staging file", error))?;
    let result = tokio::io::copy(&mut source, &mut destination_file)
        .await
        .map_err(|error| local_package_io("cannot stage local release package", error));
    let size = match result {
        Ok(size) => size,
        Err(error) => {
            drop(destination_file);
            let _ = tokio::fs::remove_file(destination).await;
            return Err(error);
        }
    };
    if let Err(error) = destination_file.flush().await {
        drop(destination_file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(local_package_io("cannot flush release staging file", error));
    }
    if let Err(error) = destination_file.sync_all().await {
        drop(destination_file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(local_package_io(
            "cannot synchronize release staging file",
            error,
        ));
    }
    Ok(size)
}

async fn stage_file_url(
    url: &Url,
    expected_size: u64,
    destination: &mut tokio::fs::File,
) -> Result<(u64, [u8; 32]), PackageError> {
    let path = url.to_file_path().map_err(|()| {
        PackageError::new(
            "release_package_download_failed",
            "download",
            "file release URL is not an absolute local path",
        )
    })?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| download_io("cannot inspect file release package", error))?;
    if !metadata.is_file() {
        return Err(PackageError::new(
            "release_package_download_failed",
            "download",
            "file release URL does not name a regular file",
        ));
    }
    if metadata.len() != expected_size {
        return Err(PackageError::new(
            "release_package_checksum_mismatch",
            "download",
            "release package size or SHA-256 differs from oll-release.json",
        ));
    }
    let mut source = tokio::fs::File::open(path)
        .await
        .map_err(|error| download_io("cannot open file release package", error))?;
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .await
            .map_err(|error| download_io("cannot read file release package", error))?;
        if read == 0 {
            break;
        }
        let next_size = size.checked_add(read as u64).ok_or_else(|| {
            PackageError::new(
                "release_package_download_failed",
                "download",
                "release package size overflowed u64",
            )
        })?;
        if next_size > expected_size {
            return Err(PackageError::new(
                "release_package_checksum_mismatch",
                "download",
                "release package size or SHA-256 differs from oll-release.json",
            ));
        }
        destination
            .write_all(&buffer[..read])
            .await
            .map_err(|error| download_io("cannot write release staging file", error))?;
        hash.update(&buffer[..read]);
        size = next_size;
    }
    Ok((size, hash.finalize().into()))
}

async fn stage_http_url(
    url: Url,
    expected_size: u64,
    destination: &mut tokio::fs::File,
) -> Result<(u64, [u8; 32]), PackageError> {
    fn allowed_redirect(attempt: Attempt<'_>) -> Action {
        match attempt.url().scheme() {
            "http" | "https" => Policy::limited(MAX_HTTP_REDIRECTS).redirect(attempt),
            _ => attempt.error("release redirect changed to an unsupported transport"),
        }
    }

    let client = reqwest::Client::builder()
        .redirect(Policy::custom(allowed_redirect))
        .build()
        .map_err(|_| {
            PackageError::new(
                "release_package_download_failed",
                "download",
                "cannot initialize release HTTP client",
            )
        })?;
    let response = client.get(url).send().await.map_err(|_| {
        PackageError::new(
            "release_package_download_failed",
            "download",
            "release HTTP request failed",
        )
    })?;
    if response.status().is_redirection() {
        return Err(PackageError::new(
            "release_package_download_failed",
            "download",
            "release HTTP redirect changed to an unsupported transport",
        ));
    }
    let response = response.error_for_status().map_err(|_| {
        PackageError::new(
            "release_package_download_failed",
            "download",
            "release HTTP request failed",
        )
    })?;
    if !matches!(response.url().scheme(), "http" | "https") {
        return Err(PackageError::new(
            "release_package_download_failed",
            "download",
            "HTTP redirect changed to an unsupported transport",
        ));
    }
    let mut stream = response.bytes_stream();
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| {
            PackageError::new(
                "release_package_download_failed",
                "download",
                "release HTTP response body failed",
            )
        })?;
        let next_size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
            PackageError::new(
                "release_package_download_failed",
                "download",
                "release package size overflowed u64",
            )
        })?;
        if next_size > expected_size {
            return Err(PackageError::new(
                "release_package_checksum_mismatch",
                "download",
                "release package size or SHA-256 differs from oll-release.json",
            ));
        }
        destination
            .write_all(&chunk)
            .await
            .map_err(|error| download_io("cannot write release staging file", error))?;
        hash.update(&chunk);
        size = next_size;
    }
    Ok((size, hash.finalize().into()))
}

pub fn extract_release_archive(
    archive: &Path,
    kind: ArchiveKind,
    install_root: &Path,
    expected_publisher: &PublisherManifest,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), PackageError> {
    check_archive_cancellation(cancelled)?;
    fs::create_dir_all(install_root)
        .map_err(|error| archive_io("cannot create release install root", error))?;
    match kind {
        ArchiveKind::TarGz => extract_tar_gz(archive, install_root, cancelled)?,
        ArchiveKind::Zip => extract_zip(archive, install_root, cancelled)?,
    }
    check_archive_cancellation(cancelled)?;
    let manifest_path = install_root.join("oll.toml");
    let source = fs::read_to_string(&manifest_path)
        .map_err(|error| archive_io("release archive has no readable oll.toml", error))?;
    let actual = PublisherManifest::parse(&source)?;
    if &actual != expected_publisher {
        return Err(PackageError::new(
            "manifest_invalid",
            "archive",
            "release archive publisher manifest differs from repository oll.toml",
        ));
    }
    Ok(())
}

fn extract_tar_gz(
    archive_path: &Path,
    install_root: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), PackageError> {
    let plan = plan_tar_gz(archive_path, cancelled)?;
    let symlink_order = validate_archive_plan(&plan)?;
    create_archive_directories(install_root, &plan)?;

    let file = File::open(archive_path)
        .map_err(|error| archive_io("cannot open release tar.gz", error))?;
    let mut archive = tar::Archive::new(GzDecoder::new(file));
    for entry in archive
        .entries()
        .map_err(|error| archive_io("cannot read release tar.gz", error))?
    {
        check_archive_cancellation(cancelled)?;
        let mut entry = entry.map_err(|error| archive_io("cannot read tar entry", error))?;
        let path = entry
            .path()
            .map_err(|error| archive_io("cannot read tar entry path", error))?;
        let relative = safe_relative_path(&path)?;
        let kind = entry.header().entry_type();
        if kind.is_file() {
            let destination = install_root.join(&relative);
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&destination)
                .map_err(|error| archive_io("cannot create archive output", error))?;
            copy_archive_entry(
                &mut entry,
                &mut output,
                cancelled,
                "cannot extract tar entry",
            )?;
            let mode = entry.header().mode().unwrap_or(0o600) & 0o777;
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode))
                .map_err(|error| archive_io("cannot set archive output permissions", error))?;
        }
    }
    create_archive_symlinks(install_root, &plan, &symlink_order, cancelled)?;
    Ok(())
}

fn plan_tar_gz(
    archive_path: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<ArchivePlan, PackageError> {
    let file = File::open(archive_path)
        .map_err(|error| archive_io("cannot open release tar.gz", error))?;
    let mut archive = tar::Archive::new(GzDecoder::new(file));
    let mut plan = ArchivePlan::default();
    for entry in archive
        .entries()
        .map_err(|error| archive_io("cannot read release tar.gz", error))?
    {
        check_archive_cancellation(cancelled)?;
        let entry = entry.map_err(|error| archive_io("cannot read tar entry", error))?;
        let path = entry
            .path()
            .map_err(|error| archive_io("cannot read tar entry path", error))?;
        let relative = safe_relative_path(&path)?;
        let entry_type = entry.header().entry_type();
        if relative.as_os_str().is_empty() {
            if entry_type.is_dir() {
                plan.insert(relative, ArchiveEntryKind::Directory, "tar")?;
                continue;
            }
            return Err(unsafe_archive(
                "tar archive root marker must be a directory",
            ));
        }
        let kind = if entry_type.is_dir() {
            ArchiveEntryKind::Directory
        } else if entry_type.is_file() {
            ArchiveEntryKind::File
        } else if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .map_err(|error| archive_io("cannot read tar symlink target", error))?
                .ok_or_else(|| unsafe_archive("tar symlink target is missing"))?
                .into_owned();
            ArchiveEntryKind::Symlink(target)
        } else if entry_type.is_hard_link() {
            return Err(unsafe_archive("tar archive hard links are unsupported"));
        } else {
            return Err(unsafe_archive(
                "tar archive contains an unsupported entry type",
            ));
        };
        plan.insert(relative, kind, "tar")?;
    }
    Ok(plan)
}

fn extract_zip(
    archive_path: &Path,
    install_root: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), PackageError> {
    let plan = plan_zip(archive_path, cancelled)?;
    let symlink_order = validate_archive_plan(&plan)?;
    create_archive_directories(install_root, &plan)?;

    let file =
        File::open(archive_path).map_err(|error| archive_io("cannot open release zip", error))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|_| unsafe_archive("release zip central directory is invalid"))?;
    for index in 0..archive.len() {
        check_archive_cancellation(cancelled)?;
        let mut entry = archive
            .by_index(index)
            .map_err(|_| unsafe_archive("release zip entry is invalid"))?;
        let relative = safe_relative_path(Path::new(entry.name()))?;
        if !entry.is_file() || entry.is_symlink() {
            continue;
        }
        let destination = install_root.join(relative);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&destination)
            .map_err(|error| archive_io("cannot create archive output", error))?;
        copy_archive_entry(
            &mut entry,
            &mut output,
            cancelled,
            "cannot extract zip entry",
        )?;
        if let Some(mode) = entry.unix_mode() {
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode & 0o777))
                .map_err(|error| archive_io("cannot set archive output permissions", error))?;
        }
    }
    create_archive_symlinks(install_root, &plan, &symlink_order, cancelled)?;
    Ok(())
}

fn plan_zip(
    archive_path: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<ArchivePlan, PackageError> {
    let file =
        File::open(archive_path).map_err(|error| archive_io("cannot open release zip", error))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|_| unsafe_archive("release zip central directory is invalid"))?;
    let mut plan = ArchivePlan::default();
    for index in 0..archive.len() {
        check_archive_cancellation(cancelled)?;
        let mut entry = archive
            .by_index(index)
            .map_err(|_| unsafe_archive("release zip entry is invalid"))?;
        let relative = safe_relative_path(Path::new(entry.name()))?;
        if relative.as_os_str().is_empty() {
            if entry.is_dir() {
                plan.insert(relative, ArchiveEntryKind::Directory, "zip")?;
                continue;
            }
            return Err(unsafe_archive(
                "zip archive root marker must be a directory",
            ));
        }
        let kind = if entry.is_symlink() {
            ArchiveEntryKind::Symlink(read_zip_symlink_target(&mut entry)?)
        } else if entry.is_dir() {
            ArchiveEntryKind::Directory
        } else if entry.is_file() {
            ArchiveEntryKind::File
        } else {
            return Err(unsafe_archive(
                "zip archive contains an unsupported entry type",
            ));
        };
        plan.insert(relative, kind, "zip")?;
    }
    Ok(plan)
}

fn read_zip_symlink_target(entry: &mut impl Read) -> Result<PathBuf, PackageError> {
    let mut bytes = Vec::new();
    entry
        .take(MAX_SYMLINK_TARGET_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| archive_io("cannot read zip symlink target", error))?;
    if bytes.len() as u64 > MAX_SYMLINK_TARGET_BYTES {
        return Err(unsafe_archive("zip symlink target is too long"));
    }
    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

fn validate_archive_plan(plan: &ArchivePlan) -> Result<Vec<PathBuf>, PackageError> {
    match plan.entries.get(Path::new("oll.toml")) {
        Some(ArchiveEntryKind::File) => {}
        _ => {
            return Err(unsafe_archive(
                "release archive oll.toml must be a regular file",
            ));
        }
    }

    for path in plan.entries.keys() {
        for ancestor in nonempty_ancestors(path) {
            if let Some(kind) = plan.entries.get(ancestor)
                && !matches!(kind, ArchiveEntryKind::Directory)
            {
                return Err(unsafe_archive(
                    "archive entry descends through a non-directory path",
                ));
            }
        }
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut order = Vec::new();
    for (path, kind) in &plan.entries {
        if matches!(kind, ArchiveEntryKind::Symlink(_)) {
            validate_archive_symlink(path, plan, &mut visiting, &mut visited, &mut order)?;
        }
    }
    Ok(order)
}

fn validate_archive_symlink(
    source: &Path,
    plan: &ArchivePlan,
    visiting: &mut BTreeSet<PathBuf>,
    visited: &mut BTreeSet<PathBuf>,
    order: &mut Vec<PathBuf>,
) -> Result<(), PackageError> {
    if visited.contains(source) {
        return Ok(());
    }
    if !visiting.insert(source.to_path_buf()) {
        return Err(unsafe_archive("archive symbolic links contain a cycle"));
    }
    let ArchiveEntryKind::Symlink(raw_target) = plan
        .entries
        .get(source)
        .expect("a planned symlink has an entry")
    else {
        unreachable!("only planned symlinks are recursively validated");
    };
    let target = resolve_archive_symlink_target(source, raw_target)?;
    if target == Path::new("oll.toml") {
        return Err(unsafe_archive("archive symbolic link resolves to oll.toml"));
    }
    match plan.entries.get(&target) {
        Some(ArchiveEntryKind::Symlink(_)) => {
            validate_archive_symlink(&target, plan, visiting, visited, order)?;
        }
        Some(ArchiveEntryKind::Directory | ArchiveEntryKind::File) => {}
        None if archive_has_implicit_directory(plan, &target) => {}
        None => {
            return Err(unsafe_archive(
                "archive symbolic link target does not exist in the archive",
            ));
        }
    }
    visiting.remove(source);
    visited.insert(source.to_path_buf());
    order.push(source.to_path_buf());
    Ok(())
}

fn resolve_archive_symlink_target(source: &Path, target: &Path) -> Result<PathBuf, PackageError> {
    if target.as_os_str().is_empty() || target.is_absolute() {
        return Err(unsafe_archive(
            "archive symbolic link target must be nonempty and relative",
        ));
    }
    let target_bytes = target.as_os_str().as_bytes();
    if target_bytes.contains(&0) {
        return Err(unsafe_archive("archive symbolic link target contains NUL"));
    }
    if target_bytes.len() as u64 > MAX_SYMLINK_TARGET_BYTES {
        return Err(unsafe_archive("archive symbolic link target is too long"));
    }

    let mut resolved = source
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_os_string()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for component in target.components() {
        match component {
            Component::Normal(value) => resolved.push(value.to_os_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if resolved.pop().is_none() {
                    return Err(unsafe_archive(
                        "archive symbolic link target escapes the install root",
                    ));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_archive(
                    "archive symbolic link target must be relative",
                ));
            }
        }
    }
    if resolved.is_empty() {
        return Err(unsafe_archive(
            "archive symbolic link must not resolve to the install root",
        ));
    }
    Ok(resolved.into_iter().collect())
}

fn archive_has_implicit_directory(plan: &ArchivePlan, path: &Path) -> bool {
    plan.entries
        .keys()
        .any(|candidate| candidate != path && candidate.starts_with(path))
}

fn nonempty_ancestors(path: &Path) -> impl Iterator<Item = &Path> {
    path.ancestors()
        .skip(1)
        .take_while(|ancestor| !ancestor.as_os_str().is_empty())
}

fn create_archive_directories(install_root: &Path, plan: &ArchivePlan) -> Result<(), PackageError> {
    let mut directories = BTreeSet::new();
    for (path, kind) in &plan.entries {
        directories.extend(nonempty_ancestors(path).map(Path::to_path_buf));
        if !path.as_os_str().is_empty() && matches!(kind, ArchiveEntryKind::Directory) {
            directories.insert(path.clone());
        }
    }
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_unstable_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .then_with(|| left.cmp(right))
    });
    for directory in directories {
        fs::create_dir(install_root.join(directory))
            .map_err(|error| archive_io("cannot create archive directory", error))?;
    }
    Ok(())
}

fn create_archive_symlinks(
    install_root: &Path,
    plan: &ArchivePlan,
    order: &[PathBuf],
    cancelled: &dyn Fn() -> bool,
) -> Result<(), PackageError> {
    for source in order {
        check_archive_cancellation(cancelled)?;
        let ArchiveEntryKind::Symlink(target) = plan
            .entries
            .get(source)
            .expect("a validated symlink has a planned entry")
        else {
            unreachable!("the symlink order contains only symlinks");
        };
        symlink(target, install_root.join(source))
            .map_err(|error| archive_io("cannot create archive symbolic link", error))?;
    }
    Ok(())
}

fn copy_archive_entry(
    source: &mut impl Read,
    destination: &mut impl Write,
    cancelled: &dyn Fn() -> bool,
    error_message: &'static str,
) -> Result<(), PackageError> {
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_archive_cancellation(cancelled)?;
        let read = source
            .read(&mut buffer)
            .map_err(|error| archive_io(error_message, error))?;
        if read == 0 {
            return Ok(());
        }
        check_archive_cancellation(cancelled)?;
        destination
            .write_all(&buffer[..read])
            .map_err(|error| archive_io(error_message, error))?;
    }
}

fn check_archive_cancellation(cancelled: &dyn Fn() -> bool) -> Result<(), PackageError> {
    if cancelled() {
        Err(PackageError::new(
            "install_publish_failed",
            "archive",
            "release extraction was cancelled",
        ))
    } else {
        Ok(())
    }
}

fn safe_relative_path(path: &Path) -> Result<PathBuf, PackageError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(unsafe_archive(
            "archive entry path must be nonempty and relative",
        ));
    }
    if path.as_os_str().as_bytes().contains(&0) {
        return Err(unsafe_archive("archive entry path contains NUL"));
    }
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => result.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_archive("archive entry escapes the install root"));
            }
        }
    }
    Ok(result)
}

fn download_io(message: &'static str, error: io::Error) -> PackageError {
    PackageError::io(
        "release_package_download_failed",
        "download",
        message,
        error,
    )
}

fn local_package_io(message: &'static str, error: io::Error) -> PackageError {
    PackageError::io(
        "release_package_path_invalid",
        "release_copy",
        message,
        error,
    )
}

fn archive_io(message: &'static str, error: io::Error) -> PackageError {
    PackageError::io("archive_unsafe", "archive", message, error)
}

fn unsafe_archive(message: impl Into<String>) -> PackageError {
    PackageError::new("archive_unsafe", "archive", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const EXPECTED_PUBLISHER: &str = r#"format_version = 1
[plugin]
id = "oll.archive-test"
name = "archive-test"
[source]
checkout = "source"
[runtime]
argv = ["/bin/true"]
"#;

    fn expected_publisher() -> PublisherManifest {
        PublisherManifest::parse(EXPECTED_PUBLISHER).unwrap()
    }

    #[tokio::test]
    async fn file_release_rejects_declared_size_mismatch_before_copying_bytes() {
        let directory = tempfile::TempDir::new().unwrap();
        let source = directory.path().join("release.tar.gz");
        let destination = directory.path().join("staged.tar.gz");
        fs::write(&source, b"larger than declared").unwrap();
        let url = Url::from_file_path(&source).unwrap();
        let mut staged = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&destination)
            .await
            .unwrap();

        let error = stage_file_url(&url, 1, &mut staged).await.unwrap_err();

        assert_eq!(error.code(), "release_package_checksum_mismatch");
        assert_eq!(tokio::fs::metadata(destination).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn http_release_stops_streaming_as_soon_as_declared_size_is_exceeded() {
        let directory = tempfile::TempDir::new().unwrap();
        let destination = directory.path().join("staged.tar.gz");
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\nxx\r\n",
                )
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let package = ReleasePackage {
            target: "test-target".to_owned(),
            url: Some(format!("http://{address}/release.tar.gz")),
            path: None,
            archive: ArchiveKind::TarGz,
            size_bytes: Some(1),
            sha256: Some("00".repeat(32)),
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stage_release_download(&package, &destination),
        )
        .await
        .expect("oversized HTTP body was read to completion")
        .unwrap_err();
        server.abort();

        assert_eq!(error.code(), "release_package_checksum_mismatch");
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn http_release_never_redirects_to_a_local_file() {
        let directory = tempfile::TempDir::new().unwrap();
        let local_package = directory.path().join("local-release.tar.gz");
        let destination = directory.path().join("staged.tar.gz");
        let bytes = b"local package must not be read through a redirect";
        fs::write(&local_package, bytes).unwrap();
        let file_url = Url::from_file_path(&local_package).unwrap();
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {file_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let package = ReleasePackage {
            target: "test-target".to_owned(),
            url: Some(format!("http://{address}/release.tar.gz")),
            path: None,
            archive: ArchiveKind::TarGz,
            size_bytes: Some(bytes.len() as u64),
            sha256: Some(crate::replica::lower_hex(&Sha256::digest(bytes))),
        };

        let error = stage_release_download(&package, &destination)
            .await
            .unwrap_err();
        server.await.unwrap();

        assert_eq!(error.code(), "release_package_download_failed");
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn http_release_rejects_a_redirect_chain_beyond_the_finite_limit() {
        let directory = tempfile::TempDir::new().unwrap();
        let destination = directory.path().join("staged.tar.gz");
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let redirect_url = format!("http://{address}/loop");
        let server_redirect = redirect_url.clone();
        let server = tokio::spawn(async move {
            for _ in 0..=MAX_HTTP_REDIRECTS {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {server_redirect}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let package = ReleasePackage {
            target: "test-target".to_owned(),
            url: Some(redirect_url),
            path: None,
            archive: ArchiveKind::TarGz,
            size_bytes: Some(0),
            sha256: Some(crate::replica::lower_hex(&Sha256::digest([]))),
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stage_release_download(&package, &destination),
        )
        .await
        .expect("redirect chain did not stop at its finite limit")
        .unwrap_err();
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .expect("redirect fixture did not observe the bounded chain")
            .unwrap();

        assert_eq!(error.code(), "release_package_download_failed");
        assert!(!destination.exists());
    }

    #[test]
    fn archive_paths_reject_traversal_and_normalize_dots() {
        assert!(safe_relative_path(Path::new("../escape")).is_err());
        assert!(safe_relative_path(Path::new("/absolute")).is_err());
        assert!(safe_relative_path(&PathBuf::from(OsString::from_vec(b"a\0b".to_vec()))).is_err());
        assert_eq!(safe_relative_path(Path::new(".")).unwrap(), Path::new(""));
        assert_eq!(
            safe_relative_path(Path::new("a/./b")).unwrap(),
            Path::new("a/b")
        );
    }

    #[test]
    fn tar_release_accepts_an_internal_pnpm_style_symbolic_link() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive_path = directory.path().join("pnpm.tar.gz");
        let file = File::create(&archive_path).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        let mut root = tar::Header::new_gnu();
        root.set_entry_type(tar::EntryType::Directory);
        root.set_path(".").unwrap();
        root.set_size(0);
        root.set_mode(0o755);
        root.set_cksum();
        archive.append(&root, std::io::empty()).unwrap();
        append_tar_file(&mut archive, "oll.toml", EXPECTED_PUBLISHER);
        append_tar_file(
            &mut archive,
            ".pnpm/sdk/node_modules/@onelastleaf/plugin-sdk/index.js",
            "export const ready = true;\n",
        );
        append_tar_symlink(
            &mut archive,
            "node_modules/@onelastleaf/plugin-sdk",
            "../../.pnpm/sdk/node_modules/@onelastleaf/plugin-sdk",
        );
        archive.into_inner().unwrap().finish().unwrap();

        let install = directory.path().join("install");
        extract_release_archive(
            &archive_path,
            ArchiveKind::TarGz,
            &install,
            &expected_publisher(),
            &|| false,
        )
        .unwrap();

        assert_eq!(
            fs::read_to_string(install.join("node_modules/@onelastleaf/plugin-sdk/index.js"))
                .unwrap(),
            "export const ready = true;\n"
        );
    }

    #[test]
    fn tar_release_still_rejects_hard_links() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive_path = directory.path().join("hardlink.tar.gz");
        let file = File::create(&archive_path).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Link);
        header.set_path("hardlink-entry").unwrap();
        header.set_link_name("internal-target").unwrap();
        header.set_size(0);
        header.set_mode(0o700);
        header.set_cksum();
        archive.append(&header, std::io::empty()).unwrap();
        archive.into_inner().unwrap().finish().unwrap();

        let error = extract_release_archive(
            &archive_path,
            ArchiveKind::TarGz,
            &directory.path().join("install"),
            &expected_publisher(),
            &|| false,
        )
        .unwrap_err();

        assert_eq!(error.code(), "archive_unsafe");
        assert_eq!(error.message(), "tar archive hard links are unsupported");
    }

    #[test]
    fn zip_release_accepts_an_internal_symbolic_link() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive_path = directory.path().join("symlink.zip");
        let file = File::create(&archive_path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        archive
            .start_file("oll.toml", zip::write::SimpleFileOptions::default())
            .unwrap();
        archive.write_all(EXPECTED_PUBLISHER.as_bytes()).unwrap();
        archive
            .start_file(
                "packages/sdk/index.js",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        archive.write_all(b"export const ready = true;\n").unwrap();
        archive
            .add_symlink(
                "node_modules/sdk",
                "../packages/sdk",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        archive.finish().unwrap();

        let install = directory.path().join("install");
        extract_release_archive(
            &archive_path,
            ArchiveKind::Zip,
            &install,
            &expected_publisher(),
            &|| false,
        )
        .unwrap();

        assert_eq!(
            fs::read_to_string(install.join("node_modules/sdk/index.js")).unwrap(),
            "export const ready = true;\n"
        );
    }

    #[test]
    fn symbolic_link_graph_rejects_unsafe_sources_and_targets() {
        for (label, entries, expected) in [
            (
                "absolute",
                vec![("link", ArchiveEntryKind::Symlink("/outside".into()))],
                "archive symbolic link target must be nonempty and relative",
            ),
            (
                "empty",
                vec![("link", ArchiveEntryKind::Symlink(PathBuf::new()))],
                "archive symbolic link target must be nonempty and relative",
            ),
            (
                "escape",
                vec![(
                    "node_modules/link",
                    ArchiveEntryKind::Symlink("../../outside".into()),
                )],
                "archive symbolic link target escapes the install root",
            ),
            (
                "root",
                vec![("directory/link", ArchiveEntryKind::Symlink("..".into()))],
                "archive symbolic link must not resolve to the install root",
            ),
            (
                "NUL",
                vec![(
                    "link",
                    ArchiveEntryKind::Symlink(PathBuf::from(OsString::from_vec(
                        b"target\0suffix".to_vec(),
                    ))),
                )],
                "archive symbolic link target contains NUL",
            ),
            (
                "too long",
                vec![("link", ArchiveEntryKind::Symlink("x".repeat(4097).into()))],
                "archive symbolic link target is too long",
            ),
            (
                "dangling",
                vec![("link", ArchiveEntryKind::Symlink("missing".into()))],
                "archive symbolic link target does not exist in the archive",
            ),
            (
                "manifest",
                vec![("link", ArchiveEntryKind::Symlink("oll.toml".into()))],
                "archive symbolic link resolves to oll.toml",
            ),
            (
                "indirect manifest",
                vec![
                    ("first", ArchiveEntryKind::Symlink("second".into())),
                    ("second", ArchiveEntryKind::Symlink("oll.toml".into())),
                ],
                "archive symbolic link resolves to oll.toml",
            ),
            (
                "cycle",
                vec![
                    ("first", ArchiveEntryKind::Symlink("second".into())),
                    ("second", ArchiveEntryKind::Symlink("first".into())),
                ],
                "archive symbolic links contain a cycle",
            ),
            (
                "symlink ancestor",
                vec![
                    ("package", ArchiveEntryKind::Symlink("target".into())),
                    ("package/index.js", ArchiveEntryKind::File),
                    ("target", ArchiveEntryKind::Directory),
                ],
                "archive entry descends through a non-directory path",
            ),
        ] {
            let mut plan = ArchivePlan::default();
            plan.insert("oll.toml".into(), ArchiveEntryKind::File, "test")
                .unwrap();
            for (path, kind) in entries {
                plan.insert(path.into(), kind, "test").unwrap();
            }

            let error = validate_archive_plan(&plan).unwrap_err();
            assert_eq!(error.code(), "archive_unsafe", "{label}");
            assert_eq!(error.message(), expected, "{label}");
        }
    }

    fn append_tar_file(
        archive: &mut tar::Builder<flate2::write::GzEncoder<File>>,
        path: &str,
        contents: &str,
    ) {
        let mut header = tar::Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_size(contents.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        archive.append(&header, contents.as_bytes()).unwrap();
    }

    fn append_tar_symlink(
        archive: &mut tar::Builder<flate2::write::GzEncoder<File>>,
        path: &str,
        target: &str,
    ) {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_path(path).unwrap();
        header.set_link_name(target).unwrap();
        header.set_size(0);
        header.set_mode(0o777);
        header.set_cksum();
        archive.append(&header, std::io::empty()).unwrap();
    }

    #[test]
    fn archive_extraction_cooperatively_stops_before_finishing_a_file() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive_path = directory.path().join("release.tar.gz");
        let install_root = directory.path().join("install");
        let publisher_source = r#"format_version = 1
[plugin]
id = "oll.archive-cancel"
name = "archive-cancel"
[source]
checkout = "source"
[runtime]
argv = ["/bin/true"]
"#;
        let publisher = PublisherManifest::parse(publisher_source).unwrap();
        let file = File::create(&archive_path).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut archive = tar::Builder::new(encoder);
        let payload = vec![42_u8; 128 * 1024];
        for (path, bytes) in [
            ("payload.bin", payload.as_slice()),
            ("oll.toml", publisher_source.as_bytes()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o600);
            header.set_cksum();
            archive.append(&header, bytes).unwrap();
        }
        archive.into_inner().unwrap().finish().unwrap();

        let checks = Cell::new(0_u32);
        let cancelled = || {
            let next = checks.get() + 1;
            checks.set(next);
            next >= 6
        };
        let error = extract_release_archive(
            &archive_path,
            ArchiveKind::TarGz,
            &install_root,
            &publisher,
            &cancelled,
        )
        .unwrap_err();

        assert_eq!(error.code(), "install_publish_failed");
        assert_eq!(error.phase(), "archive");
        assert!(checks.get() >= 4);
        assert_eq!(
            fs::metadata(install_root.join("payload.bin"))
                .unwrap()
                .len(),
            0
        );
    }
}
