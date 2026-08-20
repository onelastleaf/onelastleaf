use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
};

use flate2::read::GzDecoder;
use futures_util::StreamExt;
use reqwest::redirect::{Action, Attempt, Policy};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use super::{ArchiveKind, PackageError, PublisherManifest, ReleaseArtifact};

const MAX_HTTP_REDIRECTS: usize = 10;

pub async fn stage_release_download(
    artifact: &ReleaseArtifact,
    destination: &Path,
) -> Result<(), PackageError> {
    let url = artifact.parsed_url()?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .await
        .map_err(|error| download_io("cannot create release staging file", error))?;
    let result = match url.scheme() {
        "file" => stage_file_url(&url, artifact.size_bytes, &mut file).await,
        "http" | "https" => stage_http_url(url, artifact.size_bytes, &mut file).await,
        _ => Err(PackageError::new(
            "artifact_download_failed",
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
    if size != artifact.size_bytes || crate::replica::lower_hex(&digest) != artifact.sha256 {
        drop(file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(PackageError::new(
            "artifact_checksum_mismatch",
            "download",
            "release artifact size or SHA-256 differs from oll-release.json",
        ));
    }
    Ok(())
}

async fn stage_file_url(
    url: &Url,
    expected_size: u64,
    destination: &mut tokio::fs::File,
) -> Result<(u64, [u8; 32]), PackageError> {
    let path = url.to_file_path().map_err(|()| {
        PackageError::new(
            "artifact_download_failed",
            "download",
            "file release URL is not an absolute local path",
        )
    })?;
    let metadata = tokio::fs::metadata(&path)
        .await
        .map_err(|error| download_io("cannot inspect file release artifact", error))?;
    if !metadata.is_file() {
        return Err(PackageError::new(
            "artifact_download_failed",
            "download",
            "file release URL does not name a regular file",
        ));
    }
    if metadata.len() != expected_size {
        return Err(PackageError::new(
            "artifact_checksum_mismatch",
            "download",
            "release artifact size or SHA-256 differs from oll-release.json",
        ));
    }
    let mut source = tokio::fs::File::open(path)
        .await
        .map_err(|error| download_io("cannot open file release artifact", error))?;
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .await
            .map_err(|error| download_io("cannot read file release artifact", error))?;
        if read == 0 {
            break;
        }
        let next_size = size.checked_add(read as u64).ok_or_else(|| {
            PackageError::new(
                "artifact_download_failed",
                "download",
                "release artifact size overflowed u64",
            )
        })?;
        if next_size > expected_size {
            return Err(PackageError::new(
                "artifact_checksum_mismatch",
                "download",
                "release artifact size or SHA-256 differs from oll-release.json",
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
                "artifact_download_failed",
                "download",
                "cannot initialize release HTTP client",
            )
        })?;
    let response = client.get(url).send().await.map_err(|_| {
        PackageError::new(
            "artifact_download_failed",
            "download",
            "release HTTP request failed",
        )
    })?;
    if response.status().is_redirection() {
        return Err(PackageError::new(
            "artifact_download_failed",
            "download",
            "release HTTP redirect changed to an unsupported transport",
        ));
    }
    let response = response.error_for_status().map_err(|_| {
        PackageError::new(
            "artifact_download_failed",
            "download",
            "release HTTP request failed",
        )
    })?;
    if !matches!(response.url().scheme(), "http" | "https") {
        return Err(PackageError::new(
            "artifact_download_failed",
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
                "artifact_download_failed",
                "download",
                "release HTTP response body failed",
            )
        })?;
        let next_size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
            PackageError::new(
                "artifact_download_failed",
                "download",
                "release artifact size overflowed u64",
            )
        })?;
        if next_size > expected_size {
            return Err(PackageError::new(
                "artifact_checksum_mismatch",
                "download",
                "release artifact size or SHA-256 differs from oll-release.json",
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
    let file = File::open(archive_path)
        .map_err(|error| archive_io("cannot open release tar.gz", error))?;
    let mut archive = tar::Archive::new(GzDecoder::new(file));
    let mut seen = BTreeSet::new();
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
        if !seen.insert(relative.clone()) {
            return Err(unsafe_archive(
                "tar archive contains duplicate normalized paths",
            ));
        }
        let destination = install_root.join(&relative);
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            fs::create_dir_all(&destination)
                .map_err(|error| archive_io("cannot create archive directory", error))?;
        } else if kind.is_file() {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| archive_io("cannot create archive parent", error))?;
            }
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
        } else {
            return Err(unsafe_archive(
                "tar archive contains an unsupported entry type",
            ));
        }
    }
    Ok(())
}

fn extract_zip(
    archive_path: &Path,
    install_root: &Path,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), PackageError> {
    let file =
        File::open(archive_path).map_err(|error| archive_io("cannot open release zip", error))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|_| unsafe_archive("release zip central directory is invalid"))?;
    let mut seen = BTreeSet::new();
    for index in 0..archive.len() {
        check_archive_cancellation(cancelled)?;
        let mut entry = archive
            .by_index(index)
            .map_err(|_| unsafe_archive("release zip entry is invalid"))?;
        let relative = safe_relative_path(Path::new(entry.name()))?;
        if !seen.insert(relative.clone()) {
            return Err(unsafe_archive(
                "zip archive contains duplicate normalized paths",
            ));
        }
        if entry.is_symlink() {
            return Err(unsafe_archive("zip archive links are unsupported"));
        }
        let destination = install_root.join(relative);
        if entry.is_dir() {
            fs::create_dir_all(&destination)
                .map_err(|error| archive_io("cannot create archive directory", error))?;
            continue;
        }
        if !entry.is_file() {
            return Err(unsafe_archive(
                "zip archive contains an unsupported entry type",
            ));
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| archive_io("cannot create archive parent", error))?;
        }
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
    if result.as_os_str().is_empty() {
        return Err(unsafe_archive(
            "archive entry path is empty after normalization",
        ));
    }
    Ok(result)
}

fn download_io(message: &'static str, error: io::Error) -> PackageError {
    PackageError::io("artifact_download_failed", "download", message, error)
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

    fn expected_publisher() -> PublisherManifest {
        PublisherManifest::parse(
            r#"format_version = 1
[plugin]
id = "oll.archive-test"
name = "archive-test"
[source]
checkout = "source"
[runtime]
argv = ["/bin/true"]
"#,
        )
        .unwrap()
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

        assert_eq!(error.code(), "artifact_checksum_mismatch");
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
        let artifact = ReleaseArtifact {
            target: "test-target".to_owned(),
            url: format!("http://{address}/release.tar.gz"),
            archive: ArchiveKind::TarGz,
            size_bytes: 1,
            sha256: "00".repeat(32),
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stage_release_download(&artifact, &destination),
        )
        .await
        .expect("oversized HTTP body was read to completion")
        .unwrap_err();
        server.abort();

        assert_eq!(error.code(), "artifact_checksum_mismatch");
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn http_release_never_redirects_to_a_local_file() {
        let directory = tempfile::TempDir::new().unwrap();
        let local_artifact = directory.path().join("local-release.tar.gz");
        let destination = directory.path().join("staged.tar.gz");
        let bytes = b"local artifact must not be read through a redirect";
        fs::write(&local_artifact, bytes).unwrap();
        let file_url = Url::from_file_path(&local_artifact).unwrap();
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
        let artifact = ReleaseArtifact {
            target: "test-target".to_owned(),
            url: format!("http://{address}/release.tar.gz"),
            archive: ArchiveKind::TarGz,
            size_bytes: bytes.len() as u64,
            sha256: crate::replica::lower_hex(&Sha256::digest(bytes)),
        };

        let error = stage_release_download(&artifact, &destination)
            .await
            .unwrap_err();
        server.await.unwrap();

        assert_eq!(error.code(), "artifact_download_failed");
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
        let artifact = ReleaseArtifact {
            target: "test-target".to_owned(),
            url: redirect_url,
            archive: ArchiveKind::TarGz,
            size_bytes: 0,
            sha256: crate::replica::lower_hex(&Sha256::digest([])),
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stage_release_download(&artifact, &destination),
        )
        .await
        .expect("redirect chain did not stop at its finite limit")
        .unwrap_err();
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .expect("redirect fixture did not observe the bounded chain")
            .unwrap();

        assert_eq!(error.code(), "artifact_download_failed");
        assert!(!destination.exists());
    }

    #[test]
    fn archive_paths_reject_traversal_and_normalize_dots() {
        assert!(safe_relative_path(Path::new("../escape")).is_err());
        assert!(safe_relative_path(Path::new("/absolute")).is_err());
        assert_eq!(
            safe_relative_path(Path::new("a/./b")).unwrap(),
            Path::new("a/b")
        );
    }

    #[test]
    fn tar_release_rejects_internal_symbolic_and_hard_links() {
        for (entry_type, label) in [
            (tar::EntryType::Symlink, "symlink"),
            (tar::EntryType::Link, "hardlink"),
        ] {
            let directory = tempfile::TempDir::new().unwrap();
            let archive_path = directory.path().join(format!("{label}.tar.gz"));
            let file = File::create(&archive_path).unwrap();
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
            let mut archive = tar::Builder::new(encoder);
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(entry_type);
            header.set_path(format!("{label}-entry")).unwrap();
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

            assert_eq!(error.code(), "archive_unsafe", "{label}");
            assert_eq!(
                error.message(),
                "tar archive contains an unsupported entry type"
            );
        }
    }

    #[test]
    fn zip_release_rejects_an_internal_symbolic_link() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive_path = directory.path().join("symlink.zip");
        let file = File::create(&archive_path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        archive
            .add_symlink(
                "symlink-entry",
                "internal-target",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        archive.finish().unwrap();

        let error = extract_release_archive(
            &archive_path,
            ArchiveKind::Zip,
            &directory.path().join("install"),
            &expected_publisher(),
            &|| false,
        )
        .unwrap_err();

        assert_eq!(error.code(), "archive_unsafe");
        assert_eq!(error.message(), "zip archive links are unsupported");
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
            next >= 4
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
