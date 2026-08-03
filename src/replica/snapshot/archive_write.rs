use std::{fs::File, io::Write, path::Path};

use tar::{Builder, EntryType, Header};

use super::{super::ReplicaError, CATALOG_ENTRY, MANIFEST_ENTRY, types::Manifest};

pub(super) fn build_archive(
    output: &mut File,
    manifest_path: &Path,
    catalog_path: &Path,
    manifest: &Manifest,
    staging_root: &Path,
) -> Result<(), ReplicaError> {
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)
        .map_err(|error| ReplicaError::io("initialize zstd encoder", error))?;
    encoder
        .include_checksum(true)
        .map_err(|error| ReplicaError::io("enable zstd checksum", error))?;
    {
        let mut archive = Builder::new(&mut encoder);
        append_archive_file(&mut archive, MANIFEST_ENTRY, manifest_path)?;
        append_archive_file(&mut archive, CATALOG_ENTRY, catalog_path)?;
        for document in &manifest.documents {
            append_archive_file(
                &mut archive,
                &document.entry,
                &staging_root.join(&document.entry),
            )?;
        }
        for blob in &manifest.blobs {
            append_archive_file(&mut archive, &blob.entry, &staging_root.join(&blob.entry))?;
        }
        archive
            .finish()
            .map_err(|error| ReplicaError::io("finish tar archive", error))?;
    }
    let output = encoder
        .finish()
        .map_err(|error| ReplicaError::io("finish zstd frame", error))?;
    output
        .sync_all()
        .map_err(|error| ReplicaError::io("sync snapshot temporary file", error))
}

fn append_archive_file<W: Write>(
    archive: &mut Builder<W>,
    entry_path: &str,
    source_path: &Path,
) -> Result<(), ReplicaError> {
    let mut source = File::open(source_path)
        .map_err(|error| ReplicaError::io("open staged snapshot entry", error))?;
    let size = source
        .metadata()
        .map_err(|error| ReplicaError::io("inspect staged snapshot entry", error))?
        .len();
    let mut header = Header::new_ustar();
    header.set_entry_type(EntryType::Regular);
    header.set_size(size);
    header.set_mode(0o600);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header
        .set_username("")
        .map_err(|error| ReplicaError::Internal(format!("cannot normalize tar owner: {error}")))?;
    header
        .set_groupname("")
        .map_err(|error| ReplicaError::Internal(format!("cannot normalize tar group: {error}")))?;
    header.set_cksum();
    archive
        .append_data(&mut header, entry_path, &mut source)
        .map_err(|error| ReplicaError::io("append snapshot archive entry", error))
}
