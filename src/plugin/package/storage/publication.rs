use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::fs::{OpenOptionsExt, symlink},
    path::Path,
};

use uuid::Uuid;

use crate::plugin::PluginId;

use super::{PackageLayout, package_io};
use crate::plugin::package::PackageError;

impl PackageLayout {
    pub fn current_generation(&self, plugin_id: &PluginId) -> Result<Option<Uuid>, PackageError> {
        let current = self.plugin_root(plugin_id).join("current");
        let target = match fs::read_link(&current) {
            Ok(target) => target,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(package_io("read plugin current symlink", error)),
        };
        let mut components = target.components();
        if components.next().map(|part| part.as_os_str()) != Some("generations".as_ref())
            || components.clone().count() != 1
        {
            return Err(PackageError::new(
                "install_publish_failed",
                "publication",
                "plugin current symlink has an invalid target",
            ));
        }
        let generation = components
            .next()
            .and_then(|part| part.as_os_str().to_str())
            .and_then(|value| Uuid::parse_str(value).ok())
            .filter(|value| value.get_version_num() == 4)
            .ok_or_else(|| {
                PackageError::new(
                    "install_publish_failed",
                    "publication",
                    "plugin current symlink does not name a UUID-v4 generation",
                )
            })?;
        Ok(Some(generation))
    }

    pub fn publish_candidate(
        &self,
        plugin_id: &PluginId,
        candidate_generation: Uuid,
        expected_current: Option<Uuid>,
    ) -> Result<(), PackageError> {
        if self.current_generation(plugin_id)? != expected_current {
            return Err(PackageError::new(
                "install_publish_failed",
                "publication",
                "plugin current generation changed before publication",
            ));
        }
        let plugin_root = self.prepare_plugin_root(plugin_id)?;
        let candidate = plugin_root
            .join("candidates")
            .join(candidate_generation.to_string());
        let generation = plugin_root
            .join("generations")
            .join(candidate_generation.to_string());
        if !generation.exists() {
            fs::rename(&candidate, &generation)
                .map_err(|error| package_io("move package candidate to generations", error))?;
            sync_directory(&plugin_root.join("generations"))?;
        }
        self.replace_current(plugin_id, expected_current, Some(candidate_generation))
    }

    pub fn sync_candidate_tree(
        &self,
        plugin_id: &PluginId,
        candidate_generation: Uuid,
    ) -> Result<(), PackageError> {
        let plugin_root = self.plugin_root(plugin_id);
        let candidates = plugin_root.join("candidates");
        let candidate = candidates.join(candidate_generation.to_string());
        sync_tree(&candidate)?;

        // A durable intent may recover only paths whose complete ancestry was
        // already made durable. Synchronize from the candidate's parent back
        // through the package root after every child has reached storage.
        sync_directory(&candidates)?;
        sync_directory(&plugin_root)?;
        sync_directory(&self.root)
    }

    pub fn replace_current(
        &self,
        plugin_id: &PluginId,
        expected_current: Option<Uuid>,
        replacement: Option<Uuid>,
    ) -> Result<(), PackageError> {
        if self.current_generation(plugin_id)? != expected_current {
            return Err(PackageError::new(
                "install_publish_failed",
                "publication",
                "plugin current generation changed before publication",
            ));
        }
        let plugin_root = self.prepare_plugin_root(plugin_id)?;
        let current = plugin_root.join("current");
        let Some(replacement) = replacement else {
            match fs::remove_file(&current) {
                Ok(()) => sync_directory(&plugin_root)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(package_io("remove stale plugin current symlink", error));
                }
            }
            return Ok(());
        };
        if !self.generation(plugin_id, replacement).is_dir() {
            return Err(PackageError::new(
                "install_publish_failed",
                "publication",
                "replacement plugin generation is missing",
            ));
        }
        let temporary = plugin_root.join(format!(".current.{}.tmp", Uuid::new_v4()));
        symlink(
            Path::new("generations").join(replacement.to_string()),
            &temporary,
        )
        .map_err(|error| package_io("create temporary plugin current symlink", error))?;
        let result = fs::rename(&temporary, &current)
            .map_err(|error| package_io("publish plugin current symlink", error));
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        sync_directory(&plugin_root)?;
        Ok(())
    }
}

pub(super) fn sync_directory(path: &Path) -> Result<(), PackageError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| package_io("synchronize plugin package directory", error))
}

fn sync_tree(path: &Path) -> Result<(), PackageError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| package_io("inspect plugin package candidate", error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(PackageError::new(
            "install_publish_failed",
            "storage",
            "plugin package candidate is not a real directory",
        ));
    }

    for entry in
        fs::read_dir(path).map_err(|error| package_io("scan plugin package candidate", error))?
    {
        let entry = entry.map_err(|error| package_io("scan plugin package candidate", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| package_io("inspect plugin package candidate entry", error))?;
        if file_type.is_dir() {
            sync_tree(&entry.path())?;
        } else if file_type.is_file() {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(entry.path())
                .and_then(|file| file.sync_all())
                .map_err(|error| package_io("synchronize plugin package file", error))?;
        }
        // Symlinks and other non-regular entries have no file contents to
        // flush. Their directory entries are covered by the parent fsync.
    }
    sync_directory(path)
}
