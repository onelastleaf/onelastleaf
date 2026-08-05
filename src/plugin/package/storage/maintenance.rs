use std::{collections::BTreeSet, fs, io, path::Path};

use uuid::Uuid;

use crate::plugin::{PluginId, package::PackageError};

use super::{PackageLayout, package_io, publication::sync_directory};

impl PackageLayout {
    pub fn pending_generation(
        &self,
        plugin_id: &PluginId,
        generation: Uuid,
    ) -> Option<std::path::PathBuf> {
        let candidate = self
            .plugin_root(plugin_id)
            .join("candidates")
            .join(generation.to_string());
        if candidate.is_dir() {
            return Some(candidate);
        }
        let published = self.generation(plugin_id, generation);
        published.is_dir().then_some(published)
    }

    pub fn discard_unpublished_generation(
        &self,
        plugin_id: &PluginId,
        generation: Uuid,
    ) -> Result<(), PackageError> {
        if self.current_generation(plugin_id)? == Some(generation) {
            return Err(PackageError::new(
                "install_publish_failed",
                "recovery",
                "cannot discard the current plugin generation",
            ));
        }
        let plugin_root = self.plugin_root(plugin_id);
        remove_private_tree(&plugin_root.join("candidates").join(generation.to_string()))?;
        remove_private_tree(&self.generation(plugin_id, generation))?;
        for directory in [
            plugin_root.join("candidates"),
            plugin_root.join("generations"),
        ] {
            if directory.is_dir() {
                sync_directory(&directory)?;
            }
        }
        Ok(())
    }

    pub fn remove_candidate(&self, plugin_id: &PluginId, generation: Uuid) {
        let _ = fs::remove_dir_all(
            self.plugin_root(plugin_id)
                .join("candidates")
                .join(generation.to_string()),
        );
    }

    pub fn cleanup_incomplete_staging(
        &self,
        authoritative_plugin_ids: &BTreeSet<PluginId>,
        retained_candidates: &BTreeSet<(PluginId, Uuid)>,
    ) -> Result<(), PackageError> {
        for entry in fs::read_dir(&self.root)
            .map_err(|error| package_io("scan plugin package root", error))?
        {
            let entry = entry.map_err(|error| package_io("scan plugin package entry", error))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(".discovery-") || name.starts_with(".trash-") {
                remove_private_tree(&entry.path())?;
                continue;
            }
            let Ok(plugin_id) = name.parse::<PluginId>() else {
                continue;
            };
            let file_type = entry
                .file_type()
                .map_err(|error| package_io("inspect plugin package entry", error))?;
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            if !authoritative_plugin_ids.contains(&plugin_id) {
                remove_private_tree(&entry.path())?;
                continue;
            }
            for child in fs::read_dir(entry.path())
                .map_err(|error| package_io("scan plugin staging entry", error))?
            {
                let child =
                    child.map_err(|error| package_io("scan plugin staging entry", error))?;
                if child
                    .file_name()
                    .to_str()
                    .is_some_and(|value| value.starts_with(".operation-"))
                {
                    remove_private_tree(&child.path())?;
                }
            }
            let candidates = entry.path().join("candidates");
            let entries = match fs::read_dir(&candidates) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(package_io("scan plugin candidates", error)),
            };
            for candidate in entries {
                let candidate =
                    candidate.map_err(|error| package_io("scan plugin candidate", error))?;
                let retained = candidate
                    .file_name()
                    .to_str()
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .is_some_and(|generation| {
                        retained_candidates.contains(&(plugin_id.clone(), generation))
                    });
                if !retained {
                    remove_private_tree(&candidate.path())?;
                }
            }
        }
        Ok(())
    }

    pub fn prune_generations(
        &self,
        plugin_id: &PluginId,
        retained: &BTreeSet<Uuid>,
    ) -> Result<(), PackageError> {
        let root = self.plugin_root(plugin_id).join("generations");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(package_io("scan plugin generations", error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| package_io("scan plugin generation", error))?;
            let keep = entry
                .file_name()
                .to_str()
                .and_then(|value| Uuid::parse_str(value).ok())
                .is_some_and(|generation| retained.contains(&generation));
            if !keep {
                remove_private_tree(&entry.path())?;
            }
        }
        sync_directory(&root)
    }

    pub fn move_plugin_to(&self, plugin_id: &PluginId, trash: &Path) -> Result<bool, PackageError> {
        let root = self.plugin_root(plugin_id);
        if !root.exists() {
            return Ok(false);
        }
        if trash.exists() {
            return Err(PackageError::new(
                "install_publish_failed",
                "removal",
                "plugin removal trash path already exists",
            ));
        }
        fs::rename(root, trash)
            .map_err(|error| package_io("move plugin package tree to trash", error))?;
        sync_directory(&self.root)?;
        Ok(true)
    }
}

fn remove_private_tree(path: &Path) -> Result<(), PackageError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(package_io("remove incomplete plugin staging", error)),
    }
}
