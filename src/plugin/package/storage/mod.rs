use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::plugin::PluginId;

use super::PackageError;

mod maintenance;
mod publication;

#[derive(Clone, Debug)]
pub struct PackageLayout {
    root: PathBuf,
}

impl PackageLayout {
    pub fn initialize(root: PathBuf) -> Result<Self, PackageError> {
        ensure_private_directory(&root)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn plugin_root(&self, plugin_id: &PluginId) -> PathBuf {
        self.root.join(plugin_id.as_str())
    }

    pub fn resolution_directory(&self, operation_id: &str) -> Result<PathBuf, PackageError> {
        let path = self.root.join(format!(".resolve-{operation_id}"));
        create_private_directory(&path)?;
        Ok(path)
    }

    pub fn operation_staging(
        &self,
        plugin_id: &PluginId,
        operation_id: &str,
    ) -> Result<PathBuf, PackageError> {
        let root = self.prepare_plugin_root(plugin_id)?;
        let path = root.join(format!(".operation-{operation_id}"));
        create_private_directory(&path)?;
        Ok(path)
    }

    pub fn candidate(
        &self,
        plugin_id: &PluginId,
        generation: Uuid,
    ) -> Result<PathBuf, PackageError> {
        let plugin_root = self.prepare_plugin_root(plugin_id)?;
        let path = plugin_root.join("candidates").join(generation.to_string());
        fs::create_dir(&path).map_err(|error| package_io("create package candidate", error))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|error| package_io("restrict package candidate", error))?;
        Ok(path)
    }

    pub fn generation(&self, plugin_id: &PluginId, generation: Uuid) -> PathBuf {
        self.plugin_root(plugin_id)
            .join("generations")
            .join(generation.to_string())
    }

    pub fn direct_generation(
        &self,
        plugin_id: &PluginId,
        generation: Uuid,
    ) -> Result<PathBuf, PackageError> {
        let plugin_root = self.prepare_plugin_root(plugin_id)?;
        let path = plugin_root.join("generations").join(generation.to_string());
        fs::create_dir(&path)
            .map_err(|error| package_io("create direct package generation", error))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|error| package_io("restrict direct package generation", error))?;
        Ok(path)
    }

    pub fn build_log(
        &self,
        plugin_id: &PluginId,
        operation_id: &str,
    ) -> Result<PathBuf, PackageError> {
        let root = self.prepare_plugin_root(plugin_id)?.join("build-logs");
        Ok(root.join(format!("{operation_id}.log")))
    }

    fn prepare_plugin_root(&self, plugin_id: &PluginId) -> Result<PathBuf, PackageError> {
        let root = self.plugin_root(plugin_id);
        ensure_private_directory(&root)?;
        ensure_private_directory(&root.join("candidates"))?;
        ensure_private_directory(&root.join("generations"))?;
        ensure_private_directory(&root.join("build-logs"))?;
        Ok(root)
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), PackageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(PackageError::new(
                "install_publish_failed",
                "storage",
                format!(
                    "plugin package path {} is not a real directory",
                    path.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .map_err(|error| package_io("create plugin package directory", error))?;
        }
        Err(error) => return Err(package_io("inspect plugin package directory", error)),
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| package_io("restrict plugin package directory", error))
}

fn create_private_directory(path: &Path) -> Result<(), PackageError> {
    fs::create_dir(path).map_err(|error| package_io("create private package staging", error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| package_io("restrict private package staging", error))
}

fn package_io(operation: &'static str, error: io::Error) -> PackageError {
    PackageError::io("install_publish_failed", "storage", operation, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, os::unix::fs::symlink};

    #[test]
    fn publication_uses_plugin_id_and_atomic_relative_current_symlink() {
        let directory = tempfile::TempDir::new().unwrap();
        let layout = PackageLayout::initialize(directory.path().join("plugins")).unwrap();
        let plugin_id: PluginId = "oll.test".parse().unwrap();
        let generation = Uuid::new_v4();
        let candidate = layout.candidate(&plugin_id, generation).unwrap();
        fs::write(candidate.join("entrypoint"), b"test").unwrap();
        layout
            .publish_candidate(&plugin_id, generation, None)
            .unwrap();
        assert_eq!(
            layout.current_generation(&plugin_id).unwrap(),
            Some(generation)
        );
        assert_eq!(
            fs::read_link(layout.plugin_root(&plugin_id).join("current")).unwrap(),
            Path::new("generations").join(generation.to_string())
        );
    }

    #[test]
    fn completed_pending_tree_is_synchronized_recursively() {
        let directory = tempfile::TempDir::new().unwrap();
        let layout = PackageLayout::initialize(directory.path().join("plugins")).unwrap();
        let plugin_id: PluginId = "oll.test".parse().unwrap();
        let generation = Uuid::new_v4();
        let candidate = layout.candidate(&plugin_id, generation).unwrap();
        let nested = candidate.join("share").join("data");
        fs::create_dir_all(&nested).unwrap();
        fs::write(candidate.join("entrypoint"), b"executable").unwrap();
        fs::write(nested.join("index"), b"contents").unwrap();
        symlink(
            directory.path().join("missing-target"),
            candidate.join("optional-link"),
        )
        .unwrap();

        layout.sync_pending_tree(&plugin_id, generation).unwrap();
    }

    #[test]
    fn startup_cleanup_removes_staging_and_only_unretained_candidates() {
        let directory = tempfile::TempDir::new().unwrap();
        let layout = PackageLayout::initialize(directory.path().join("plugins")).unwrap();
        let plugin_id: PluginId = "oll.cleanup-test".parse().unwrap();
        let orphan_id: PluginId = "oll.orphan-test".parse().unwrap();
        let discovery = layout
            .resolution_directory("interrupted-discovery")
            .unwrap();
        let operation = layout
            .operation_staging(&plugin_id, "interrupted-operation")
            .unwrap();
        let discarded = Uuid::new_v4();
        let retained = Uuid::new_v4();
        layout.candidate(&plugin_id, discarded).unwrap();
        layout.candidate(&plugin_id, retained).unwrap();
        let orphan_generation = Uuid::new_v4();
        layout.candidate(&orphan_id, orphan_generation).unwrap();
        layout
            .publish_candidate(&orphan_id, orphan_generation, None)
            .unwrap();
        fs::write(discovery.join("partial"), b"partial").unwrap();
        fs::write(operation.join("partial"), b"partial").unwrap();
        let retained_candidates = BTreeSet::from([(plugin_id.clone(), retained)]);
        let authoritative_plugin_ids = BTreeSet::from([plugin_id.clone()]);

        layout
            .cleanup_incomplete_staging(&authoritative_plugin_ids, &retained_candidates)
            .unwrap();

        assert!(!discovery.exists());
        assert!(!operation.exists());
        assert!(layout.pending_generation(&plugin_id, discarded).is_none());
        assert!(layout.pending_generation(&plugin_id, retained).is_some());
        assert!(!layout.plugin_root(&orphan_id).exists());
    }
}
