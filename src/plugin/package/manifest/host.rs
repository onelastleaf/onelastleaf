use std::{
    env,
    ffi::OsStr,
    path::{Component, Path, PathBuf},
};

use super::PackageError;

pub fn executable_exists(executable: &str, cwd: &Path) -> bool {
    executable_exists_on_path(executable, cwd, env::var_os("PATH").as_deref())
}

fn executable_exists_on_path(executable: &str, cwd: &Path, path_env: Option<&OsStr>) -> bool {
    let path = Path::new(executable);
    if path.is_absolute() {
        return executable_file(path);
    }
    if executable.contains(std::path::MAIN_SEPARATOR) {
        return executable_file(&cwd.join(path));
    }
    let Some(path_env) = path_env else {
        return false;
    };
    env::split_paths(path_env).any(|directory| {
        let directory = if directory.is_absolute() {
            directory
        } else {
            cwd.join(directory)
        };
        executable_file(&directory.join(executable))
    })
}

#[cfg(unix)]
fn executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn executable_file(path: &Path) -> bool {
    path.is_file()
}

pub(crate) fn ensure_contained_path(
    root: &Path,
    candidate: &Path,
) -> Result<PathBuf, PackageError> {
    let root = lexical_normalize(root);
    let candidate = lexical_normalize(candidate);
    if !candidate.starts_with(&root) {
        return Err(PackageError::entrypoint(
            "runtime path escapes its permitted root",
        ));
    }

    let canonical_root = std::fs::canonicalize(&root).map_err(|error| {
        PackageError::io(
            "entrypoint_invalid",
            "validation",
            "cannot resolve runtime path root",
            error,
        )
    })?;
    let mut existing = candidate.as_path();
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing.parent().ok_or_else(|| {
                    PackageError::entrypoint("runtime path has no existing permitted ancestor")
                })?;
            }
            Err(error) => {
                return Err(PackageError::io(
                    "entrypoint_invalid",
                    "validation",
                    "cannot inspect runtime path",
                    error,
                ));
            }
        }
    }
    let canonical_existing = std::fs::canonicalize(existing).map_err(|error| {
        PackageError::io(
            "entrypoint_invalid",
            "validation",
            "cannot resolve runtime path",
            error,
        )
    })?;
    if !canonical_existing.starts_with(&canonical_root) {
        return Err(PackageError::entrypoint(
            "runtime path resolves outside its permitted root",
        ));
    }
    Ok(candidate)
}

pub fn validate_local_package_config(config_root: &Path) -> Result<(), PackageError> {
    super::super::read_plugin_declarations(config_root)?;
    let mask_root = config_root.join("plugin-masks");
    let entries = match std::fs::read_dir(&mask_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(PackageError::io(
                "mask_invalid",
                "mask",
                "cannot read plugin-masks directory",
                error,
            ));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            PackageError::io("mask_invalid", "mask", "cannot inspect plugin mask", error)
        })?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Err(PackageError::mask(
                "plugin mask filenames must be valid UTF-8",
            ));
        };
        if !file_name.ends_with(".toml") {
            continue;
        }
        let plugin_id = file_name
            .strip_suffix(".toml")
            .expect("checked suffix")
            .parse()
            .map_err(|error: String| {
                PackageError::mask(format!(
                    "plugin mask filename has invalid PluginId: {error}"
                ))
            })?;
        super::super::build::read_mask(config_root, &plugin_id)?;
    }
    Ok(())
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn executable_lookup_resolves_relative_and_empty_path_entries_from_child_cwd() {
        use std::{fs, os::unix::fs::PermissionsExt};

        fn create_executable(path: &Path) {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"not executed").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }

        let directory = tempfile::TempDir::new().unwrap();
        let child_cwd = directory.path().join("child-cwd");
        let absolute_bin = directory.path().join("absolute-bin");
        create_executable(&child_cwd.join("relative-bin/relative-tool"));
        create_executable(&child_cwd.join("empty-tool"));
        create_executable(&absolute_bin.join("absolute-tool"));

        assert!(executable_exists_on_path(
            "relative-tool",
            &child_cwd,
            Some(OsStr::new("relative-bin")),
        ));
        assert!(executable_exists_on_path(
            "empty-tool",
            &child_cwd,
            Some(OsStr::new("")),
        ));
        let absolute_path = env::join_paths([&absolute_bin]).unwrap();
        assert!(executable_exists_on_path(
            "absolute-tool",
            &child_cwd,
            Some(&absolute_path),
        ));
    }
}
