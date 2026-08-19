use std::{fs, path::Path};

use crate::plugin::PluginId;
use crate::plugin::package::{
    ArchiveKind, EffectiveManifest, ExpansionPaths, ManifestMask, PackageError, SourceCheckout,
    executable_exists, mask_path,
};

pub(in crate::plugin::package) fn read_mask(
    config_root: &Path,
    plugin_id: &PluginId,
) -> Result<Option<ManifestMask>, PackageError> {
    let mask_root = config_root.join("plugin-masks");
    let canonical_config_root = fs::canonicalize(config_root).map_err(|error| {
        PackageError::io(
            "mask_invalid",
            "mask",
            "cannot resolve plugin configuration root",
            error,
        )
    })?;
    let canonical_mask_root = match fs::canonicalize(&mask_root) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(PackageError::io(
                "mask_invalid",
                "mask",
                "cannot resolve plugin mask directory",
                error,
            ));
        }
    };
    if !canonical_mask_root.starts_with(&canonical_config_root) {
        return Err(PackageError::mask(
            "plugin-masks directory escapes the configuration root",
        ));
    }
    let path = mask_path(config_root, plugin_id);
    let canonical_path = match fs::canonicalize(&path) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(PackageError::io(
                "mask_invalid",
                "mask",
                "cannot resolve plugin mask",
                error,
            ));
        }
    };
    if !canonical_path.starts_with(&canonical_mask_root) {
        return Err(PackageError::mask("plugin mask escapes plugin-masks"));
    }
    let source = fs::read_to_string(&canonical_path).map_err(|error| {
        PackageError::io("mask_invalid", "mask", "cannot read plugin mask", error)
    })?;
    ManifestMask::parse(&source).map(Some)
}

pub(super) fn check_dependencies(
    effective: &EffectiveManifest,
    source_root: &Path,
) -> Result<(), PackageError> {
    for dependency in &effective.source.dependencies {
        if !executable_exists(&dependency.executable, source_root) {
            return Err(PackageError::new(
                "dependency_missing",
                "dependency",
                format!(
                    "required executable {} is unavailable",
                    dependency.executable
                ),
            )
            .with_hint(dependency.hint.clone()));
        }
    }
    Ok(())
}

pub(super) fn validate_runtime(
    effective: &EffectiveManifest,
    install: &Path,
    mask_dir: &Path,
) -> Result<(), PackageError> {
    let paths = ExpansionPaths {
        source: None,
        install: (effective.source.checkout != SourceCheckout::Generation).then_some(install),
        generation: (effective.source.checkout == SourceCheckout::Generation).then_some(install),
        mask_dir,
    };
    let argv = effective.expanded_runtime_argv(&paths)?;
    for (template, expanded) in effective.runtime.argv.iter().zip(&argv) {
        for (placeholder, root) in [
            ("{install}", install),
            ("{generation}", install),
            ("{mask_dir}", mask_dir),
        ] {
            if template.contains(placeholder) {
                let root = root.to_str().ok_or_else(|| {
                    PackageError::entrypoint("runtime path root is not valid UTF-8")
                })?;
                let start = expanded.find(root).ok_or_else(|| {
                    PackageError::entrypoint("runtime path placeholder was not expanded")
                })?;
                super::super::manifest::ensure_contained_path(
                    Path::new(root),
                    Path::new(&expanded[start..]),
                )?;
            }
        }
    }
    let executable = Path::new(&argv[0]);
    if executable.is_absolute() {
        if !executable_exists(&argv[0], install) {
            return Err(PackageError::entrypoint(
                "runtime executable is missing or not executable",
            ));
        }
    } else if argv[0].contains(std::path::MAIN_SEPARATOR) {
        let resolved =
            super::super::manifest::ensure_contained_path(install, &install.join(executable))?;
        let resolved = resolved.to_str().ok_or_else(|| {
            PackageError::entrypoint("runtime executable path is not valid UTF-8")
        })?;
        if !executable_exists(resolved, install) {
            return Err(PackageError::entrypoint(
                "runtime executable is missing or not executable",
            ));
        }
    } else if !executable_exists(&argv[0], install) {
        return Err(PackageError::entrypoint(
            "runtime executable is unavailable through PATH",
        ));
    }
    Ok(())
}

pub(super) fn validate_platform_archive(kind: ArchiveKind) -> Result<(), PackageError> {
    match (std::env::consts::OS, kind) {
        ("linux", ArchiveKind::TarGz) | ("macos", ArchiveKind::Zip) => Ok(()),
        _ => Err(PackageError::new(
            "artifact_unavailable",
            "release_index",
            "release archive kind is not supported on this node platform",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn mask_directory_must_remain_inside_the_configuration_root() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::TempDir::new().unwrap();
        let config_root = directory.path().join("config");
        let external = directory.path().join("external-masks");
        fs::create_dir(&config_root).unwrap();
        fs::create_dir(&external).unwrap();
        symlink(&external, config_root.join("plugin-masks")).unwrap();
        let plugin_id: PluginId = "oll.mask-root-escape".parse().unwrap();

        let missing_leaf = read_mask(&config_root, &plugin_id).unwrap_err();
        assert_eq!(missing_leaf.code(), "mask_invalid");
        assert_eq!(
            missing_leaf.message(),
            "plugin-masks directory escapes the configuration root"
        );

        fs::write(external.join("oll.mask-root-escape.toml"), "").unwrap();
        let existing_leaf = read_mask(&config_root, &plugin_id).unwrap_err();
        assert_eq!(existing_leaf.code(), "mask_invalid");
        assert_eq!(
            existing_leaf.message(),
            "plugin-masks directory escapes the configuration root"
        );
    }
}
