mod templates;

use std::{
    ffi::CString,
    fs::{self, OpenOptions},
    io::{self, Write},
    os::unix::{ffi::OsStrExt as _, fs::OpenOptionsExt as _},
    path::Path,
};

use uuid::Uuid;

use crate::plugin::{PluginError, PluginId, PluginName};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PluginLanguage {
    Dotnet,
    Cpp,
    Go,
    Java,
    Kotlin,
    Scala,
    Clojure,
    Javascript,
    Typescript,
    Python,
    Rust,
    Swift,
    Elixir,
    Haskell,
}

pub(crate) struct GeneratedPluginProject {
    pub(crate) id: PluginId,
    pub(crate) name: PluginName,
}

pub(crate) fn scaffold_plugin_project(
    destination: &Path,
    language: PluginLanguage,
    plugin_id: Option<&str>,
    plugin_name: Option<&str>,
) -> Result<GeneratedPluginProject, PluginError> {
    if !destination.is_absolute() {
        return Err(PluginError::InvalidArgument(
            "plugin project destination must be absolute".to_owned(),
        ));
    }
    let plugin_id = match plugin_id {
        Some(plugin_id) => plugin_id.parse::<PluginId>().map_err(|error| {
            PluginError::InvalidArgument(format!("invalid plugin ID `{plugin_id}`: {error}"))
        })?,
        None => format!("generated.{}", Uuid::new_v4())
            .parse::<PluginId>()
            .expect("a UUID v4 in the generated namespace is a valid PluginId"),
    };
    let name = resolve_name(destination, plugin_name)?;
    let parent = destination.parent().ok_or_else(|| {
        PluginError::InvalidArgument("plugin project destination has no parent".to_owned())
    })?;
    let parent_metadata = fs::metadata(parent)
        .map_err(|error| PluginError::io("inspect plugin project parent", error))?;
    if !parent_metadata.is_dir() {
        return Err(PluginError::InvalidArgument(
            "plugin project parent is not a directory".to_owned(),
        ));
    }
    match fs::symlink_metadata(destination) {
        Ok(_) => {
            return Err(PluginError::AlreadyExists(format!(
                "plugin project destination already exists: {}",
                destination.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(PluginError::io("inspect plugin project destination", error)),
    }

    let basename = destination.file_name().ok_or_else(|| {
        PluginError::InvalidArgument("plugin project destination has no basename".to_owned())
    })?;
    let candidate = parent.join(format!(
        ".{}.oll-new-{}",
        basename.to_string_lossy(),
        Uuid::new_v4()
    ));
    fs::create_dir(&candidate)
        .map_err(|error| PluginError::io("create plugin project candidate", error))?;

    let result = write_and_publish(&candidate, destination, language, &plugin_id, &name);
    if result.is_err() {
        let _ = fs::remove_dir_all(&candidate);
    }
    result?;
    Ok(GeneratedPluginProject {
        id: plugin_id,
        name,
    })
}

fn resolve_name(destination: &Path, configured: Option<&str>) -> Result<PluginName, PluginError> {
    let value = match configured {
        Some(value) => value,
        None => destination
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                PluginError::InvalidArgument(
                    "a non-UTF-8 destination basename requires --name".to_owned(),
                )
            })?,
    };
    value.parse::<PluginName>().map_err(|error| {
        PluginError::InvalidArgument(format!("invalid plugin name `{value}`: {error}"))
    })
}

fn write_and_publish(
    candidate: &Path,
    destination: &Path,
    language: PluginLanguage,
    plugin_id: &PluginId,
    plugin_name: &PluginName,
) -> Result<(), PluginError> {
    for file in templates::render(language, plugin_id.as_str(), plugin_name.as_str()) {
        let path = candidate.join(&file.path);
        let parent = path.parent().expect("a generated file always has a parent");
        fs::create_dir_all(parent)
            .map_err(|error| PluginError::io("create generated project directory", error))?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&path)
            .map_err(|error| PluginError::io("create generated project file", error))?;
        output
            .write_all(file.contents.as_bytes())
            .and_then(|()| output.sync_all())
            .map_err(|error| PluginError::io("write generated project file", error))?;
    }
    sync_tree(candidate)?;
    rename_no_replace(candidate, destination)?;
    sync_directory(
        destination
            .parent()
            .expect("destination parent was validated"),
    )
}

fn sync_tree(path: &Path) -> Result<(), PluginError> {
    for entry in
        fs::read_dir(path).map_err(|error| PluginError::io("scan generated project", error))?
    {
        let entry = entry.map_err(|error| PluginError::io("scan generated project", error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| PluginError::io("inspect generated project entry", error))?;
        if file_type.is_dir() {
            sync_tree(&entry.path())?;
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> Result<(), PluginError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| PluginError::io("synchronize generated project directory", error))
}

fn rename_no_replace(source: &Path, destination: &Path) -> Result<(), PluginError> {
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| PluginError::InvalidArgument("plugin project path contains NUL".to_owned()))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| PluginError::InvalidArgument("plugin project path contains NUL".to_owned()))?;
    // Ordinary rename may replace an empty directory created by another
    // process after our existence check, so each supported Unix platform uses
    // its native no-replace publication primitive.
    #[cfg(target_os = "linux")]
    let status = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let status =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if status == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::AlreadyExists {
        Err(PluginError::AlreadyExists(
            "plugin project destination was created concurrently".to_owned(),
        ))
    } else {
        Err(PluginError::io("publish generated plugin project", error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_language_generates_a_complete_project_without_overwrite() {
        let root = tempfile::TempDir::new().unwrap();
        let languages = [
            (
                PluginLanguage::Dotnet,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Cpp,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Go,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Java,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Kotlin,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Scala,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Clojure,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Javascript,
                crate::plugin::package::SourceCheckout::Install,
            ),
            (
                PluginLanguage::Typescript,
                crate::plugin::package::SourceCheckout::Install,
            ),
            (
                PluginLanguage::Python,
                crate::plugin::package::SourceCheckout::Generation,
            ),
            (
                PluginLanguage::Rust,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Swift,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Elixir,
                crate::plugin::package::SourceCheckout::Source,
            ),
            (
                PluginLanguage::Haskell,
                crate::plugin::package::SourceCheckout::Install,
            ),
        ];
        for (index, (language, checkout)) in languages.into_iter().enumerate() {
            let destination = root.path().join(format!("plugin-{index}"));
            scaffold_plugin_project(
                &destination,
                language,
                Some(&format!("org.example.plugin-{index}")),
                Some(&format!("plugin-{index}")),
            )
            .unwrap();
            for required in ["oll.toml", "README.md", ".gitignore"] {
                assert!(destination.join(required).is_file(), "missing {required}");
            }
            let manifest = fs::read_to_string(destination.join("oll.toml")).unwrap();
            let parsed = crate::plugin::package::PublisherManifest::parse(&manifest).unwrap();
            assert_eq!(parsed.source.checkout, checkout);
            assert!(!manifest.contains("[[source."));
            assert!(!manifest.contains("{staging}"));
            assert!(
                scaffold_plugin_project(
                    &destination,
                    language,
                    Some(&format!("org.example.plugin-{index}")),
                    Some(&format!("plugin-{index}")),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn invalid_identity_leaves_no_candidate() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        assert!(
            scaffold_plugin_project(&destination, PluginLanguage::Rust, Some("invalid"), None)
                .is_err()
        );
        assert!(fs::read_dir(root.path()).unwrap().next().is_none());
    }

    #[test]
    fn omitted_identity_generates_one_immutable_uuid_identity() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        let generated =
            scaffold_plugin_project(&destination, PluginLanguage::Rust, None, None).unwrap();
        let suffix = generated
            .id
            .as_str()
            .strip_prefix("generated.")
            .expect("generated identity uses its documented namespace");
        assert_eq!(Uuid::parse_str(suffix).unwrap().get_version_num(), 4);

        let manifest = fs::read_to_string(destination.join("oll.toml")).unwrap();
        assert!(manifest.contains(&format!("id = \"{}\"", generated.id)));
    }

    #[test]
    fn cpp_scaffold_keeps_tests_out_of_install_builds() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Cpp,
            Some("org.example.cpp"),
            Some("example"),
        )
        .unwrap();

        let manifest = fs::read_to_string(destination.join("oll.toml")).unwrap();
        assert!(manifest.contains("\"-DBUILD_TESTING=OFF\""));

        let cmake = fs::read_to_string(destination.join("CMakeLists.txt")).unwrap();
        assert!(cmake.contains("option(BUILD_TESTING \"Build generated plugin tests\" OFF)"));
        assert!(cmake.contains("if(BUILD_TESTING)\n  enable_testing()"));
        assert!(!cmake.contains("include(CTest)"));
    }
}
