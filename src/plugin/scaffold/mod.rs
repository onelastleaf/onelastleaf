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
                "dotnet build",
            ),
            (
                PluginLanguage::Cpp,
                crate::plugin::package::SourceCheckout::Source,
                "cmake -S",
            ),
            (
                PluginLanguage::Go,
                crate::plugin::package::SourceCheckout::Source,
                "go build",
            ),
            (
                PluginLanguage::Java,
                crate::plugin::package::SourceCheckout::Source,
                "gradle --no-daemon classes",
            ),
            (
                PluginLanguage::Kotlin,
                crate::plugin::package::SourceCheckout::Source,
                "gradle --no-daemon classes",
            ),
            (
                PluginLanguage::Scala,
                crate::plugin::package::SourceCheckout::Source,
                "gradle --no-daemon classes",
            ),
            (
                PluginLanguage::Clojure,
                crate::plugin::package::SourceCheckout::Source,
                "gradle --no-daemon classes",
            ),
            (
                PluginLanguage::Javascript,
                crate::plugin::package::SourceCheckout::Install,
                "pnpm install",
            ),
            (
                PluginLanguage::Typescript,
                crate::plugin::package::SourceCheckout::Install,
                "pnpm run build",
            ),
            (
                PluginLanguage::Python,
                crate::plugin::package::SourceCheckout::Generation,
                "uv sync --extra dev",
            ),
            (
                PluginLanguage::Rust,
                crate::plugin::package::SourceCheckout::Source,
                "cargo build --release",
            ),
            (
                PluginLanguage::Swift,
                crate::plugin::package::SourceCheckout::Source,
                "swift build --configuration release",
            ),
            (
                PluginLanguage::Elixir,
                crate::plugin::package::SourceCheckout::Source,
                "mix compile",
            ),
            (
                PluginLanguage::Haskell,
                crate::plugin::package::SourceCheckout::Install,
                "cabal build",
            ),
        ];
        let mut readmes = std::collections::HashSet::new();
        for (index, (language, checkout, build_command)) in languages.into_iter().enumerate() {
            let destination = root.path().join(format!("plugin-{index}"));
            scaffold_plugin_project(
                &destination,
                language,
                Some(&format!("org.example.plugin-{index}")),
                Some("example"),
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
            let readme = fs::read_to_string(destination.join("README.md")).unwrap();
            assert!(readme.contains("## Requirements"));
            assert!(readme.contains("## Build and test"));
            assert!(readme.contains(build_command));
            assert!(readme.to_lowercase().contains("incremental"));
            assert!(readme.contains("## Local source development"));
            assert!(readme.contains("oll plugin install . --source"));
            assert!(readme.contains("## Local release development"));
            assert!(readme.contains("oll plugin install . --release dev"));
            assert!(readme.contains("## Development loop"));
            assert!(readme.contains("## Publish a release"));
            assert!(readme.contains("oll plugin install <git-remote> --release <release-id>"));
            assert!(!readme.contains("__OLL_"));
            assert!(readmes.insert(readme), "language templates shared a README");
            for entry in walkdir::WalkDir::new(&destination) {
                let entry = entry.unwrap();
                if entry.file_type().is_file() {
                    let contents = fs::read_to_string(entry.path()).unwrap();
                    assert!(
                        !contents.contains("__OLL_"),
                        "unrendered placeholder in {}",
                        entry.path().display()
                    );
                }
            }
            assert!(
                scaffold_plugin_project(
                    &destination,
                    language,
                    Some(&format!("org.example.plugin-{index}")),
                    Some("example"),
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

    #[test]
    fn rust_scaffold_uses_the_public_action_result_constructor() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Rust,
            Some("org.example.rust"),
            Some("example"),
        )
        .unwrap();

        let main = fs::read_to_string(destination.join("src/main.rs")).unwrap();
        assert!(main.contains("ActionResult::value(example::echo(arguments))"));
        assert!(!main.contains("ActionResult {"));
    }

    #[test]
    fn dotnet_scaffold_uses_the_published_action_result_constructor() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Dotnet,
            Some("org.example.dotnet"),
            Some("example"),
        )
        .unwrap();

        let program = fs::read_to_string(destination.join("src/Program.cs")).unwrap();
        assert!(program.contains("ActionResult.FromString"));
        assert!(!program.contains("ActionResult.String"));
    }

    #[test]
    fn typescript_scaffold_accepts_readonly_action_arguments() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Typescript,
            Some("org.example.typescript"),
            Some("example"),
        )
        .unwrap();

        let echo = fs::read_to_string(destination.join("src/echo.ts")).unwrap();
        assert!(echo.contains("arguments_: readonly string[]"));
    }

    #[test]
    fn generated_node_and_python_projects_use_the_documented_package_managers() {
        let root = tempfile::TempDir::new().unwrap();
        for (directory, language) in [
            ("javascript", PluginLanguage::Javascript),
            ("typescript", PluginLanguage::Typescript),
        ] {
            let destination = root.path().join(directory);
            scaffold_plugin_project(
                &destination,
                language,
                Some(&format!("org.example.{directory}")),
                Some(directory),
            )
            .unwrap();

            let manifest = fs::read_to_string(destination.join("oll.toml")).unwrap();
            assert!(manifest.contains("[\"pnpm\", \"install\", \"--frozen-lockfile\"]"));
            assert!(destination.join("pnpm-lock.yaml").is_file());
            let workspace = fs::read_to_string(destination.join("pnpm-workspace.yaml")).unwrap();
            assert!(workspace.contains("packageImportMethod: copy"));
            assert!(workspace.contains("enableGlobalVirtualStore: false"));
            assert!(workspace.contains("protobufjs: false"));
            let readme = fs::read_to_string(destination.join("README.md")).unwrap();
            assert!(readme.contains("--config.package-import-method=copy"));
            assert!(readme.contains("--config.enable-global-virtual-store=false"));
            if language == PluginLanguage::Typescript {
                assert!(workspace.contains("esbuild: true"));
            }
        }

        let destination = root.path().join("python");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Python,
            Some("org.example.python"),
            Some("python"),
        )
        .unwrap();
        let manifest = fs::read_to_string(destination.join("oll.toml")).unwrap();
        assert!(manifest.contains("[\"uv\", \"sync\", \"--no-dev\""));
        assert!(!manifest.contains("pip"));
        let readme = fs::read_to_string(destination.join("README.md")).unwrap();
        assert!(readme.contains("uv export --locked --no-dev --no-emit-project --no-hashes"));
    }

    #[test]
    fn go_scaffold_includes_the_published_sdk_module_graph() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Go,
            Some("org.example.go"),
            Some("example"),
        )
        .unwrap();

        let module = fs::read_to_string(destination.join("go.mod")).unwrap();
        let sums = fs::read_to_string(destination.join("go.sum")).unwrap();
        assert!(module.contains("github.com/onelastleaf/go-plugin-sdk v0.1.0"));
        assert!(module.contains("google.golang.org/grpc v1.75.1 // indirect"));
        assert!(sums.contains("github.com/onelastleaf/go-plugin-sdk v0.1.0 h1:"));
    }

    #[test]
    fn swift_scaffold_matches_the_published_sdk_toolchain() {
        let root = tempfile::TempDir::new().unwrap();
        let destination = root.path().join("example");
        scaffold_plugin_project(
            &destination,
            PluginLanguage::Swift,
            Some("org.example.swift"),
            Some("example"),
        )
        .unwrap();

        let package = fs::read_to_string(destination.join("Package.swift")).unwrap();
        assert!(package.starts_with("// swift-tools-version: 6.2\n"));
        assert!(package.contains(
            ".package(url: \"https://github.com/onelastleaf/swift-plugin-sdk.git\", exact: \"0.1.0\")"
        ));

        let readme = fs::read_to_string(destination.join("README.md")).unwrap();
        assert!(readme.contains("Swift 6.2 or newer"));
        assert!(readme.contains("macOS 15 or newer when building for macOS"));
    }
}
