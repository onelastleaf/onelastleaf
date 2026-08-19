use std::{collections::BTreeMap, path::Path, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    plugin::{PluginId, PluginName},
    protocol::PROTOCOL_SCHEMA_SHA256,
};

use super::PackageError;

mod arguments;
mod host;

pub use arguments::ExpansionPaths;
use arguments::{
    expand_argv, validate_mask_runtime_placeholders, validate_mask_step_placeholders,
    validate_runtime_placeholders, validate_step_placeholders,
};
pub(crate) use host::ensure_contained_path;
pub use host::{executable_exists, validate_local_package_config};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherManifest {
    pub format_version: u32,
    pub plugin: PublisherPlugin,
    pub source: SourceRecipe,
    pub runtime: RuntimeRecipe,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherPlugin {
    pub id: String,
    pub name: String,
    pub protocol_fingerprint: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceCheckout {
    Source,
    Install,
    Generation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRecipe {
    pub checkout: SourceCheckout,
    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
    #[serde(default)]
    pub steps: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRecipe {
    pub argv: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManifestMask {
    pub format_version: u32,
    #[serde(default)]
    pub plugin: Option<PluginMask>,
    #[serde(default)]
    pub source: Option<SourceMask>,
    #[serde(default)]
    pub runtime: Option<RuntimeMask>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PluginMask {
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceMask {
    pub dependencies: Option<BTreeMap<String, String>>,
    pub steps: Option<Vec<Vec<String>>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeMask {
    pub argv: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveManifest {
    pub plugin_id: String,
    pub plugin_name: String,
    pub protocol_fingerprint: String,
    pub source: SourceRecipe,
    pub runtime: RuntimeRecipe,
}

impl PublisherManifest {
    pub fn parse(source: &str) -> Result<Self, PackageError> {
        let manifest: Self = toml::from_str(source).map_err(|error: toml::de::Error| {
            let location = error
                .span()
                .map(|span| format!(" at byte range {}..{}", span.start, span.end))
                .unwrap_or_default();
            PackageError::manifest(format!("invalid oll.toml syntax or schema{location}"))
        })?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), PackageError> {
        if self.format_version != 1 {
            return Err(PackageError::manifest("oll.toml format_version must be 1"));
        }
        PluginId::from_str(&self.plugin.id)
            .map_err(|error| PackageError::manifest(error.to_string()))?;
        PluginName::from_str(&self.plugin.name)
            .map_err(|error| PackageError::manifest(error.to_string()))?;
        validate_fingerprint(&self.plugin.protocol_fingerprint)?;
        validate_dependencies(&self.source.dependencies)?;
        validate_steps(&self.source.steps)?;
        validate_argv(&self.runtime.argv, "runtime.argv")?;
        validate_step_placeholders(&self.source.steps, self.source.checkout)?;
        validate_runtime_placeholders(&self.runtime.argv, self.source.checkout)?;
        Ok(())
    }
}

impl ManifestMask {
    pub fn parse(source: &str) -> Result<Self, PackageError> {
        let mask: Self = toml::from_str(source).map_err(|error: toml::de::Error| {
            let location = error
                .span()
                .map(|span| format!(" at byte range {}..{}", span.start, span.end))
                .unwrap_or_default();
            PackageError::mask(format!("invalid plugin mask syntax or schema{location}"))
        })?;
        if mask.format_version != 1 {
            return Err(PackageError::mask("plugin mask format_version must be 1"));
        }
        if let Some(plugin) = &mask.plugin
            && let Some(name) = &plugin.name
        {
            PluginName::from_str(name).map_err(|error| PackageError::mask(error.to_string()))?;
        }
        if let Some(source) = &mask.source {
            if let Some(dependencies) = &source.dependencies {
                validate_dependencies(dependencies).map_err(PackageError::as_mask)?;
            }
            if let Some(steps) = &source.steps {
                validate_steps(steps).map_err(PackageError::as_mask)?;
                validate_mask_step_placeholders(steps).map_err(PackageError::as_mask)?;
            }
        }
        if let Some(runtime) = &mask.runtime
            && let Some(argv) = &runtime.argv
        {
            validate_argv(argv, "runtime.argv").map_err(PackageError::as_mask)?;
            validate_mask_runtime_placeholders(argv).map_err(PackageError::as_mask)?;
        }
        Ok(mask)
    }
}

impl EffectiveManifest {
    pub fn merge(
        publisher: PublisherManifest,
        mask: Option<ManifestMask>,
    ) -> Result<Self, PackageError> {
        let mut plugin_name = publisher.plugin.name;
        let mut source = publisher.source;
        let mut runtime = publisher.runtime;
        if let Some(mask) = mask {
            if let Some(name) = mask.plugin.and_then(|plugin| plugin.name) {
                plugin_name = name;
            }
            if let Some(mask_source) = mask.source {
                if let Some(dependencies) = mask_source.dependencies {
                    source.dependencies = dependencies;
                }
                if let Some(steps) = mask_source.steps {
                    source.steps = steps;
                }
            }
            if let Some(argv) = mask.runtime.and_then(|runtime| runtime.argv) {
                runtime.argv = argv;
            }
        }

        let effective = Self {
            plugin_id: publisher.plugin.id,
            plugin_name,
            protocol_fingerprint: publisher.plugin.protocol_fingerprint,
            source,
            runtime,
        };
        effective.validate()?;
        Ok(effective)
    }

    pub fn plugin_id(&self) -> Result<PluginId, PackageError> {
        self.plugin_id
            .parse()
            .map_err(|error: String| PackageError::manifest(error))
    }

    pub fn plugin_name(&self) -> Result<PluginName, PackageError> {
        self.plugin_name
            .parse()
            .map_err(|error: String| PackageError::manifest(error))
    }

    pub fn expanded_source_steps(
        &self,
        paths: &ExpansionPaths<'_>,
    ) -> Result<Vec<Vec<String>>, PackageError> {
        self.source
            .steps
            .iter()
            .map(|step| {
                expand_argv(
                    step,
                    paths,
                    arguments::PlaceholderScope::Step(self.source.checkout),
                )
            })
            .collect()
    }

    pub fn expanded_runtime_argv(
        &self,
        paths: &ExpansionPaths<'_>,
    ) -> Result<Vec<String>, PackageError> {
        expand_argv(
            &self.runtime.argv,
            paths,
            arguments::PlaceholderScope::Runtime(self.source.checkout),
        )
    }

    pub(crate) fn validate(&self) -> Result<(), PackageError> {
        PluginId::from_str(&self.plugin_id)
            .map_err(|error| PackageError::manifest(error.to_string()))?;
        PluginName::from_str(&self.plugin_name)
            .map_err(|error| PackageError::manifest(error.to_string()))?;
        validate_fingerprint(&self.protocol_fingerprint)?;
        validate_dependencies(&self.source.dependencies)?;
        validate_steps(&self.source.steps)?;
        validate_argv(&self.runtime.argv, "runtime.argv")?;
        validate_step_placeholders(&self.source.steps, self.source.checkout)?;
        validate_runtime_placeholders(&self.runtime.argv, self.source.checkout)
    }
}

fn validate_fingerprint(value: &str) -> Result<(), PackageError> {
    let expected = crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256);
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(PackageError::manifest(
            "plugin.protocol_fingerprint must be 64 lower-case hexadecimal characters",
        ));
    }
    if value != expected {
        return Err(PackageError::protocol(
            "plugin protocol fingerprint does not match this oll binary",
        ));
    }
    Ok(())
}

fn validate_dependencies(dependencies: &BTreeMap<String, String>) -> Result<(), PackageError> {
    for (executable, hint) in dependencies {
        if executable.is_empty() || hint.is_empty() {
            return Err(PackageError::manifest(
                "source dependencies require nonempty executable and hint",
            ));
        }
        let path = Path::new(executable);
        if !path.is_absolute() && executable.contains(std::path::MAIN_SEPARATOR) {
            return Err(PackageError::manifest(
                "a dependency executable must be a basename or absolute path",
            ));
        }
    }
    Ok(())
}

fn validate_steps(steps: &[Vec<String>]) -> Result<(), PackageError> {
    for step in steps {
        validate_argv(step, "source.steps[]")?;
    }
    Ok(())
}

fn validate_argv(argv: &[String], field: &'static str) -> Result<(), PackageError> {
    if argv.is_empty() || argv[0].is_empty() {
        return Err(PackageError::manifest(format!(
            "{field} must be a nonempty argv with a nonempty executable"
        )));
    }
    if argv.iter().any(|value| value.as_bytes().contains(&0)) {
        return Err(PackageError::manifest(format!(
            "{field} must not contain NUL bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint() -> String {
        crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256)
    }

    #[test]
    fn typed_mask_replaces_dependency_table_and_preserves_omitted_steps() {
        let publisher = PublisherManifest::parse(&format!(
            r#"
format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
steps = [["cargo", "build", "--root", "{{install}}"]]
[source.dependencies]
"cargo" = "install cargo"
[runtime]
argv = ["{{install}}/bin/oll-test"]
"#,
            fingerprint()
        ))
        .unwrap();
        let mask = ManifestMask::parse(
            r#"
format_version = 1
[plugin]
name = "personal-test"
[source.dependencies]
"/usr/bin/cargo" = "system cargo"
"#,
        )
        .unwrap();
        let effective = EffectiveManifest::merge(publisher, Some(mask)).unwrap();
        assert_eq!(effective.plugin_id, "oll.test");
        assert_eq!(effective.plugin_name, "personal-test");
        assert_eq!(effective.source.dependencies.len(), 1);
        assert_eq!(
            effective
                .source
                .dependencies
                .get("/usr/bin/cargo")
                .map(String::as_str),
            Some("system cargo")
        );
        assert_eq!(effective.source.steps.len(), 1);
    }

    #[test]
    fn typed_mask_can_clear_steps_and_dependencies() {
        let publisher = PublisherManifest::parse(&format!(
            r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
steps = [["cargo", "build", "--root", "{{install}}"]]
[source.dependencies]
"cargo" = "install cargo"
[runtime]
argv = ["{{install}}/bin/oll-test"]
"#,
            fingerprint()
        ))
        .unwrap();
        let mask = ManifestMask::parse(
            r#"format_version = 1
[source]
steps = []
[source.dependencies]
"#,
        )
        .unwrap();

        let effective = EffectiveManifest::merge(publisher, Some(mask)).unwrap();
        assert!(effective.source.steps.is_empty());
        assert!(effective.source.dependencies.is_empty());
    }

    #[test]
    fn mask_cannot_smuggle_immutable_fields() {
        let error = ManifestMask::parse(
            r#"
format_version = 1
[plugin]
id = "oll.other"
"#,
        )
        .unwrap_err();
        assert_eq!(error.code(), "mask_invalid");
    }

    #[test]
    fn toml_parse_diagnostics_never_echo_source_values() {
        let publisher_secret = "publisher-super-secret";
        let publisher =
            PublisherManifest::parse(&format!("format_version = \"{publisher_secret}\"\n"))
                .unwrap_err();
        assert_eq!(publisher.code(), "manifest_invalid");
        assert!(!publisher.message().contains(publisher_secret));
        assert!(!publisher.to_string().contains(publisher_secret));

        let mask_secret = "mask-super-secret";
        let mask =
            ManifestMask::parse(&format!("format_version = \"{mask_secret}\"\n")).unwrap_err();
        assert_eq!(mask.code(), "mask_invalid");
        assert!(!mask.message().contains(mask_secret));
        assert!(!mask.to_string().contains(mask_secret));
    }

    #[test]
    fn runtime_rejects_source_only_placeholders() {
        let source = format!(
            r#"
format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
[runtime]
argv = ["{{source}}/plugin"]
"#,
            fingerprint()
        );
        assert!(PublisherManifest::parse(&source).is_err());
    }

    #[test]
    fn checkout_controls_source_and_runtime_placeholders() {
        for (checkout, step_path, runtime_path) in [
            ("source", "{source}/input", "{install}/plugin"),
            ("install", "{install}/input", "{install}/plugin"),
            ("generation", "{generation}/input", "{generation}/plugin"),
        ] {
            let manifest = format!(
                r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "{checkout}"
steps = [["/bin/true", "{step_path}"]]
[runtime]
argv = ["{runtime_path}"]
"#,
                fingerprint()
            );
            PublisherManifest::parse(&manifest).unwrap();
        }

        for (checkout, unavailable) in [
            ("source", "{generation}/input"),
            ("install", "{source}/input"),
            ("generation", "{install}/input"),
            ("source", "{staging}/input"),
        ] {
            let manifest = format!(
                r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "{checkout}"
steps = [["/bin/true", "{unavailable}"]]
[runtime]
argv = ["/bin/true"]
"#,
                fingerprint()
            );
            assert!(PublisherManifest::parse(&manifest).is_err());
        }
    }

    #[test]
    fn checkout_is_required_and_cannot_be_masked() {
        let missing = format!(
            r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
[runtime]
argv = ["/bin/true"]
"#,
            fingerprint()
        );
        assert!(PublisherManifest::parse(&missing).is_err());

        let mask = r#"format_version = 1
[source]
checkout = "generation"
"#;
        assert!(ManifestMask::parse(mask).is_err());
    }

    #[test]
    fn old_array_of_tables_source_shape_is_rejected() {
        let old_dependencies = format!(
            r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
[[source.dependencies]]
executable = "cargo"
hint = "install cargo"
[runtime]
argv = ["/bin/true"]
"#,
            fingerprint()
        );
        assert!(PublisherManifest::parse(&old_dependencies).is_err());

        let old_steps = format!(
            r#"format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
[[source.steps]]
argv = ["/bin/true"]
[runtime]
argv = ["/bin/true"]
"#,
            fingerprint()
        );
        assert!(PublisherManifest::parse(&old_steps).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn nonexistent_leaf_below_symlinked_ancestor_cannot_escape_root() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::TempDir::new().unwrap();
        let permitted = directory.path().join("permitted");
        let outside = directory.path().join("outside");
        std::fs::create_dir_all(&permitted).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, permitted.join("escape")).unwrap();

        let error =
            ensure_contained_path(&permitted, &permitted.join("escape/new-file")).unwrap_err();
        assert_eq!(error.code(), "entrypoint_invalid");
        assert_eq!(
            error.message(),
            "runtime path resolves outside its permitted root"
        );
    }
}
