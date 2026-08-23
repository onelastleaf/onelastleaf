use std::{
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    path::{Component, Path, PathBuf},
    str::FromStr,
};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use url::Url;

use crate::plugin::PluginId;

use super::{PackageError, PublisherManifest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseIndex {
    pub plugin_id: String,
    default_release: Option<String>,
    releases: BTreeMap<String, Release>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Release {
    packages: Vec<ReleasePackage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePackage {
    pub target: String,
    pub url: Option<String>,
    pub path: Option<String>,
    pub archive: ArchiveKind,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ArchiveKind {
    #[serde(rename = "tar.gz")]
    TarGz,
    #[serde(rename = "zip")]
    Zip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseListing {
    pub release_id: String,
    pub targets: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawReleaseIndex {
    format_version: u32,
    plugin_id: String,
    #[serde(default, deserialize_with = "deserialize_present_string")]
    default_release: Option<String>,
    releases: UniqueMap<Release>,
}

fn deserialize_present_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

impl ReleaseIndex {
    pub fn parse(source: &str, publisher: &PublisherManifest) -> Result<Self, PackageError> {
        let raw: RawReleaseIndex = serde_json::from_str(source).map_err(|error| {
            PackageError::new(
                "release_index_invalid",
                "release_index",
                format!(
                    "invalid oll-release.json syntax or schema at line {} column {}",
                    error.line(),
                    error.column()
                ),
            )
        })?;
        if raw.format_version != 1 {
            return Err(invalid("oll-release.json format_version must be 1"));
        }
        PluginId::from_str(&raw.plugin_id)
            .map_err(|error| invalid(format!("release index PluginId is invalid: {error}")))?;
        if raw.plugin_id != publisher.plugin.id {
            return Err(invalid(
                "release index PluginId differs from publisher manifest",
            ));
        }
        for (release_id, release) in &raw.releases.entries {
            if release_id.is_empty() {
                return Err(invalid("release IDs must not be empty"));
            }
            for package in &release.packages {
                package.validate()?;
            }
        }
        let default_release = match raw.default_release {
            Some(release_id) if release_id.is_empty() => {
                return Err(invalid("default_release must not be empty"));
            }
            Some(release_id) if !raw.releases.entries.contains_key(&release_id) => {
                return Err(invalid("default_release does not name a release"));
            }
            Some(release_id) => Some(release_id),
            None => raw.releases.first,
        };
        Ok(Self {
            plugin_id: raw.plugin_id,
            default_release,
            releases: raw.releases.entries,
        })
    }

    pub fn resolve_release_id<'a>(
        &'a self,
        release_id: Option<&'a str>,
    ) -> Result<&'a str, PackageError> {
        release_id
            .or(self.default_release.as_deref())
            .ok_or_else(|| {
                PackageError::new(
                    "release_not_found",
                    "release_index",
                    "release index has no release to select by default",
                )
            })
    }

    pub fn listings(&self) -> Vec<ReleaseListing> {
        self.releases
            .iter()
            .map(|(release_id, release)| {
                let mut targets = release
                    .packages
                    .iter()
                    .map(|package| package.target.clone())
                    .collect::<Vec<_>>();
                targets.sort();
                targets.dedup();
                ReleaseListing {
                    release_id: release_id.clone(),
                    targets,
                }
            })
            .collect()
    }

    pub fn select(&self, release_id: &str, target: &str) -> Result<&ReleasePackage, PackageError> {
        let release = self.releases.get(release_id).ok_or_else(|| {
            PackageError::new(
                "release_not_found",
                "release_index",
                "selected opaque release ID does not exist",
            )
        })?;
        let matches = release
            .packages
            .iter()
            .filter(|package| package.target == target)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Err(PackageError::new(
                "release_package_unavailable",
                "release_index",
                format!("release has no package for target {target}"),
            )),
            [package] => Ok(*package),
            _ => Err(PackageError::new(
                "release_package_ambiguous",
                "release_index",
                format!("release has multiple packages for target {target}"),
            )),
        }
    }
}

impl ReleasePackage {
    pub fn parsed_url(&self) -> Result<Url, PackageError> {
        let url = self.url.as_deref().ok_or_else(|| {
            PackageError::new(
                "release_package_unavailable",
                "release_index",
                "release package has no URL for Git installation",
            )
        })?;
        Url::parse(url).map_err(|_| invalid("release package URL is invalid"))
    }

    pub(crate) fn remote_integrity(&self) -> Result<(u64, &str), PackageError> {
        match (self.size_bytes, self.sha256.as_deref()) {
            (Some(size), Some(sha256)) => Ok((size, sha256)),
            _ => Err(PackageError::new(
                "release_package_unavailable",
                "release_index",
                "release package has no remote integrity metadata",
            )),
        }
    }

    pub(crate) fn local_package_path(&self, workspace: &Path) -> Result<PathBuf, PackageError> {
        let relative = self.path.as_deref().ok_or_else(|| {
            PackageError::new(
                "release_package_path_invalid",
                "release_index",
                "release package has no local workspace path",
            )
        })?;
        let workspace = std::fs::canonicalize(workspace).map_err(|error| {
            PackageError::io(
                "release_package_path_invalid",
                "release_index",
                "cannot resolve local plugin workspace",
                error,
            )
        })?;
        let package = std::fs::canonicalize(workspace.join(relative)).map_err(|error| {
            PackageError::io(
                "release_package_path_invalid",
                "release_index",
                "cannot resolve local release package",
                error,
            )
        })?;
        if !package.starts_with(&workspace) {
            return Err(PackageError::new(
                "release_package_path_invalid",
                "release_index",
                "local release package escapes the plugin workspace",
            ));
        }
        let metadata = std::fs::metadata(&package).map_err(|error| {
            PackageError::io(
                "release_package_path_invalid",
                "release_index",
                "cannot inspect local release package",
                error,
            )
        })?;
        if !metadata.is_file() {
            return Err(PackageError::new(
                "release_package_path_invalid",
                "release_index",
                "local release package must be a regular file",
            ));
        }
        Ok(package)
    }

    fn validate(&self) -> Result<(), PackageError> {
        if self.target.is_empty() {
            return Err(invalid("release package target must not be empty"));
        }
        if self.url.is_none() && self.path.is_none() {
            return Err(invalid("release package requires url or path"));
        }
        match (&self.url, self.size_bytes, &self.sha256) {
            (Some(_), Some(_), Some(sha256)) => {
                if sha256.len() != 64
                    || !sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                {
                    return Err(invalid(
                        "release package sha256 must be 64 lower-case hexadecimal characters",
                    ));
                }
            }
            (Some(_), _, _) => {
                return Err(invalid(
                    "release package URL requires size_bytes and sha256",
                ));
            }
            (None, None, None) => {}
            (None, _, _) => {
                return Err(invalid(
                    "release package size_bytes and sha256 require a URL",
                ));
            }
        }
        if self.url.is_some() {
            let url = self.parsed_url()?;
            match url.scheme() {
                "http" | "https" => {}
                "file" => {
                    if url.host_str().is_some_and(|host| !host.is_empty())
                        || url.to_file_path().is_err()
                    {
                        return Err(invalid(
                            "file release URL must name an absolute local path without authority",
                        ));
                    }
                }
                _ => return Err(invalid("release package URL scheme is not allowed")),
            }
            if !self.suffix_matches(url.path()) {
                return Err(invalid(
                    "release package archive kind and URL suffix differ",
                ));
            }
        }
        if let Some(path) = &self.path {
            if path.is_empty()
                || Path::new(path)
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
            {
                return Err(invalid_local_path(
                    "release package path must be a safe relative workspace path",
                ));
            }
            if !self.suffix_matches(path) {
                return Err(invalid_local_path(
                    "release package archive kind and path suffix differ",
                ));
            }
        }
        Ok(())
    }

    fn suffix_matches(&self, path: &str) -> bool {
        match self.archive {
            ArchiveKind::TarGz => path.ends_with(".tar.gz"),
            ArchiveKind::Zip => path.ends_with(".zip"),
        }
    }
}

pub fn local_target() -> Result<&'static str, PackageError> {
    match (std::env::consts::ARCH, std::env::consts::OS) {
        ("x86_64", "linux") if cfg!(target_env = "musl") => Ok("x86_64-unknown-linux-musl"),
        ("x86_64", "linux") => Ok("x86_64-unknown-linux-gnu"),
        ("aarch64", "linux") => Ok("aarch64-unknown-linux-gnu"),
        ("x86_64", "macos") => Ok("x86_64-apple-darwin"),
        ("aarch64", "macos") => Ok("aarch64-apple-darwin"),
        _ => Err(PackageError::new(
            "release_package_unavailable",
            "release_index",
            "the current process target has no canonical release target",
        )),
    }
}

fn invalid(message: impl Into<String>) -> PackageError {
    PackageError::new("release_index_invalid", "release_index", message)
}

fn invalid_local_path(message: impl Into<String>) -> PackageError {
    PackageError::new("release_package_path_invalid", "release_index", message)
}

struct UniqueMap<T> {
    entries: BTreeMap<String, T>,
    first: Option<String>,
}

impl<'de, T> Deserialize<'de> for UniqueMap<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(UniqueMapVisitor(PhantomData))
    }
}

struct UniqueMapVisitor<T>(PhantomData<T>);

impl<'de, T> Visitor<'de> for UniqueMapVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = UniqueMap<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object with unique string keys")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut entries = BTreeMap::new();
        let mut first = None;
        while let Some((key, value)) = access.next_entry::<String, T>()? {
            if entries.insert(key.clone(), value).is_some() {
                return Err(serde::de::Error::custom(format!(
                    "duplicate object key {key:?}"
                )));
            }
            if first.is_none() {
                first = Some(key);
            }
        }
        Ok(UniqueMap { entries, first })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher() -> PublisherManifest {
        PublisherManifest::parse(
            r#"
format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
[source]
checkout = "source"
[runtime]
argv = ["{install}/bin/test"]
"#,
        )
        .unwrap()
    }

    #[test]
    fn opaque_releases_are_sorted_and_selected_exactly() {
        let source = format!(
            r#"{{
  "format_version": 1,
  "plugin_id": "oll.test",
  "default_release": "v0.3.1",
  "releases": {{
    "z": {{"packages": []}},
    "v0.3.1": {{"packages": [{{
      "target": "x86_64-unknown-linux-gnu",
      "url": "https://example.test/a.tar.gz",
      "archive": "tar.gz",
      "size_bytes": 4,
      "sha256": "{}"
    }}]}}
  }}
}}"#,
            "0".repeat(64)
        );
        let index = ReleaseIndex::parse(&source, &publisher()).unwrap();
        assert_eq!(index.listings()[0].release_id, "v0.3.1");
        assert_eq!(index.resolve_release_id(None).unwrap(), "v0.3.1");
        assert!(index.select("V0.3.1", "x86_64-unknown-linux-gnu").is_err());
        assert!(index.select("v0.3.1", "x86_64-unknown-linux-gnu").is_ok());
    }

    #[test]
    fn omitted_default_uses_the_first_release_in_json_source_order() {
        let source = r#"{
  "format_version": 1,
  "plugin_id": "oll.test",
  "releases": {
    "z-first": {"packages": []},
    "a-second": {"packages": []}
  }
}"#;

        let index = ReleaseIndex::parse(source, &publisher()).unwrap();
        assert_eq!(index.resolve_release_id(None).unwrap(), "z-first");
        assert_eq!(index.listings()[0].release_id, "a-second");
    }

    #[test]
    fn explicit_default_must_be_a_nonempty_release_id() {
        for default_release in [r#"""#, r#""missing""#, "null"] {
            let source = format!(
                r#"{{
  "format_version": 1,
  "plugin_id": "oll.test",
  "default_release": {default_release},
  "releases": {{"present": {{"packages": []}}}}
}}"#
            );

            let error = ReleaseIndex::parse(&source, &publisher()).unwrap_err();
            assert_eq!(error.code(), "release_index_invalid");
        }
    }

    #[test]
    fn an_empty_release_index_has_no_implicit_default() {
        let source = r#"{"format_version":1,"plugin_id":"oll.test","releases":{}}"#;
        let index = ReleaseIndex::parse(source, &publisher()).unwrap();

        let error = index.resolve_release_id(None).unwrap_err();
        assert_eq!(error.code(), "release_not_found");
    }

    #[test]
    fn duplicate_release_ids_are_rejected() {
        let source = r#"{"format_version":1,"plugin_id":"oll.test","releases":{"x":{"packages":[]},"x":{"packages":[]}}}"#;
        assert!(ReleaseIndex::parse(source, &publisher()).is_err());
    }

    #[test]
    fn schema_diagnostics_do_not_echo_release_values() {
        let secret = "representative-release-secret";
        let source = format!(
            r#"{{"format_version":1,"plugin_id":"oll.test","releases":{{}},"unexpected":"{secret}"}}"#
        );

        let error = ReleaseIndex::parse(&source, &publisher()).unwrap_err();
        assert_eq!(error.code(), "release_index_invalid");
        assert!(!error.message().contains(secret));
    }

    #[test]
    fn local_package_path_does_not_require_remote_integrity_fields() {
        let source = r#"{
  "format_version": 1,
  "plugin_id": "oll.test",
  "releases": {
    "dev": {"packages": [{
      "target": "x86_64-unknown-linux-gnu",
      "path": "build/dev.tar.gz",
      "archive": "tar.gz"
    }]}
  }
}"#;

        let index = ReleaseIndex::parse(source, &publisher()).unwrap();
        let package = index.select("dev", "x86_64-unknown-linux-gnu").unwrap();
        assert_eq!(package.path.as_deref(), Some("build/dev.tar.gz"));
        assert!(package.url.is_none());
        assert!(package.size_bytes.is_none());
        assert!(package.sha256.is_none());
    }

    #[test]
    fn package_locator_and_integrity_combinations_are_strict() {
        for (package, code) in [
            (
                r#"{"target":"x","archive":"tar.gz"}"#,
                "release_index_invalid",
            ),
            (
                r#"{"target":"x","path":"build/x.tar.gz","archive":"tar.gz","size_bytes":1}"#,
                "release_index_invalid",
            ),
            (
                r#"{"target":"x","url":"https://example.test/x.tar.gz","archive":"tar.gz"}"#,
                "release_index_invalid",
            ),
            (
                r#"{"target":"x","path":"../x.tar.gz","archive":"tar.gz"}"#,
                "release_package_path_invalid",
            ),
        ] {
            let source = format!(
                r#"{{"format_version":1,"plugin_id":"oll.test","releases":{{"dev":{{"packages":[{package}]}}}}}}"#
            );
            assert_eq!(
                ReleaseIndex::parse(&source, &publisher())
                    .unwrap_err()
                    .code(),
                code
            );
        }
    }

    #[test]
    fn local_package_path_rejects_a_symlink_escape() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::TempDir::new().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let outside = directory.path().join("outside.tar.gz");
        std::fs::write(&outside, b"archive").unwrap();
        symlink(&outside, workspace.join("package.tar.gz")).unwrap();
        let package = ReleasePackage {
            target: "x".to_owned(),
            url: None,
            path: Some("package.tar.gz".to_owned()),
            archive: ArchiveKind::TarGz,
            size_bytes: None,
            sha256: None,
        };

        let error = package.local_package_path(&workspace).unwrap_err();
        assert_eq!(error.code(), "release_package_path_invalid");
    }
}
