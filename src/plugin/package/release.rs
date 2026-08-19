use std::{collections::BTreeMap, fmt, marker::PhantomData, str::FromStr};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use url::Url;

use crate::{plugin::PluginId, protocol::PROTOCOL_SCHEMA_SHA256};

use super::{PackageError, PublisherManifest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseIndex {
    pub plugin_id: String,
    pub protocol_fingerprint: String,
    releases: BTreeMap<String, Release>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Release {
    artifacts: Vec<ReleaseArtifact>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseArtifact {
    pub target: String,
    pub url: String,
    pub archive: ArchiveKind,
    pub size_bytes: u64,
    pub sha256: String,
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
    protocol_fingerprint: String,
    releases: UniqueMap<Release>,
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
        if raw.protocol_fingerprint != publisher.plugin.protocol_fingerprint {
            return Err(invalid(
                "release index protocol fingerprint differs from publisher manifest",
            ));
        }
        let expected = crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256);
        if raw.protocol_fingerprint != expected {
            return Err(PackageError::new(
                "protocol_incompatible",
                "release_index",
                "release index protocol fingerprint differs from this oll binary",
            ));
        }
        for (release_id, release) in &raw.releases.0 {
            if release_id.is_empty() {
                return Err(invalid("release IDs must not be empty"));
            }
            for artifact in &release.artifacts {
                artifact.validate()?;
            }
        }
        Ok(Self {
            plugin_id: raw.plugin_id,
            protocol_fingerprint: raw.protocol_fingerprint,
            releases: raw.releases.0,
        })
    }

    pub fn listings(&self) -> Vec<ReleaseListing> {
        self.releases
            .iter()
            .map(|(release_id, release)| {
                let mut targets = release
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.target.clone())
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

    pub fn select(&self, release_id: &str, target: &str) -> Result<&ReleaseArtifact, PackageError> {
        let release = self.releases.get(release_id).ok_or_else(|| {
            PackageError::new(
                "release_not_found",
                "release_index",
                "selected opaque release ID does not exist",
            )
        })?;
        let matches = release
            .artifacts
            .iter()
            .filter(|artifact| artifact.target == target)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Err(PackageError::new(
                "artifact_unavailable",
                "release_index",
                format!("release has no artifact for target {target}"),
            )),
            [artifact] => Ok(*artifact),
            _ => Err(PackageError::new(
                "artifact_ambiguous",
                "release_index",
                format!("release has multiple artifacts for target {target}"),
            )),
        }
    }
}

impl ReleaseArtifact {
    pub fn parsed_url(&self) -> Result<Url, PackageError> {
        Url::parse(&self.url).map_err(|_| invalid("release artifact URL is invalid"))
    }

    fn validate(&self) -> Result<(), PackageError> {
        if self.target.is_empty() {
            return Err(invalid("release artifact target must not be empty"));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(invalid(
                "release artifact sha256 must be 64 lower-case hexadecimal characters",
            ));
        }
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
            _ => return Err(invalid("release artifact URL scheme is not allowed")),
        }
        let suffix_matches = match self.archive {
            ArchiveKind::TarGz => url.path().ends_with(".tar.gz"),
            ArchiveKind::Zip => url.path().ends_with(".zip"),
        };
        if !suffix_matches {
            return Err(invalid(
                "release artifact archive kind and URL suffix differ",
            ));
        }
        Ok(())
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
            "artifact_unavailable",
            "release_index",
            "the current process target has no canonical release target",
        )),
    }
}

fn invalid(message: impl Into<String>) -> PackageError {
    PackageError::new("release_index_invalid", "release_index", message)
}

struct UniqueMap<T>(BTreeMap<String, T>);

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
        while let Some((key, value)) = access.next_entry::<String, T>()? {
            if entries.insert(key.clone(), value).is_some() {
                return Err(serde::de::Error::custom(format!(
                    "duplicate object key {key:?}"
                )));
            }
        }
        Ok(UniqueMap(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher() -> PublisherManifest {
        PublisherManifest::parse(&format!(
            r#"
format_version = 1
[plugin]
id = "oll.test"
name = "oll-test"
protocol_fingerprint = "{}"
[source]
checkout = "source"
[runtime]
argv = ["{{install}}/bin/test"]
"#,
            crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256)
        ))
        .unwrap()
    }

    #[test]
    fn opaque_releases_are_sorted_and_selected_exactly() {
        let source = format!(
            r#"{{
  "format_version": 1,
  "plugin_id": "oll.test",
  "protocol_fingerprint": "{}",
  "releases": {{
    "z": {{"artifacts": []}},
    "v0.3.1": {{"artifacts": [{{
      "target": "x86_64-unknown-linux-gnu",
      "url": "https://example.test/a.tar.gz",
      "archive": "tar.gz",
      "size_bytes": 4,
      "sha256": "{}"
    }}]}}
  }}
}}"#,
            crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256),
            "0".repeat(64)
        );
        let index = ReleaseIndex::parse(&source, &publisher()).unwrap();
        assert_eq!(index.listings()[0].release_id, "v0.3.1");
        assert!(index.select("V0.3.1", "x86_64-unknown-linux-gnu").is_err());
        assert!(index.select("v0.3.1", "x86_64-unknown-linux-gnu").is_ok());
    }

    #[test]
    fn duplicate_release_ids_are_rejected() {
        let fingerprint = crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256);
        let source = format!(
            r#"{{"format_version":1,"plugin_id":"oll.test","protocol_fingerprint":"{fingerprint}","releases":{{"x":{{"artifacts":[]}},"x":{{"artifacts":[]}}}}}}"#
        );
        assert!(ReleaseIndex::parse(&source, &publisher()).is_err());
    }

    #[test]
    fn schema_diagnostics_do_not_echo_release_values() {
        let secret = "representative-release-secret";
        let source = format!(
            r#"{{"format_version":1,"plugin_id":"oll.test","protocol_fingerprint":"{}","releases":{{}},"unexpected":"{secret}"}}"#,
            crate::replica::lower_hex(&PROTOCOL_SCHEMA_SHA256)
        );

        let error = ReleaseIndex::parse(&source, &publisher()).unwrap_err();
        assert_eq!(error.code(), "release_index_invalid");
        assert!(!error.message().contains(secret));
    }
}
