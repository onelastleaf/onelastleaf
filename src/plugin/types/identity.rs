use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PluginId(String);

impl PluginId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for PluginId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 191 || value.split('.').count() < 2 {
            return Err(
                "plugin ID must contain at least two DNS labels and be at most 191 bytes"
                    .to_owned(),
            );
        }
        if !value.split('.').all(valid_dns_label) {
            return Err("plugin ID must be a lower-case ASCII dotted DNS name".to_owned());
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for PluginId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PluginId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PluginName(String);

impl PluginName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for PluginName {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !valid_dns_label(value) {
            return Err("plugin name must be one lower-case ASCII DNS label".to_owned());
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for PluginName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for PluginName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PluginName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

fn valid_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && (bytes[bytes.len() - 1].is_ascii_lowercase() || bytes[bytes.len() - 1].is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PluginSelector {
    Id(PluginId),
    Name(PluginName),
}

impl PluginSelector {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Id(value) => value.as_str(),
            Self::Name(value) => value.as_str(),
        }
    }
}

impl FromStr for PluginSelector {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.contains('.') {
            value.parse().map(Self::Id)
        } else {
            value.parse().map(Self::Name)
        }
    }
}

impl fmt::Display for PluginSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(value) => value.fmt(formatter),
            Self::Name(value) => value.fmt(formatter),
        }
    }
}

macro_rules! uuid_id {
    ($name:ident, $description:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let parsed = Uuid::parse_str(value)
                    .map_err(|_| concat!($description, " is not a UUID v4"))?;
                if parsed.get_version_num() != 4 || parsed.to_string() != value {
                    return Err(concat!($description, " must be a canonical UUID v4").to_owned());
                }
                Ok(Self(parsed))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(PluginJobId, "plugin job ID");
uuid_id!(PluginArtifactId, "plugin artifact ID");
uuid_id!(PluginInstanceId, "plugin instance ID");

impl PluginArtifactId {
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PluginOperationId(String);

impl PluginOperationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for PluginOperationId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            Err("plugin operation ID must not be empty".to_owned())
        } else {
            Ok(Self(value.to_owned()))
        }
    }
}

impl fmt::Display for PluginOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
