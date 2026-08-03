use std::{fmt, net::SocketAddr, path::PathBuf, str::FromStr};

use url::Url;
use zeroize::Zeroizing;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectUrl(Url);

impl ConnectUrl {
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    pub fn host(&self) -> &str {
        self.0
            .host_str()
            .expect("validated oll connect URL always has a host")
    }

    pub fn port(&self) -> u16 {
        self.0
            .port()
            .expect("validated oll connect URL always has an explicit port")
    }
}

impl fmt::Display for ConnectUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConnectUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(input).map_err(|error| error.to_string())?;
        if url.scheme() != "oll" {
            return Err("connect URL scheme must be oll".to_owned());
        }
        if url.host().is_none() {
            return Err("connect URL must include a host".to_owned());
        }
        if url.port().is_none_or(|port| port == 0) {
            return Err("connect URL must include an explicit nonzero port".to_owned());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("connect URL must not include user information".to_owned());
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err("connect URL must not include a query or fragment".to_owned());
        }
        if !matches!(url.path(), "" | "/") {
            return Err("connect URL path must be empty or root".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct NetworkKey(Zeroizing<Vec<u8>>);

impl NetworkKey {
    pub(super) fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    pub(crate) fn expose(&self) -> &[u8] {
        self.0.as_slice()
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl fmt::Debug for NetworkKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NetworkKey(REDACTED)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PostgresUrl(Url);

impl PostgresUrl {
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for PostgresUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgresUrl(REDACTED)")
    }
}

impl FromStr for PostgresUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(input).map_err(|error| error.to_string())?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err("replica store URL scheme must be postgres or postgresql".to_owned());
        }
        Ok(Self(url))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaStoreConfig {
    Sqlite { path: PathBuf },
    Postgres { url: PostgresUrl },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedNodeConfig {
    pub replica_root: PathBuf,
    pub replica_store: ReplicaStoreConfig,
    pub log_dir: PathBuf,
    pub listen: Option<SocketAddr>,
    pub connect: Vec<ConnectUrl>,
    pub network_key: Option<NetworkKey>,
}

impl ResolvedNodeConfig {
    pub fn validate_sync_topology(&self) -> Result<(), &'static str> {
        if (self.listen.is_some() || !self.connect.is_empty()) && self.network_key.is_none() {
            Err("node.network_key is required when listen or connect is configured")
        } else {
            Ok(())
        }
    }
}
