mod error;
mod layout;
mod runtime;
mod schema;
mod values;

#[cfg(test)]
mod tests;

pub use error::ConfigError;
pub(crate) use layout::{validate_storage_layout, validate_working_tree_roots};
pub use runtime::ConfigRuntime;
pub use values::{ConnectUrl, NetworkKey, PostgresUrl, ReplicaStoreConfig, ResolvedNodeConfig};

const CONFIG_FILENAME: &str = "config.lua";
const ROOT_REGISTRY_KEY: &str = "oll.config.root";
