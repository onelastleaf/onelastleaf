use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard, RwLock, watch};
use uuid::Uuid;

use crate::{
    cli::NodeName,
    protocol::oll::{
        NodeId as ProtoNodeId, NodeIdentity as ProtoNodeIdentity, NodeName as ProtoNodeName,
    },
};

use super::runtime::NodeError;

const IDENTITY_FILENAME: &str = "node.json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIdentity {
    node_id: Uuid,
    node_name: NodeName,
}

pub(crate) struct IdentityCoordinator {
    node: RwLock<NodeIdentity>,
    commit_gate: Arc<Mutex<()>>,
    epoch: AtomicU64,
    epoch_tx: watch::Sender<u64>,
}

impl IdentityCoordinator {
    pub fn new(node: NodeIdentity) -> Arc<Self> {
        let (epoch_tx, _) = watch::channel(0);
        Arc::new(Self {
            node: RwLock::new(node),
            commit_gate: Arc::new(Mutex::new(())),
            epoch: AtomicU64::new(0),
            epoch_tx,
        })
    }

    pub async fn node(&self) -> NodeIdentity {
        self.node.read().await.clone()
    }

    pub async fn node_id(&self) -> Uuid {
        self.node.read().await.node_id()
    }

    pub async fn commit_guard(&self) -> MutexGuard<'_, ()> {
        self.commit_gate.lock().await
    }

    pub async fn commit_guard_owned(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.commit_gate).lock_owned().await
    }

    pub async fn replace_node_while_commits_paused(&self, replacement: NodeIdentity) -> bool {
        let mut current = self.node.write().await;
        if *current == replacement {
            return false;
        }
        *current = replacement;
        true
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn subscribe_epoch(&self) -> watch::Receiver<u64> {
        self.epoch_tx.subscribe()
    }

    pub fn advance_epoch(&self) -> Result<u64, NodeError> {
        let mut current = self.epoch.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| NodeError::Internal("node identity epoch overflowed".to_owned()))?;
            match self.epoch.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.epoch_tx.send_replace(next);
                    return Ok(next);
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredIdentity {
    format_version: u32,
    node_id: String,
    node_name: String,
}

impl NodeIdentity {
    pub(crate) fn new(node_id: Uuid, node_name: NodeName) -> Self {
        Self { node_id, node_name }
    }

    pub fn generate(node_name: NodeName) -> Self {
        Self {
            node_id: Uuid::new_v4(),
            node_name,
        }
    }

    pub fn load(config_root: &Path) -> Result<Self, NodeError> {
        let path = config_root.join(IDENTITY_FILENAME);
        let source = fs::read(&path)
            .map_err(|error| NodeError::config_io("read node identity", path.clone(), error))?;
        let stored: StoredIdentity = serde_json::from_slice(&source).map_err(|_| {
            NodeError::Config(format!("invalid node identity JSON in {}", path.display()))
        })?;

        if stored.format_version != 1 {
            return Err(identity_error(
                &path,
                "format_version",
                "is not a supported version",
            ));
        }

        let node_id = Uuid::parse_str(&stored.node_id)
            .map_err(|_| identity_error(&path, "node_id", "must be a UUID v4"))?;
        if node_id.get_version_num() != 4 || node_id.to_string() != stored.node_id {
            return Err(identity_error(
                &path,
                "node_id",
                "must be a canonical lower-case UUID v4",
            ));
        }

        let node_name = NodeName::from_str(&stored.node_name)
            .map_err(|_| identity_error(&path, "node_name", "must be a lowercase DNS label"))?;

        Ok(Self { node_id, node_name })
    }

    pub fn write(config_root: &Path, identity: &Self) -> Result<(), NodeError> {
        let path = config_root.join(IDENTITY_FILENAME);
        let stored = StoredIdentity {
            format_version: 1,
            node_id: identity.node_id.to_string(),
            node_name: identity.node_name.as_str().to_owned(),
        };
        let mut contents = serde_json::to_vec_pretty(&stored)
            .map_err(|_| NodeError::Internal("cannot serialize node identity".to_owned()))?;
        contents.push(b'\n');
        atomic_write(&path, &contents)
    }

    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    pub fn node_name(&self) -> &NodeName {
        &self.node_name
    }

    pub fn to_proto(&self) -> ProtoNodeIdentity {
        ProtoNodeIdentity {
            node_id: Some(ProtoNodeId {
                value: self.node_id.to_string(),
            }),
            node_name: Some(ProtoNodeName {
                value: self.node_name.as_str().to_owned(),
            }),
        }
    }

    pub(crate) fn from_proto(identity: ProtoNodeIdentity) -> Result<Self, &'static str> {
        let node_id = identity
            .node_id
            .ok_or("SyncHello is missing node.node_id")?
            .value;
        let parsed = Uuid::parse_str(&node_id)
            .map_err(|_| "SyncHello node_id is not a canonical UUID v4")?;
        if parsed.get_version_num() != 4 || parsed.to_string() != node_id {
            return Err("SyncHello node_id is not a canonical UUID v4");
        }
        let node_name = identity
            .node_name
            .ok_or("SyncHello is missing node.node_name")?
            .value
            .parse()
            .map_err(|_| "SyncHello node_name is not a lowercase DNS label")?;
        Ok(Self {
            node_id: parsed,
            node_name,
        })
    }
}

fn identity_error(path: &Path, field: &str, problem: &str) -> NodeError {
    NodeError::Config(format!(
        "invalid node identity in {}: {field} {problem}",
        path.display()
    ))
}

/// Write a replacement beside its target, sync it, and atomically rename it.
/// Renaming replaces a target symlink itself instead of following it.
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), NodeError> {
    let parent = path.parent().ok_or_else(|| {
        NodeError::Internal("persistent file path has no parent directory".to_owned())
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| NodeError::Config("persistent file name is not valid UTF-8".to_owned()))?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));

    let result = (|| -> Result<(), NodeError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| NodeError::io("create temporary persistent file", error))?;
        file.write_all(contents)
            .map_err(|error| NodeError::io("write temporary persistent file", error))?;
        file.sync_all()
            .map_err(|error| NodeError::io("sync temporary persistent file", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| NodeError::io("replace persistent file", error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| NodeError::io("sync persistent file directory", error))?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub fn identity_path(config_root: &Path) -> PathBuf {
    config_root.join(IDENTITY_FILENAME)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_and_normalizes_a_uuid_v4_identity() {
        let directory = TempDir::new().unwrap();
        let identity = NodeIdentity::generate("home-node".parse().unwrap());
        NodeIdentity::write(directory.path(), &identity).unwrap();

        let loaded = NodeIdentity::load(directory.path()).unwrap();
        assert_eq!(loaded, identity);
        assert_eq!(loaded.node_id().get_version_num(), 4);
    }

    #[test]
    fn rejects_unknown_fields_and_non_v4_ids() {
        let directory = TempDir::new().unwrap();
        fs::write(
            identity_path(directory.path()),
            r#"{"format_version":1,"node_id":"00000000-0000-1000-8000-000000000000","node_name":"home","extra":true}"#,
        )
        .unwrap();
        assert!(matches!(
            NodeIdentity::load(directory.path()),
            Err(NodeError::Config(_))
        ));

        fs::write(
            identity_path(directory.path()),
            r#"{"format_version":1,"node_id":"9BA4A1AA-4C7D-4B11-B902-3155CF8CA5F3","node_name":"home"}"#,
        )
        .unwrap();
        assert!(matches!(
            NodeIdentity::load(directory.path()),
            Err(NodeError::Config(_))
        ));
    }

    #[test]
    fn protocol_identity_requires_the_complete_canonical_pair() {
        let identity = NodeIdentity::generate("remote-node".parse().unwrap());
        assert_eq!(
            NodeIdentity::from_proto(identity.to_proto()).unwrap(),
            identity
        );

        let mut noncanonical = identity.to_proto();
        noncanonical.node_id.as_mut().unwrap().value =
            identity.node_id().to_string().to_uppercase();
        assert!(NodeIdentity::from_proto(noncanonical).is_err());
        assert!(NodeIdentity::from_proto(ProtoNodeIdentity::default()).is_err());
    }
}
