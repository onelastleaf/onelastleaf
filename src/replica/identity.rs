use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::node::identity::atomic_write;

use super::{
    ReplicaError,
    store::{IdentityTransition, IdentityTransitionKind, ReplicaStore},
    types::ActiveReplica,
};

const REPLICA_IDENTITY_FILENAME: &str = "replica.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaIdentity {
    replica_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredReplicaIdentity {
    format_version: u32,
    replica_id: String,
}

impl ReplicaIdentity {
    pub fn new(replica_id: Uuid) -> Self {
        Self { replica_id }
    }

    pub fn replica_id(self) -> Uuid {
        self.replica_id
    }

    pub fn load(config_root: &Path) -> Result<Self, ReplicaError> {
        let path = identity_path(config_root);
        let source = fs::read(&path).map_err(|error| {
            ReplicaError::Configuration(format!(
                "cannot read replica identity at {}: {error}",
                path.display()
            ))
        })?;
        Self::decode(&path, &source)
    }

    #[cfg(test)]
    pub fn load_optional(config_root: &Path) -> Result<Option<Self>, ReplicaError> {
        let path = identity_path(config_root);
        match fs::read(&path) {
            Ok(source) => Self::decode(&path, &source).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ReplicaError::Configuration(format!(
                "cannot read replica identity at {}: {error}",
                path.display()
            ))),
        }
    }

    pub fn encode(self) -> Result<Vec<u8>, ReplicaError> {
        let stored = StoredReplicaIdentity {
            format_version: 1,
            replica_id: self.replica_id.to_string(),
        };
        let mut bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|_| ReplicaError::Internal("cannot serialize replica identity".to_owned()))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    #[cfg(test)]
    pub fn write(self, config_root: &Path) -> Result<Vec<u8>, ReplicaError> {
        let bytes = self.encode()?;
        atomic_write(&identity_path(config_root), &bytes)
            .map_err(|error| ReplicaError::Internal(error.to_string()))?;
        Ok(bytes)
    }

    fn decode(path: &Path, source: &[u8]) -> Result<Self, ReplicaError> {
        let stored: StoredReplicaIdentity = serde_json::from_slice(source).map_err(|_| {
            invalid_identity(
                path,
                "must be strict JSON with exactly the documented fields",
            )
        })?;
        if stored.format_version != 1 {
            return Err(invalid_identity(
                path,
                "format_version is not a supported version",
            ));
        }
        let replica_id = Uuid::parse_str(&stored.replica_id)
            .map_err(|_| invalid_identity(path, "replica_id must be a canonical UUID v4"))?;
        if replica_id.get_version_num() != 4 || stored.replica_id != replica_id.to_string() {
            return Err(invalid_identity(
                path,
                "replica_id must be a canonical lower-case UUID v4",
            ));
        }
        Ok(Self { replica_id })
    }
}

pub(crate) fn identity_path(config_root: &Path) -> PathBuf {
    config_root.join(REPLICA_IDENTITY_FILENAME)
}

pub(crate) fn read_identity_bytes(config_root: &Path) -> Result<Option<Vec<u8>>, ReplicaError> {
    let path = identity_path(config_root);
    match fs::read(&path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ReplicaError::Configuration(format!(
            "cannot read replica identity at {}: {error}",
            path.display()
        ))),
    }
}

pub(crate) fn remove_identity(config_root: &Path) -> Result<(), ReplicaError> {
    let path = identity_path(config_root);
    match fs::remove_file(&path) {
        Ok(()) => sync_parent(&path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ReplicaError::io("remove replica identity", error)),
    }
}

pub(crate) async fn recover_transition(
    store: &ReplicaStore,
    config_root: &Path,
) -> Result<(), ReplicaError> {
    let Some(transition) = store.identity_transition().await? else {
        return Ok(());
    };
    validate_transition_identity_bytes(config_root, &transition)?;
    let active = store.active_generation_id().await?;
    if active == Some(transition.candidate_generation) {
        if !transition.committed {
            return Err(ReplicaError::CorruptStore(
                "active replica identity transition is not marked committed".to_owned(),
            ));
        }
        let current = read_identity_bytes(config_root)?;
        if current.as_deref() != Some(transition.new_identity_file.as_slice()) {
            let may_restore =
                current.is_none() || current.as_deref() == transition.old_identity_file.as_deref();
            if !may_restore {
                return Err(identity_conflict(config_root));
            }
            write_identity_bytes(config_root, &transition.new_identity_file)?;
        }
        store
            .clear_identity_transition(transition.candidate_generation)
            .await?;
        return Ok(());
    }

    if active != transition.expected_active_generation || transition.committed {
        return Err(ReplicaError::CorruptStore(
            "active generation contradicts replica identity transition".to_owned(),
        ));
    }
    let current = read_identity_bytes(config_root)?;
    match transition.old_identity_file.as_deref() {
        Some(old) if current.as_deref() == Some(old) => {}
        Some(old) if current.as_deref() == Some(transition.new_identity_file.as_slice()) => {
            write_identity_bytes(config_root, old)?;
        }
        None if current.is_none() => {}
        None if current.as_deref() == Some(transition.new_identity_file.as_slice()) => {
            remove_identity(config_root)?;
        }
        _ => return Err(identity_conflict(config_root)),
    }
    store
        .rollback_identity_transition(transition.candidate_generation)
        .await
}

pub(crate) async fn reconcile_startup_identity(
    store: &ReplicaStore,
    config_root: &Path,
    active: &mut Option<ActiveReplica>,
) -> Result<(), ReplicaError> {
    match active {
        None => {
            if read_identity_bytes(config_root)?.is_some() {
                return Err(ReplicaError::Configuration(format!(
                    "{} exists without an active replica generation",
                    identity_path(config_root).display()
                )));
            }
        }
        Some(replica) => {
            let identity = ReplicaIdentity::load(config_root)?;
            if identity.replica_id() != replica.replica_id {
                store
                    .update_active_replica_id(
                        replica.generation_id,
                        replica.replica_id,
                        identity.replica_id(),
                    )
                    .await?;
                replica.replica_id = identity.replica_id();
            }
        }
    }
    Ok(())
}

pub(crate) async fn activate_candidate(
    store: &ReplicaStore,
    config_root: &Path,
    expected_active: Option<(Uuid, Uuid)>,
    candidate: &ActiveReplica,
    kind: IdentityTransitionKind,
    projection_pending: bool,
) -> Result<(), ReplicaError> {
    let old_identity_file = read_identity_bytes(config_root)?;
    match (expected_active, old_identity_file.as_deref()) {
        (None, None) => {}
        (None, Some(_)) => {
            return Err(ReplicaError::Configuration(format!(
                "{} exists while the replica slot is uninitialized",
                identity_path(config_root).display()
            )));
        }
        (Some((_, expected_replica_id)), Some(bytes)) => {
            let identity = ReplicaIdentity::decode(&identity_path(config_root), bytes)?;
            if identity.replica_id() != expected_replica_id {
                return Err(ReplicaError::Configuration(
                    "replica identity file differs from the active SQL cache".to_owned(),
                ));
            }
        }
        (Some(_), None) => {
            return Err(ReplicaError::Configuration(format!(
                "active replica is missing {}",
                identity_path(config_root).display()
            )));
        }
    }
    let new_identity_file = ReplicaIdentity::new(candidate.replica_id).encode()?;
    let transition = IdentityTransition {
        kind,
        expected_active_generation: expected_active.map(|(generation, _)| generation),
        candidate_generation: candidate.generation_id,
        old_replica_id: expected_active.map(|(_, replica_id)| replica_id),
        new_replica_id: candidate.replica_id,
        old_identity_file,
        new_identity_file: new_identity_file.clone(),
        projection_pending,
        committed: false,
    };
    if let Err(error) = store.prepare_identity_transition(&transition).await {
        let _ = store
            .discard_inactive_generation(candidate.generation_id)
            .await;
        return Err(error);
    }
    if let Err(error) = write_identity_bytes(config_root, &new_identity_file) {
        recover_transition(store, config_root).await?;
        return Err(error);
    }
    if let Err(error) = store
        .activate_identity_transition(candidate.generation_id)
        .await
    {
        recover_transition(store, config_root).await?;
        return Err(error);
    }
    // Once SQL activation commits, the business operation succeeded. Recovery
    // completes or verifies the identity file and transition cleanup before the
    // caller publishes in-memory state.
    recover_transition(store, config_root).await
}

fn write_identity_bytes(config_root: &Path, bytes: &[u8]) -> Result<(), ReplicaError> {
    atomic_write(&identity_path(config_root), bytes)
        .map_err(|error| ReplicaError::Internal(error.to_string()))
}

fn validate_transition_identity_bytes(
    config_root: &Path,
    transition: &IdentityTransition,
) -> Result<(), ReplicaError> {
    let new_identity =
        ReplicaIdentity::decode(&identity_path(config_root), &transition.new_identity_file)?;
    if new_identity.replica_id() != transition.new_replica_id {
        return Err(ReplicaError::CorruptStore(
            "prepared replica identity bytes contain another ReplicaId".to_owned(),
        ));
    }
    match (&transition.old_identity_file, transition.old_replica_id) {
        (None, None) => Ok(()),
        (Some(bytes), Some(old_replica_id)) => {
            let old_identity = ReplicaIdentity::decode(&identity_path(config_root), bytes)?;
            if old_identity.replica_id() != old_replica_id {
                return Err(ReplicaError::CorruptStore(
                    "prepared old identity bytes contain another ReplicaId".to_owned(),
                ));
            }
            Ok(())
        }
        _ => Err(ReplicaError::CorruptStore(
            "prepared replica identity transition has incomplete old identity".to_owned(),
        )),
    }
}

fn identity_conflict(config_root: &Path) -> ReplicaError {
    ReplicaError::Configuration(format!(
        "{} was independently changed during replica identity recovery",
        identity_path(config_root).display()
    ))
}

fn sync_parent(path: &Path) -> Result<(), ReplicaError> {
    let parent = path
        .parent()
        .ok_or_else(|| ReplicaError::Internal("replica identity path has no parent".to_owned()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ReplicaError::io("sync replica identity directory", error))
}

fn invalid_identity(path: &Path, problem: &str) -> ReplicaError {
    ReplicaError::Configuration(format!(
        "invalid replica identity in {}: {problem}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn replica_identity_round_trips_and_requires_canonical_uuid_v4() {
        let directory = TempDir::new().unwrap();
        let identity = ReplicaIdentity::new(Uuid::new_v4());
        identity.write(directory.path()).unwrap();
        assert_eq!(ReplicaIdentity::load(directory.path()).unwrap(), identity);

        fs::write(
            identity_path(directory.path()),
            r#"{"format_version":1,"replica_id":"00000000-0000-1000-8000-000000000000"}"#,
        )
        .unwrap();
        assert!(matches!(
            ReplicaIdentity::load(directory.path()),
            Err(ReplicaError::Configuration(_))
        ));
    }

    #[test]
    fn optional_identity_distinguishes_absence_from_invalid_contents() {
        let directory = TempDir::new().unwrap();
        assert_eq!(
            ReplicaIdentity::load_optional(directory.path()).unwrap(),
            None
        );
        fs::write(identity_path(directory.path()), b"{}").unwrap();
        assert!(ReplicaIdentity::load_optional(directory.path()).is_err());
    }
}
