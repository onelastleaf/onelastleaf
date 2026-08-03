use std::collections::BTreeSet;

use super::{
    super::{ReplicaError, model::absolute_to_namespace, types::ActiveReplica},
    filesystem::remove_path,
    types::ReplicaRuntime,
};

impl ReplicaRuntime {
    pub(crate) async fn replace_state(&self, replica: ActiveReplica) {
        *self.state.write().await = Some(replica);
    }

    pub(crate) async fn project_complete(
        &self,
        replica: &ActiveReplica,
    ) -> Result<(), ReplicaError> {
        let desired = replica.projected_paths()?;
        let desired_paths = desired.values().cloned().collect::<BTreeSet<_>>();
        let mut actual = Vec::new();
        for item in walkdir::WalkDir::new(&self.root)
            .follow_links(false)
            .min_depth(1)
        {
            let item = item.map_err(|error| {
                ReplicaError::InvalidArgument(format!(
                    "cannot inspect working tree during projection: {error}"
                ))
            })?;
            actual.push((
                absolute_to_namespace(&self.root, item.path())?,
                item.path().to_owned(),
            ));
        }
        actual.sort_by_key(|(path, _)| std::cmp::Reverse(path.matches('/').count()));
        for (namespace, path) in actual {
            if !desired_paths.contains(&namespace) {
                remove_path(&path)?;
            }
        }
        self.materialize_paths(replica, desired.keys().copied())
            .await
    }
}
