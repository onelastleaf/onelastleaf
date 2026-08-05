use crate::plugin::{
    InstalledPlugin, PackagePublishIntent, PluginError, PluginJobCounts, PluginSelector,
    RemovalIntent, runtime::PluginSessionSnapshot,
};

#[cfg(test)]
use crate::plugin::PluginId;

use super::PluginRuntime;

#[derive(Clone, Debug)]
pub struct PluginListEntry {
    pub installed: InstalledPlugin,
    pub process: Option<PluginSessionSnapshot>,
}

#[derive(Clone, Debug)]
pub struct PluginInspection {
    pub installed: InstalledPlugin,
    pub process: Option<PluginSessionSnapshot>,
    pub package_publish_intent: Option<PackagePublishIntent>,
    pub removal_intent: Option<RemovalIntent>,
    pub job_counts: PluginJobCounts,
}

impl PluginRuntime {
    pub async fn list_plugins(&self) -> Result<Vec<PluginListEntry>, PluginError> {
        let installed = self.store.list_plugins().await?;
        let mut sessions = self.supervisor.session_snapshots().await;
        Ok(installed
            .into_iter()
            .map(|installed| PluginListEntry {
                process: sessions
                    .remove(&installed.plugin_id)
                    .filter(|process| process_matches_store(&installed, process)),
                installed,
            })
            .collect())
    }

    #[cfg(test)]
    pub(crate) async fn hold_package_gate(
        &self,
        plugin_id: &PluginId,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.packages.gates().lock(plugin_id).await
    }

    #[cfg(test)]
    pub(crate) async fn supervisor_barrier(&self) -> Result<(), PluginError> {
        self.supervisor.controller_barrier().await
    }

    #[cfg(test)]
    pub(crate) async fn saturate_instance_work_queue(
        &self,
        plugin_id: &PluginId,
    ) -> Result<crate::plugin::runtime::SaturatedInstanceWorkQueue, PluginError> {
        self.supervisor
            .saturate_instance_work_queue(plugin_id)
            .await
    }

    pub async fn inspect_plugin(
        &self,
        selector: &PluginSelector,
    ) -> Result<PluginInspection, PluginError> {
        let installed = self.store.get_plugin(selector).await?;
        let process = self
            .supervisor
            .session_snapshot(&installed.plugin_id)
            .await
            .filter(|process| process_matches_store(&installed, process));
        let plugin_id = &installed.plugin_id;
        let package_publish_intent = self.store.package_publish_intent(plugin_id).await?;
        let removal_intent = self.store.removal_intent(plugin_id).await?;
        let job_counts = self.store.job_counts(plugin_id).await?;
        Ok(PluginInspection {
            installed,
            process,
            package_publish_intent,
            removal_intent,
            job_counts,
        })
    }
}

fn process_matches_store(installed: &InstalledPlugin, process: &PluginSessionSnapshot) -> bool {
    installed.running_instance_id == Some(process.instance_id)
        && installed.running_generation == Some(process.install_generation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{DesiredPluginState, InstallMode, ObservedPluginState, PluginInstanceId};
    use uuid::Uuid;

    #[test]
    fn a_transient_process_identity_mismatch_is_omitted_from_the_snapshot() {
        let running_instance = PluginInstanceId::new();
        let running_generation = Uuid::new_v4();
        let installed = InstalledPlugin {
            plugin_id: "oll.snapshot-test".parse().unwrap(),
            plugin_name: "snapshot-test".parse().unwrap(),
            normalized_declaration: Vec::new(),
            declaration_sha256: [0; 32],
            effective_manifest: Vec::new(),
            selected_commit: None,
            install_mode: InstallMode::Source,
            release_id: None,
            current_generation: running_generation,
            running_generation: Some(running_generation),
            running_instance_id: Some(running_instance),
            desired_state: DesiredPluginState::Running,
            restart_sequence: 0,
            consumed_restart_sequence: 0,
            restart_attempt: 0,
            restart_not_before: None,
            last_lifecycle_failure: None,
        };
        let process = PluginSessionSnapshot {
            state: ObservedPluginState::Starting,
            instance_id: PluginInstanceId::new(),
            install_generation: running_generation,
            process_id: None,
            started_at: None,
            ready_at: None,
            actions: Vec::new(),
        };

        assert!(!process_matches_store(&installed, &process));
    }
}
