use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

use futures_util::{StreamExt, stream::FuturesUnordered};

use super::*;

pub(super) struct ReconcileCandidateBatch(pub(super) Vec<PreparedResolution>);

pub(super) struct ReconcileRemoval {
    pub(super) plugin_id: PluginId,
    pub(super) plugin_name: PluginName,
    pub(super) dependents: ReconcileCandidateBatch,
}

pub(super) struct ExactReconcilePlan {
    pub(super) removals: Vec<ReconcileRemoval>,
    pub(super) independent: ReconcileCandidateBatch,
    pub(super) immediate: Vec<PackageOperationResult>,
}

enum ExactReconcileWork {
    Removal {
        removal: ReconcileRemoval,
        result: Result<PackageOperationResult, PluginError>,
    },
    Publication(Vec<PackageOperationResult>),
}

type ExactReconcileFutures<'a> = FuturesUnordered<
    Pin<Box<dyn Future<Output = Result<ExactReconcileWork, PluginError>> + Send + 'a>>,
>;

impl PackageManager {
    pub(crate) async fn reconcile_exact_with<F, Fut>(
        &self,
        correlation_id: &str,
        remove: F,
    ) -> Result<Vec<PackageOperationResult>, PluginError>
    where
        F: Fn(PluginId) -> Fut + Send + Sync,
        Fut: Future<Output = Result<PackageOperationResult, PluginError>> + Send,
    {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        let plan = self.prepare_exact_reconcile(correlation_id).await?;
        self.execute_exact_reconcile_plan(plan, remove).await
    }

    pub(super) async fn execute_exact_reconcile_plan<F, Fut>(
        &self,
        plan: ExactReconcilePlan,
        remove: F,
    ) -> Result<Vec<PackageOperationResult>, PluginError>
    where
        F: Fn(PluginId) -> Fut + Send + Sync,
        Fut: Future<Output = Result<PackageOperationResult, PluginError>> + Send,
    {
        let mut results = plan.immediate;
        let mut work: ExactReconcileFutures<'_> = FuturesUnordered::new();
        if !plan.independent.0.is_empty() {
            let manager = self.clone();
            work.push(Box::pin(async move {
                manager
                    .publish_resolved_set(plan.independent.0)
                    .await
                    .map(ExactReconcileWork::Publication)
            }));
        }
        for removal in plan.removals {
            let future = remove(removal.plugin_id.clone());
            work.push(Box::pin(async move {
                Ok(ExactReconcileWork::Removal {
                    removal,
                    result: future.await,
                })
            }));
        }
        while let Some(completed) = work.next().await {
            match completed? {
                ExactReconcileWork::Publication(mut published) => results.append(&mut published),
                ExactReconcileWork::Removal { removal, result } => match result {
                    Ok(removed) if removed.outcome == PackageOperationOutcome::Removed => {
                        results.push(removed);
                        if !removal.dependents.0.is_empty() {
                            let manager = self.clone();
                            work.push(Box::pin(async move {
                                manager
                                    .publish_resolved_set(removal.dependents.0)
                                    .await
                                    .map(ExactReconcileWork::Publication)
                            }));
                        }
                    }
                    Ok(failed) => {
                        results.push(failed);
                        results.extend(Self::fail_removal_dependents(removal));
                    }
                    Err(_) => {
                        results.push(PackageOperationResult::failed(
                            Some(removal.plugin_id.clone()),
                            Some(removal.plugin_name.clone()),
                            PackageDiagnostic {
                                code: "install_publish_failed".to_owned(),
                                phase: "removal".to_owned(),
                                message: "plugin removal failed".to_owned(),
                                hint: None,
                                build_log_path: None,
                                sanitized_remote: None,
                                branch: None,
                                revision: None,
                                release_id: None,
                                target: None,
                            },
                        ));
                        results.extend(Self::fail_removal_dependents(removal));
                    }
                },
            }
        }
        results.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        Ok(results)
    }

    async fn prepare_exact_reconcile(
        &self,
        correlation_id: &str,
    ) -> Result<ExactReconcilePlan, PluginError> {
        let declarations =
            read_plugin_declarations(&self.config_root).map_err(package_configuration_error)?;
        let mut installed = self.store.list_plugins().await?;
        installed.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        let installed_by_id = installed
            .iter()
            .cloned()
            .map(|plugin| (plugin.plugin_id.clone(), plugin))
            .collect::<BTreeMap<_, _>>();

        let mut resolving = FuturesUnordered::new();
        for (plugin_id, declaration) in declarations.iter() {
            let manager = self.clone();
            let plugin_id = plugin_id.clone();
            let declaration = declaration.clone();
            let installed = installed_by_id.get(&plugin_id).cloned();
            resolving.push(async move {
                manager
                    .resolve_candidate(
                        plugin_id,
                        declaration,
                        false,
                        None,
                        InstalledResolution::Snapshot(Box::new(installed)),
                        correlation_id,
                    )
                    .await
            });
        }
        let mut immediate = Vec::new();
        let mut candidates = Vec::new();
        while let Some(resolved) = resolving.next().await {
            match resolved {
                Resolved::Candidate(candidate) => candidates.push(*candidate),
                Resolved::Result(result) => immediate.push(result),
            }
        }

        Ok(Self::partition_exact_reconcile(
            &declarations,
            installed,
            candidates,
            immediate,
        ))
    }

    pub(super) fn partition_exact_reconcile(
        declarations: &PluginDeclarations,
        installed: Vec<InstalledPlugin>,
        candidates: Vec<PreparedResolution>,
        mut immediate: Vec<PackageOperationResult>,
    ) -> ExactReconcilePlan {
        let installed_by_name = installed
            .iter()
            .map(|plugin| (plugin.plugin_name.clone(), plugin.plugin_id.clone()))
            .collect::<BTreeMap<_, _>>();
        let declared_ids = declarations
            .iter()
            .map(|(plugin_id, _)| plugin_id.clone())
            .collect::<BTreeSet<_>>();
        let mut desired_names: BTreeMap<PluginName, Vec<PluginId>> = BTreeMap::new();
        for candidate in &candidates {
            desired_names
                .entry(candidate.resolved.plugin_name.clone())
                .or_default()
                .push(candidate.resolved.plugin_id.clone());
        }
        let duplicate_claims = desired_names
            .into_values()
            .filter(|plugin_ids| plugin_ids.len() > 1)
            .flatten()
            .collect::<BTreeSet<_>>();
        let mut independent = Vec::new();
        let mut dependents: BTreeMap<PluginId, Vec<PreparedResolution>> = BTreeMap::new();
        for candidate in candidates {
            let plugin_id = &candidate.resolved.plugin_id;
            let name = &candidate.resolved.plugin_name;
            if duplicate_claims.contains(plugin_id) {
                immediate.push(PackageOperationResult::failed(
                    Some(plugin_id.clone()),
                    Some(name.clone()),
                    PackageDiagnostic::name_conflict(name, &candidate.declaration),
                ));
                continue;
            }
            match installed_by_name.get(name) {
                None => independent.push(candidate),
                Some(owner) if owner == plugin_id => independent.push(candidate),
                Some(owner) if declared_ids.contains(owner) => {
                    immediate.push(PackageOperationResult::failed(
                        Some(plugin_id.clone()),
                        Some(name.clone()),
                        PackageDiagnostic::name_conflict(name, &candidate.declaration),
                    ));
                }
                Some(owner) => dependents.entry(owner.clone()).or_default().push(candidate),
            }
        }
        let removals = installed
            .into_iter()
            .filter(|plugin| !declared_ids.contains(&plugin.plugin_id))
            .map(|plugin| ReconcileRemoval {
                dependents: ReconcileCandidateBatch(
                    dependents.remove(&plugin.plugin_id).unwrap_or_default(),
                ),
                plugin_id: plugin.plugin_id,
                plugin_name: plugin.plugin_name,
            })
            .collect();
        debug_assert!(dependents.is_empty());
        ExactReconcilePlan {
            removals,
            independent: ReconcileCandidateBatch(independent),
            immediate,
        }
    }

    fn fail_removal_dependents(removal: ReconcileRemoval) -> Vec<PackageOperationResult> {
        removal
            .dependents
            .0
            .into_iter()
            .map(|candidate| {
                PackageOperationResult::failed(
                    Some(candidate.resolved.plugin_id.clone()),
                    Some(candidate.resolved.plugin_name.clone()),
                    PackageDiagnostic::blocked_by_removal(
                        &candidate.resolved.plugin_name,
                        &removal.plugin_id,
                        &candidate.declaration,
                    ),
                )
            })
            .collect()
    }
}
