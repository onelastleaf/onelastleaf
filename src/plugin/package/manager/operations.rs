use std::fs;

use uuid::Uuid;

use super::*;

impl PackageManager {
    pub async fn install_declared(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        let declarations =
            read_plugin_declarations(&self.config_root).map_err(package_configuration_error)?;
        self.install_declaration_set(&declarations, false, correlation_id)
            .await
    }

    pub async fn update(
        &self,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        let installed = self.store.get_plugin(selector).await?;
        let declarations =
            read_plugin_declarations(&self.config_root).map_err(package_configuration_error)?;
        let declaration = declarations.get(&installed.plugin_id).ok_or_else(|| {
            PluginError::FailedPrecondition(format!(
                "plugin {} has no declaration in plugins.lua",
                installed.plugin_id
            ))
        })?;
        let prepared = self
            .prepare_candidate(
                installed.plugin_id.clone(),
                declaration.clone(),
                true,
                None,
                correlation_id,
            )
            .await;
        Ok(vec![self.finish_single(prepared).await])
    }

    pub async fn install_remote(
        &self,
        request: InstallRemoteRequest,
        correlation_id: &str,
    ) -> Result<Vec<PackageOperationResult>, PluginError> {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        request
            .declaration
            .validate()
            .map_err(package_configuration_error)?;
        let operation_id = Uuid::new_v4().to_string();
        let staging = match self.layout.discovery_staging(&operation_id) {
            Ok(staging) => staging,
            Err(error) => {
                return Ok(vec![PackageOperationResult::failed(
                    None,
                    None,
                    PackageDiagnostic::from_package(error).with_declaration(&request.declaration),
                )]);
            }
        };
        let staging_guard = StagingGuard(staging.clone());
        let discovery_log = staging.join("build.log");
        let checkout = match self
            .builder
            .checkout(
                &request.declaration,
                &staging,
                &discovery_log,
                None,
                &operation_id,
                correlation_id,
            )
            .await
        {
            Ok(checkout) => checkout,
            Err(error) => {
                let mut diagnostic =
                    PackageDiagnostic::from_package(error).with_declaration(&request.declaration);
                diagnostic.build_log_path = None;
                return Ok(vec![PackageOperationResult::failed(None, None, diagnostic)]);
            }
        };
        let publisher_source = match fs::read_to_string(checkout.source_root.join("oll.toml")) {
            Ok(source) => source,
            Err(error) => {
                return Ok(vec![PackageOperationResult::failed(
                    None,
                    None,
                    PackageDiagnostic::from_package(PackageError::io(
                        "manifest_missing",
                        "manifest",
                        "selected repository has no readable oll.toml",
                        error,
                    ))
                    .with_declaration(&request.declaration),
                )]);
            }
        };
        let publisher = match PublisherManifest::parse(&publisher_source) {
            Ok(publisher) => publisher,
            Err(error) => {
                return Ok(vec![PackageOperationResult::failed(
                    None,
                    None,
                    PackageDiagnostic::from_package(error).with_declaration(&request.declaration),
                )]);
            }
        };
        let plugin_id = publisher
            .plugin
            .id
            .parse::<PluginId>()
            .map_err(PluginError::InvalidArgument)?;

        self.require_package_admission().await?;

        let _declaration_guard = self.declarations.lock().await;
        self.require_package_admission().await?;
        let mut declarations = match read_plugin_declarations(&self.config_root) {
            Ok(declarations) => declarations,
            Err(error) if error.code() == "plugin_config_missing" => PluginDeclarations::default(),
            Err(error) => return Err(package_configuration_error(error)),
        };
        if let Some(current) = declarations.get(&plugin_id)
            && current != &request.declaration
        {
            let current_digest = current.normalized_sha256();
            let authorized = request.overwrite.as_ref().is_some_and(|authorization| {
                authorization.plugin_id == plugin_id
                    && authorization.expected_declaration_sha256 == current_digest
            });
            if request.overwrite.is_some() && !authorized {
                return Err(PluginError::Aborted(
                    "plugin declaration changed before overwrite authorization".to_owned(),
                ));
            }
            if request.overwrite.is_none() {
                return Ok(vec![PackageOperationResult {
                    plugin_id: Some(plugin_id),
                    plugin_name: publisher.plugin.name.parse().ok(),
                    outcome: PackageOperationOutcome::ConfirmationRequired,
                    diagnostics: Vec::new(),
                    confirmation_summary: Some(
                        "replace the existing sanitized installation declaration".to_owned(),
                    ),
                    confirmation_digest: Some(current_digest),
                }]);
            }
        } else if request.overwrite.is_some() {
            return Err(PluginError::Aborted(
                "plugin declaration changed before overwrite authorization".to_owned(),
            ));
        }
        if let Some(authorization) = &request.overwrite
            && authorization.plugin_id != plugin_id
        {
            return Err(PluginError::Aborted(
                "overwrite authorization names another PluginId".to_owned(),
            ));
        }
        declarations.insert(plugin_id.clone(), request.declaration.clone());
        write_plugin_declarations(&self.config_root, &declarations)
            .map_err(package_configuration_error)?;
        drop(_declaration_guard);

        let retained_log = self
            .layout
            .build_log(&plugin_id, &operation_id)
            .map_err(package_configuration_error)?;
        if discovery_log.exists() {
            fs::rename(&discovery_log, &retained_log)
                .map_err(|error| PluginError::io("retain plugin discovery build log", error))?;
        }
        let prepared = self
            .prepare_candidate(
                plugin_id,
                request.declaration,
                false,
                Some((operation_id, checkout)),
                correlation_id,
            )
            .await;
        let result = self.finish_single(prepared).await;
        drop(staging_guard);
        Ok(vec![result])
    }

    pub async fn list_releases(
        &self,
        selector: &PluginSelector,
        correlation_id: &str,
    ) -> Result<(PluginId, Vec<ReleaseListing>), PluginError> {
        require_correlation(correlation_id)?;
        self.require_package_admission().await?;
        let declarations =
            read_plugin_declarations(&self.config_root).map_err(package_configuration_error)?;
        let plugin_id = match selector {
            PluginSelector::Id(plugin_id) if declarations.get(plugin_id).is_some() => {
                plugin_id.clone()
            }
            _ => self.store.get_plugin(selector).await?.plugin_id,
        };
        let declaration = declarations.get(&plugin_id).ok_or_else(|| {
            PluginError::FailedPrecondition(format!(
                "plugin {plugin_id} has no declaration in plugins.lua"
            ))
        })?;
        let operation_id = Uuid::new_v4().to_string();
        let staging = self
            .layout
            .discovery_staging(&operation_id)
            .map_err(package_configuration_error)?;
        let _guard = StagingGuard(staging.clone());
        let checkout = self
            .builder
            .checkout(
                declaration,
                &staging,
                &staging.join("build.log"),
                Some(&plugin_id),
                &operation_id,
                correlation_id,
            )
            .await
            .map_err(|error| PluginError::FailedPrecondition(error.to_string()))?;
        let publisher_source = fs::read_to_string(checkout.source_root.join("oll.toml"))
            .map_err(|error| PluginError::io("read plugin publisher manifest", error))?;
        let publisher =
            PublisherManifest::parse(&publisher_source).map_err(package_configuration_error)?;
        if publisher.plugin.id != plugin_id.as_str() {
            return Err(PluginError::FailedPrecondition(
                "publisher PluginId differs from the declaration".to_owned(),
            ));
        }
        let source = fs::read_to_string(checkout.source_root.join("oll-release.json"))
            .map_err(|error| PluginError::io("read plugin release index", error))?;
        let index =
            ReleaseIndex::parse(&source, &publisher).map_err(package_configuration_error)?;
        Ok((plugin_id, index.listings()))
    }
}
