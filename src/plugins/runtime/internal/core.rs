/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;
use tracing::{debug, error, info, trace, warn};

use crate::{
    Shutdown,
    database::Keyspaces,
    plugins::{
        discord_bot::plugin::{
            core_export_types::Host as CoreExportTypesHost,
            core_import_functions::Host as CoreImportFunctionsHost,
            core_import_types::{
                CoreRegistrations, CoreRegistrationsResult, Error, Host as CoreImportTypesHost,
                LogLevels, SupportedCoreRegistrations,
            },
            core_types::Host as CoreTypesHost,
        },
        permissions::{ConfigPluginPermissions, ConfigSupportedCoreRegistrations},
        runtime::internal::InternalRuntime,
    },
    utils::channels::{CoreMessages, DatabaseMessages},
};

impl CoreTypesHost for InternalRuntime {}
impl CoreImportTypesHost for InternalRuntime {}
impl CoreExportTypesHost for InternalRuntime {}

impl CoreImportFunctionsHost for InternalRuntime {
    async fn get_supported_registrations(&mut self) -> SupportedCoreRegistrations {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::GetState(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )));

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<ConfigPluginPermissions>(&response_bytes).unwrap();

        plugin_permissions.core.into()
    }

    async fn register(&mut self, registrations: CoreRegistrations) -> CoreRegistrationsResult {
        let mut result = CoreRegistrationsResult {
            dependency_functions: None,
        };

        if let Some(dependency_functions) = registrations.dependency_functions {
            result.dependency_functions = Some((Vec::new(), Vec::new()));

            for dependency_function in dependency_functions {
                let (sender, receiver) = channel();

                let key = format!("{}-{dependency_function}", self.plugin_id.to_string());

                self.core_tx
                    .send(CoreMessages::DatabaseModule(DatabaseMessages::InsertState(
                        Keyspaces::DependencyFunctions,
                        key.as_bytes().to_vec(),
                        Vec::new(),
                        sender,
                    )));

                let _ = receiver.await;
                result
                    .dependency_functions
                    .as_mut()
                    .unwrap()
                    .0
                    .push(dependency_function)
            }
        }

        result
    }

    async fn log(&mut self, level: LogLevels, message: String) {
        match level {
            LogLevels::Trace => trace!(message),
            LogLevels::Debug => debug!(message),
            LogLevels::Info => info!(message),
            LogLevels::Warn => warn!(message),
            LogLevels::Error => error!(message),
        }
    }

    async fn shutdown(&mut self, restart: bool) -> Result<(), Error> {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::GetState(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )));

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<ConfigPluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
            .core
            .contains(&ConfigSupportedCoreRegistrations::Shutdown)
        {
            return Err(Error::from("Not allowed to call shutdown"));
        }

        let shutdown_type = if restart {
            Shutdown::Restart
        } else {
            Shutdown::Normal
        };

        self.core_tx.send(CoreMessages::Shutdown(shutdown_type));

        Ok(())
    }

    async fn dependency_function(
        &mut self,
        dependency_id: String,
        function_id: String,
        params: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        todo!()
    }
}
