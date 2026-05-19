/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::{
    Shutdown,
    config::plugins::permissions::{PluginPermissions, core::PluginPermissionsCore},
    database::Keyspaces,
    runtime::{
        internal::InternalRuntime,
        plugins::wpbs::plugin::{
            core_export_types::{Error, Host as CoreExportTypesHost},
            core_import_functions::Host as CoreImportFunctionsHost,
            core_import_types::{
                CoreRegistrations, CoreRegistrationsResult, Host as CoreImportTypesHost, LogLevels,
                SupportedCoreRegistrations,
            },
            core_types::Host as CoreTypesHost,
        },
    },
    utils::channels::{CoreMessages, DatabaseMessages, RuntimeMessages, RuntimeMessagesCore},
};

impl CoreTypesHost for InternalRuntime {}
impl CoreImportTypesHost for InternalRuntime {}
impl CoreExportTypesHost for InternalRuntime {}

impl CoreImportFunctionsHost for InternalRuntime {
    async fn log(&mut self, level: LogLevels, message: String) {
        match level {
            LogLevels::Trace => trace!(message),
            LogLevels::Debug => debug!(message),
            LogLevels::Info => info!(message),
            LogLevels::Warn => warn!(message),
            LogLevels::Error => error!(message),
        }
    }

    async fn get_state(&mut self, key: String) -> Option<Vec<u8>> {
        let (sender, receiver) = channel();

        let key = format!("{}-{key}", self.plugin_id);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::PluginStore,
                key.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        receiver.await.unwrap().unwrap().map(|v| v.to_vec())
    }

    async fn set_state(&mut self, key: String, value: Vec<u8>) -> Result<(), Error> {
        let (sender, receiver) = channel();

        let key = format!("{}-{key}", self.plugin_id);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                Keyspaces::PluginStore,
                key.as_bytes().to_vec(),
                value,
                sender,
            )))
            .unwrap();

        receiver.await.unwrap().map_err(|err| err.to_string())
    }

    async fn get_supported_registrations(&mut self) -> SupportedCoreRegistrations {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        plugin_permissions.core.into()
    }

    async fn register(&mut self, registrations: CoreRegistrations) -> CoreRegistrationsResult {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
            .core
            .contains(&PluginPermissionsCore::DependencyFunctions)
        {
            return CoreRegistrationsResult {
                dependency_functions: Err(Error::from(
                    "Plugin is not allowed to register dependency functions",
                )),
            };
        }

        let mut result = CoreRegistrationsResult {
            dependency_functions: Ok(vec![]),
        };

        for dependency_function in registrations.dependency_functions {
            let (sender, receiver) = channel();

            let key = format!(
                "{}/{}/{dependency_function}",
                self.plugin_metadata.registry_id, self.plugin_metadata.id
            );

            self.core_tx
                .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                    Keyspaces::DependencyFunctions,
                    key.as_bytes().to_vec(),
                    Vec::new(),
                    sender,
                )))
                .unwrap();

            receiver.await.unwrap();

            result
                .dependency_functions
                .as_mut()
                .unwrap()
                .push((dependency_function, key))
        }

        result
    }

    async fn unload(&mut self, reason: String) {
        self.core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::UnloadPlugin(self.plugin_id),
            )))
            .unwrap();

        info!(
            "The {} plugin has unloaded itself, reason: {reason}",
            self.plugin_metadata.user_id
        )
    }

    async fn shutdown(&mut self, restart: bool) -> Result<(), Error> {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
            .core
            .contains(&PluginPermissionsCore::Shutdown)
        {
            return Err(Error::from("Not allowed to call shutdown"));
        }

        let shutdown_type = if restart {
            Shutdown::Restart
        } else {
            Shutdown::Normal
        };

        self.core_tx
            .send(CoreMessages::Shutdown(shutdown_type))
            .unwrap();

        Ok(())
    }

    async fn dependency_function(
        &mut self,
        registry_id: String,
        plugin_id: String,
        function_id: String,
        params: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let (sender, receiver) = channel();

        let key = format!("{registry_id}/{plugin_id}/{function_id}");

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                key.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
            return Err(format!("The {key} dependency function was not found"));
        };

        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::CallDependencyFunction(
                    Uuid::from_slice(&response_bytes).unwrap(),
                    function_id,
                    params,
                    sender,
                ),
            )))
            .unwrap();

        receiver.await.unwrap()
    }
}
