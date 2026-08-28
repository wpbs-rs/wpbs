/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::future;

use fjall::KeyspaceCreateOptions;
use tracing::{debug, error, info, trace, warn};

use crate::{
    Shutdown,
    config::plugins::permissions::core::PluginPermissionsCore,
    runtime::{
        internal::InternalRuntime,
        plugins::bindings::core::wpbs::{
            core::{
                core_import_functions::Host as CoreImportFunctionsHost,
                core_types::{Host as CoreTypesHost, LogLevels},
            },
            shared::shared_types::{Host as SharedTypesHost, HostError},
        },
    },
    utils::channels::CoreMessages,
};

impl SharedTypesHost for InternalRuntime {}

impl CoreTypesHost for InternalRuntime {}

impl CoreImportFunctionsHost for InternalRuntime {
    fn log(&mut self, level: LogLevels, message: String) -> impl Future<Output = ()> + Send {
        match level {
            LogLevels::Trace => trace!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Debug => debug!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Info => info!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Warn => warn!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Error => error!("[{}]: {message}", self.metadata.user_id),
        }

        future::ready(())
    }

    fn get_value(
        &mut self,
        key: String,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, HostError>> + Send {
        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        let plugin_store_keyspace = match self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
        {
            Ok(plugin_store_keyspace) => plugin_store_keyspace,
            Err(err) => return future::ready(Err(err.to_string())),
        };

        future::ready(
            plugin_store_keyspace
                .get(&key)
                .map_err(|err| err.to_string())
                .map(|r| r.map(|s| s.to_vec())),
        )
    }

    fn set_value(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> impl Future<Output = Result<(), HostError>> + Send {
        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        let plugin_store_keyspace = match self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
        {
            Ok(plugin_store_keyspace) => plugin_store_keyspace,
            Err(err) => return future::ready(Err(err.to_string())),
        };

        future::ready(
            plugin_store_keyspace
                .insert(&key, &value)
                .map_err(|err| err.to_string()),
        )
    }

    fn remove_value(&mut self, key: String) -> impl Future<Output = Result<(), HostError>> + Send {
        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        let plugin_store_keyspace = match self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
        {
            Ok(plugin_store_keyspace) => plugin_store_keyspace,
            Err(err) => return future::ready(Err(err.to_string())),
        };

        future::ready(
            plugin_store_keyspace
                .remove(&key)
                .map_err(|err| err.to_string()),
        )
    }

    fn get_all_entries(
        &mut self,
    ) -> impl Future<Output = Result<Vec<(String, Vec<u8>)>, HostError>> + Send {
        let plugin_store_keyspace = match self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
        {
            Ok(plugin_store_keyspace) => plugin_store_keyspace,
            Err(err) => return future::ready(Err(err.to_string())),
        };

        future::ready(
            plugin_store_keyspace
                .prefix(self.metadata.plugin_uuid.as_bytes())
                .map(|g| {
                    let (key, value) = g.into_inner()?;

                    Ok((String::from_utf8(key.to_vec()).unwrap(), value.to_vec()))
                })
                .collect::<Result<Vec<(String, Vec<u8>)>, anyhow::Error>>()
                .map_err(|err| err.to_string()),
        )
    }

    fn clear_all_entries(&mut self) -> impl Future<Output = Result<(), HostError>> + Send {
        let plugin_store_keyspace = match self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
        {
            Ok(plugin_store_keyspace) => plugin_store_keyspace,
            Err(err) => return future::ready(Err(err.to_string())),
        };

        let entries = plugin_store_keyspace.prefix(self.metadata.plugin_uuid.as_bytes());

        for entry in entries {
            let key = match entry.key() {
                Ok(key) => key,
                Err(err) => return future::ready(Err(err.to_string())),
            };

            if let Err(err) = plugin_store_keyspace.remove(key) {
                return future::ready(Err(err.to_string()));
            }
        }

        future::ready(Ok(()))
    }

    fn shutdown(&mut self, restart: bool) -> impl Future<Output = Result<(), HostError>> + Send {
        if !self
            .metadata
            .permissions
            .core
            .contains(&PluginPermissionsCore::Shutdown)
        {
            return future::ready(Err(HostError::from(
                "Plugin does not have the permission to call shutdown",
            )));
        }

        let shutdown_kind = if restart {
            Shutdown::Restart
        } else {
            Shutdown::Normal
        };

        self.core_tx
            .send(CoreMessages::Shutdown(shutdown_kind))
            .unwrap();

        future::ready(Ok(()))
    }
}
