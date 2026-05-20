/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

mod builder;
mod internal;
pub mod plugins;

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, bail};
use semver::Version;
use tokio::{
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
    task::JoinHandle,
};
use tracing::{error, info};
use uuid::Uuid;
use wasmtime::{Store, component::Component};

use crate::{
    config::plugins::permissions::PluginPermissions,
    registry::plugins::AvailablePlugin,
    runtime::{
        builder::PluginBuilder,
        internal::InternalRuntime,
        plugins::{
            Plugin, PluginPre,
            wpbs::plugin::discord_export_types::{
                DiscordEvents, DiscordRegistrationsResultApplicationCommands, Error,
            },
        },
    },
    utils::channels::{
        CoreMessages, RuntimeMessages, RuntimeMessagesCore, RuntimeMessagesDiscord,
        RuntimeMessagesJobScheduler,
    },
};

pub struct Runtime {
    plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
    plugin_builder: Arc<PluginBuilder>,
    rx: UnboundedReceiver<RuntimeMessages>,
}

pub struct RuntimePlugin {
    plugin_pre: PluginPre<InternalRuntime>,
    state_pre: RuntimePluginStatePre,
}

pub struct RuntimePluginStatePre {
    pub registry_id: Arc<String>,
    pub id: Arc<String>,
    pub user_id: Arc<String>,
    pub version: Arc<Version>,
    pub permissions: Arc<PluginPermissions>,
    pub environment: Arc<[(String, String)]>,
    pub workspace_directory: Arc<PathBuf>,
    pub core_tx: UnboundedSender<CoreMessages>,
}

impl Runtime {
    pub fn new(rx: UnboundedReceiver<RuntimeMessages>) -> Self {
        info!("Creating the WASI plugin builder");

        let plugin_builder = Arc::new(PluginBuilder::new());

        info!("Creating the WASI runtime");

        Runtime {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            plugin_builder,
            rx,
        }
    }

    #[hotpath::measure]
    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    RuntimeMessages::Core(core_message) => match core_message {
                        RuntimeMessagesCore::CallDependencyFunction(
                            plugin_id,
                            function_id,
                            params,
                            response_sender,
                        ) => {
                            let plugins = self.plugins.clone();
                            let plugin_builder = self.plugin_builder.clone();

                            tokio::spawn(Self::call_dependency_function(
                                plugins,
                                plugin_builder,
                                plugin_id,
                                function_id,
                                params,
                                response_sender,
                            ));
                        }
                        RuntimeMessagesCore::UnloadPlugin(plugin) => {
                            let plugins = self.plugins.clone();

                            tokio::spawn(async move {
                                plugins.write().await.remove(&plugin);
                            });
                        }
                        RuntimeMessagesCore::Shutdown => {
                            self.rx.close();
                        }
                    },
                    RuntimeMessages::JobScheduler(job_scheduler_message) => {
                        match job_scheduler_message {
                            RuntimeMessagesJobScheduler::CallScheduledJob(plugin_id, job_id) => {
                                let plugins = self.plugins.clone();
                                let plugin_builder = self.plugin_builder.clone();

                                tokio::spawn(Self::call_scheduled_job(
                                    plugins,
                                    plugin_builder,
                                    plugin_id,
                                    job_id,
                                ));
                            }
                        }
                    }
                    RuntimeMessages::Discord(discord_message) => match discord_message {
                        RuntimeMessagesDiscord::CallDiscordApplicationCommands(
                            plugin_id,
                            results,
                        ) => {
                            let plugins = self.plugins.clone();
                            let plugin_builder = self.plugin_builder.clone();

                            tokio::spawn(Self::call_discord_application_commands(
                                plugins,
                                plugin_builder,
                                plugin_id,
                                results,
                            ));
                        }
                        RuntimeMessagesDiscord::CallDiscordEvent(plugin_id, event) => {
                            let plugins = self.plugins.clone();
                            let plugin_builder = self.plugin_builder.clone();

                            tokio::spawn(Self::call_discord_event(
                                plugins,
                                plugin_builder,
                                plugin_id,
                                event,
                            ));
                        }
                    },
                }
            }

            self.shutdown().await;
        })
    }

    #[hotpath::measure]
    pub async fn initialize_plugins(
        &self,
        available_plugins: Vec<(Uuid, AvailablePlugin)>,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_directory: &Path,
    ) -> Result<()> {
        info!("Initializing the plugins");

        for (plugin_id, plugin_metadata) in available_plugins {
            let plugin_user_id = plugin_metadata.user_id.clone();
            let plugin_settings = plugin_metadata.settings.clone();

            let plugin_directory = plugin_directory
                .join(&plugin_metadata.registry_id)
                .join(&plugin_metadata.id)
                .join(plugin_metadata.version.to_string());

            let bytes = match fs::read(plugin_directory.join("plugin.wasm")) {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!(
                        "An error occured while reading the {} plugin file: {err}",
                        plugin_user_id
                    );
                    continue;
                }
            };

            let component = match Component::new(&self.plugin_builder.engine, bytes) {
                Ok(component) => component,
                Err(err) => {
                    error!(
                        "An error occured while creating a WASI component from the {} plugin: {err}",
                        plugin_user_id
                    );
                    continue;
                }
            };

            let workspace_plugin_directory = plugin_directory.join("workspace");

            match fs::exists(&workspace_plugin_directory) {
                Ok(exists) => {
                    if !exists && let Err(err) = fs::create_dir(&workspace_plugin_directory) {
                        bail!(
                            "Something went wrong while creating the workspace directory for the {} plugin, error: {err}",
                            plugin_user_id
                        );
                    }
                }
                Err(err) => {
                    bail!(
                        "Something went wrong while checking if the workspace directory of the {} plugin exists, error: {err}",
                        plugin_user_id
                    );
                }
            }

            let instance_pre = match self.plugin_builder.linker.instantiate_pre(&component) {
                Ok(instance_pre) => instance_pre,
                Err(err) => {
                    error!(
                        "The {plugin_user_id} plugin returned an error while pre_instantiating (r1): {err}"
                    );
                    continue;
                }
            };

            let plugin_pre = match PluginPre::new(instance_pre) {
                Ok(plugin_pre) => plugin_pre,
                Err(err) => {
                    error!(
                        "The {plugin_user_id} plugin returned an error while instantiating (r2): {err}"
                    );
                    continue;
                }
            };

            let state_pre = RuntimePluginStatePre {
                registry_id: Arc::new(plugin_metadata.registry_id),
                id: Arc::new(plugin_metadata.id),
                user_id: Arc::new(plugin_metadata.user_id),
                version: Arc::new(plugin_metadata.version),
                permissions: Arc::new(plugin_metadata.permissions),
                environment: plugin_metadata
                    .environment
                    .into_iter()
                    .collect::<Arc<[(String, String)]>>(),
                workspace_directory: Arc::new(workspace_plugin_directory),
                core_tx: core_tx.clone(),
            };

            {
                let (instance, mut store) = match Self::instantiate(
                    self.plugins.clone(),
                    self.plugin_builder.clone(),
                    plugin_id,
                )
                .await
                {
                    Ok((instance, store)) => (instance, store),
                    Err(err) => {
                        error!(
                            "The {plugin_user_id} plugin returned an error while instantiating: {err}"
                        );
                        continue;
                    }
                };

                match instance
                    .wpbs_plugin_core_export_functions()
                    .call_initialization(&mut store, &sonic_rs::to_vec(&plugin_settings).unwrap())
                    .await
                {
                    Ok(init_result) => {
                        if let Err(err) = init_result {
                            error!(
                                "The {plugin_user_id} plugin returned an error while intializing: {err}"
                            );
                            continue;
                        }
                    }
                    Err(err) => {
                        error!("The {plugin_user_id} plugin exprienced a critical error: {err}");
                        continue;
                    }
                };
            }

            let plugin_context = RuntimePlugin {
                plugin_pre,
                state_pre,
            };

            self.plugins.write().await.insert(plugin_id, plugin_context);
        }

        Ok(())
    }

    async fn instantiate(
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_builder: Arc<PluginBuilder>,
        plugin_id: Uuid,
    ) -> Result<(Plugin, Store<InternalRuntime>)> {
        let plugins = plugins.read().await;
        let plugin = plugins.get(&plugin_id).unwrap();

        let mut store = plugin_builder.store_builder(plugin_id, &plugin.state_pre);

        match plugin.plugin_pre.instantiate_async(&mut store).await {
            Ok(instance) => Ok((instance, store)),
            Err(err) => {
                bail!(
                    "Failed to instantiate the {} plugin, error: {err}",
                    plugin.state_pre.user_id
                );
            }
        }
    }

    // TODO: Remove trapped plugins

    async fn call_dependency_function(
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_builder: Arc<PluginBuilder>,
        plugin_id: Uuid,
        function_id: String,
        params: Vec<u8>,
        response_sender: Sender<Result<Vec<u8>, Error>>,
    ) {
        let (instance, store) = match Self::instantiate(plugins, plugin_builder, plugin_id).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("{err}");
                return;
            }
        };

        match instance
            .wpbs_plugin_core_export_functions()
            .call_dependency_function(store, &function_id, &params)
            .await
        {
            Ok(result) => {
                let _ = response_sender.send(result);
            }
            Err(err) => {
                let err = format!("The {plugin_id} plugin exprienced a critical error: {err}");

                error!(err);

                let _ = response_sender.send(Err(err));
            }
        };
    }

    async fn call_scheduled_job(
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_builder: Arc<PluginBuilder>,
        plugin_id: Uuid,
        job_id: Uuid,
    ) {
        let (instance, store) = match Self::instantiate(plugins, plugin_builder, plugin_id).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("{err}");
                return;
            }
        };

        match instance
            .wpbs_plugin_job_scheduler_export_functions()
            .call_scheduled_job(store, &job_id.to_string())
            .await
        {
            Ok(result) => {
                if let Err(err) = result {
                    error!("The {plugin_id} plugin returned an error: {err}");
                }
            }
            Err(err) => {
                error!("The {plugin_id} plugin exprienced a critical error: {err}");
            }
        }
    }

    async fn call_discord_application_commands(
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_builder: Arc<PluginBuilder>,
        plugin_id: Uuid,
        results: DiscordRegistrationsResultApplicationCommands,
    ) {
        let (instance, store) = match Self::instantiate(plugins, plugin_builder, plugin_id).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("{err}");
                return;
            }
        };

        match instance
            .wpbs_plugin_discord_export_functions()
            .call_discord_application_commands(store, &results)
            .await
        {
            Ok(result) => {
                if let Err(err) = result {
                    error!("The {plugin_id} plugin returned an error: {err}");
                }
            }
            Err(err) => {
                error!("The {plugin_id} plugin exprienced a critical error: {err}");
            }
        }
    }

    async fn call_discord_event(
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_builder: Arc<PluginBuilder>,
        plugin_id: Uuid,
        event: DiscordEvents,
    ) {
        let (instance, store) = match Self::instantiate(plugins, plugin_builder, plugin_id).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("{err}");
                return;
            }
        };

        match instance
            .wpbs_plugin_discord_export_functions()
            .call_discord_event(store, &event)
            .await
        {
            Ok(result) => {
                if let Err(err) = result {
                    error!("The {plugin_id} plugin returned an error: {err}");
                }
            }
            Err(err) => {
                error!("The {plugin_id} plugin exprienced a critical error: {err}");
            }
        }
    }

    async fn shutdown(self) {
        // TODO: Allow all plugin calls to finish and then call the shutdown methods
        // This will be achieved by closing the plugin call channel tasks which then will call
        // shutdown one more time before returning
        // Bellow code will get replaced with channel closers

        for (plugin_id, plugin) in self.plugins.write().await.drain() {
            let mut store = self
                .plugin_builder
                .store_builder(plugin_id, &plugin.state_pre);

            let instance = match plugin.plugin_pre.instantiate_async(&mut store).await {
                Ok(instance) => instance,
                Err(err) => {
                    error!(
                        "Failed to instantiate the {} plugin, error: {err}",
                        plugin.state_pre.user_id
                    );
                    continue;
                }
            };

            match instance
                .wpbs_plugin_core_export_functions()
                .call_shutdown(store)
                .await
            {
                Ok(result) => {
                    if let Err(err) = result {
                        error!("The {plugin_id} plugin returned an error: {err}");
                    }
                }
                Err(err) => {
                    error!("The {plugin_id} plugin exprienced a critical error: {err}");
                }
            }
        }
    }
}
