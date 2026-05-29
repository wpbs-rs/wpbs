/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

mod internal;
pub mod plugins;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Result;
use tokio::{
    fs,
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use tracing::{error, info};
use uuid::Uuid;
use wasmtime::component::Component;

use crate::{
    registry::plugins::AvailablePlugin,
    runtime::plugins::{
        PluginPre, RuntimePlugin, RuntimePluginMetadata, RuntimePluginStatePre,
        builder::PluginBuilder,
        wpbs::plugin::{
            core_types::PluginError,
            discord_export_types::{DiscordEvents, DiscordRegistrationsResultApplicationCommands},
        },
    },
    utils::channels::{
        CoreMessages, RuntimeMessages, RuntimeMessagesCore, RuntimeMessagesDiscord,
        RuntimeMessagesJobScheduler,
    },
};

pub struct Runtime {
    plugins: Arc<RwLock<HashMap<Uuid, Arc<RuntimePlugin>>>>,
    plugin_builder: Arc<PluginBuilder>,
    rx: UnboundedReceiver<RuntimeMessages>,
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
    pub fn run(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let task_tracker = TaskTracker::new();

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

                            task_tracker.spawn(async move {
                                if let Some(plugin) =
                                    plugins.read().await.get(&plugin_id).map(|p| (*p).clone())
                                {
                                    Self::call_dependency_function(
                                        plugin_builder,
                                        plugin,
                                        function_id,
                                        params,
                                        response_sender,
                                    )
                                    .await;
                                }
                            });
                        }
                        RuntimeMessagesCore::RemovePlugin(plugin_id) => {
                            let plugins = self.plugins.clone();
                            let plugin_builder = self.plugin_builder.clone();

                            task_tracker.spawn(async move {
                                if let Some(plugin) = plugins.write().await.remove(&plugin_id) {
                                    // TODO: Delay calling shutdown until all plugin calls have finished.
                                    Self::call_shutdown(plugin_builder, plugin).await;
                                }
                            });
                        }
                    },
                    RuntimeMessages::JobScheduler(job_scheduler_message) => {
                        match job_scheduler_message {
                            RuntimeMessagesJobScheduler::CallScheduledJob(plugin_id, job_id) => {
                                let plugins = self.plugins.clone();
                                let plugin_builder = self.plugin_builder.clone();

                                task_tracker.spawn(async move {
                                    if let Some(plugin) =
                                        plugins.read().await.get(&plugin_id).map(|p| (*p).clone())
                                    {
                                        Self::call_scheduled_job(plugin_builder, plugin, job_id)
                                            .await;
                                    }
                                });
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

                            task_tracker.spawn(async move {
                                if let Some(plugin) =
                                    plugins.read().await.get(&plugin_id).map(|p| (*p).clone())
                                {
                                    Self::call_discord_application_commands(
                                        plugin_builder,
                                        plugin,
                                        results,
                                    )
                                    .await;
                                }
                            });
                        }
                        RuntimeMessagesDiscord::CallDiscordEvent(plugin_id, event) => {
                            let plugins = self.plugins.clone();
                            let plugin_builder = self.plugin_builder.clone();

                            task_tracker.spawn(async move {
                                if let Some(plugin) =
                                    plugins.read().await.get(&plugin_id).map(|p| (*p).clone())
                                {
                                    Self::call_discord_event(plugin_builder, plugin, event).await;
                                }
                            });
                        }
                    },
                }
            }

            task_tracker.close();
            task_tracker.wait().await;

            self.shutdown().await;
        })
    }

    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    #[hotpath::measure]
    pub async fn initialize_plugins(
        &self,
        available_plugins: Vec<(Uuid, AvailablePlugin)>,
        core_tx: UnboundedSender<CoreMessages>,
        plugins_directory_path: PathBuf,
    ) -> Result<()> {
        info!("Initializing the plugins");

        let plugins_directory_path = Arc::new(plugins_directory_path);

        let mut tasks = Vec::new();

        for (plugin_uuid, plugin_metadata) in available_plugins {
            let plugins = self.plugins.clone();
            let plugin_builder = self.plugin_builder.clone();
            let core_tx = core_tx.clone();
            let plugins_directory_path = plugins_directory_path.clone();

            tasks.push(tokio::spawn(async move {
                let plugin_directory_path = plugins_directory_path
                    .join(&plugin_metadata.registry_id)
                    .join(&plugin_metadata.plugin_id)
                    .join(plugin_metadata.version.to_string());

                let bytes = match fs::read(plugin_directory_path.join("plugin.wasm")).await {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        error!(
                            "An error occurred while reading the {} plugin file: {err}",
                            plugin_metadata.user_id
                        );
                        return;
                    }
                };

                let component = match Component::new(&plugin_builder.engine, bytes) {
                    Ok(component) => component,
                    Err(err) => {
                        error!(
                            "An error occurred while creating a WASI component from the {} plugin: {err}",
                            plugin_metadata.user_id
                        );
                        return;
                    }
                };

                let workspace_plugin_directory_path = plugin_directory_path.join("workspace");

                match fs::try_exists(&workspace_plugin_directory_path).await {
                    Ok(exists) => {
                        if !exists && let Err(err) = fs::create_dir(&workspace_plugin_directory_path).await {
                            error!(
                                "Something went wrong while creating the workspace directory for the {} plugin, error: {err}",
                                plugin_metadata.user_id
                            );
                            return;
                        }
                    }
                    Err(err) => {
                        error!(
                            "Something went wrong while checking if the workspace directory of the {} plugin exists, error: {err}",
                            plugin_metadata.user_id
                        );
                        return;
                    }
                }

                let instance_pre = match plugin_builder.linker.instantiate_pre(&component) {
                    Ok(instance_pre) => instance_pre,
                    Err(err) => {
                        error!(
                            "The {} plugin returned an error while pre-instantiating (phase 1): {err}",
                            plugin_metadata.user_id
                        );
                        return;
                    }
                };

                let plugin_pre = match PluginPre::new(instance_pre) {
                    Ok(plugin_pre) => plugin_pre,
                    Err(err) => {
                        error!(
                            "The {} plugin returned an error while pre-instantiating (phase 2): {err}",
                            plugin_metadata.user_id

                        );
                        return;
                    }
                };

                let state_pre = RuntimePluginStatePre {
                    metadata: Arc::new(RuntimePluginMetadata {
                        plugin_uuid,
                        registry_id: plugin_metadata.registry_id,
                        plugin_id: plugin_metadata.plugin_id,
                        user_id: plugin_metadata.user_id,
                        version: plugin_metadata.version,
                        permissions: plugin_metadata.permissions,
                    }),
                    environment: plugin_metadata
                        .environment
                        .into_iter()
                        .collect::<Box<[(String, String)]>>(),
                    workspace_directory_path: workspace_plugin_directory_path,
                    core_tx,
                };

                {
                    let mut store = plugin_builder.store_builder(&state_pre);

                    let (instance, mut store) = match plugin_pre.instantiate_async(&mut store).await {
                        Ok(instance) => (instance, store),
                        Err(err) => {
                            error!(
                                "Failed to instantiate the {} plugin, error: {err}",
                                state_pre.metadata.user_id
                            );
                            return;
                        }
                    };

                    match instance
                        .wpbs_plugin_core_export_functions()
                        .call_initialization(&mut store, &sonic_rs::to_string(&plugin_metadata.settings).unwrap())
                        .await
                    {
                        Ok(init_result) => {
                            if let Err(err) = init_result {
                                error!(
                                    "The {} plugin returned an error while initializing: {err}",
                                    state_pre.metadata.user_id
                                );
                                return;
                            }
                        }
                        Err(err) => {
                            error!(
                                "The {} plugin experienced a critical error: {err}",
                                state_pre.metadata.user_id
                            );
                            return;
                        }
                    }
                }

                let plugin_context = Arc::new(RuntimePlugin {
                    plugin_pre,
                    state_pre,
                });

                plugins.write().await.insert(plugin_uuid, plugin_context);
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        Ok(())
    }

    // TODO:
    // - Remove trapped plugins
    // - Make plugin metadata accessible

    async fn call_dependency_function(
        plugin_builder: Arc<PluginBuilder>,
        plugin: Arc<RuntimePlugin>,
        signature: String,
        params: Vec<u8>,
        response_sender: Sender<Result<Vec<u8>, PluginError>>,
    ) {
        let (instance, store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                let err = format!("Plugin instantiation error: {err}");

                error!(err);

                response_sender.send(Err(err)).unwrap();

                return;
            }
        };

        match instance
            .wpbs_plugin_core_export_functions()
            .call_dependency_function(store, &signature, &params)
            .await
        {
            Ok(result) => {
                response_sender.send(result).unwrap();
            }
            Err(err) => {
                error!(
                    "The {} plugin experienced a critical error: {err}",
                    plugin.state_pre.metadata.user_id
                );

                response_sender
                    .send(Err(format!(
                        "The dependency plugin experienced a critical error: {err}"
                    )))
                    .unwrap();
            }
        }
    }

    async fn call_scheduled_job(
        plugin_builder: Arc<PluginBuilder>,
        plugin: Arc<RuntimePlugin>,
        job_id: Uuid,
    ) {
        let (instance, store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
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
                    error!("[{}]: {err}", plugin.state_pre.metadata.user_id);
                }
            }
            Err(err) => {
                error!(
                    "The {} plugin experienced a critical error: {err}",
                    plugin.state_pre.metadata.user_id
                );
            }
        }
    }

    async fn call_discord_application_commands(
        plugin_builder: Arc<PluginBuilder>,
        plugin: Arc<RuntimePlugin>,
        results: DiscordRegistrationsResultApplicationCommands,
    ) {
        let (instance, store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
                return;
            }
        };

        if let Err(err) = instance
            .wpbs_plugin_discord_export_functions()
            .call_discord_application_commands(store, &results)
            .await
        {
            error!(
                "The {} plugin experienced a critical error: {err}",
                plugin.state_pre.metadata.user_id
            );
        }
    }

    async fn call_discord_event(
        plugin_builder: Arc<PluginBuilder>,
        plugin: Arc<RuntimePlugin>,
        event: DiscordEvents,
    ) {
        let (instance, store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
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
                    error!("[{}]: {err}", plugin.state_pre.metadata.user_id);
                }
            }
            Err(err) => {
                error!(
                    "The {} plugin experienced a critical error: {err}",
                    plugin.state_pre.metadata.user_id
                );
            }
        }
    }

    async fn call_shutdown(plugin_builder: Arc<PluginBuilder>, plugin: Arc<RuntimePlugin>) {
        let mut store = plugin_builder.store_builder(&plugin.state_pre);

        let instance = match plugin.plugin_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                error!(
                    "Failed to instantiate the {} plugin, error: {err}",
                    plugin.state_pre.metadata.user_id
                );
                return;
            }
        };

        match instance
            .wpbs_plugin_core_export_functions()
            .call_shutdown(store)
            .await
        {
            Ok(result) => {
                if let Err(err) = result {
                    error!("[{}]: {err}", plugin.state_pre.metadata.user_id);
                }
            }
            Err(err) => {
                error!(
                    "The {} plugin experienced a critical error: {err}",
                    plugin.state_pre.metadata.user_id
                );
            }
        }
    }

    // TODO: Delay calling shutdown until all plugin calls have finished.
    async fn shutdown(self) {
        let task_tracker = TaskTracker::new();

        for (_plugin_uuid, plugin) in self.plugins.write().await.drain() {
            let plugin_builder = self.plugin_builder.clone();

            task_tracker.spawn(Self::call_shutdown(plugin_builder, plugin));
        }

        task_tracker.close();
        task_tracker.wait().await;

        Arc::into_inner(self.plugin_builder)
            .unwrap()
            .shutdown()
            .await;
    }
}
