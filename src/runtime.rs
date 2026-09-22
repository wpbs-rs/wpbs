/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

mod internal;
pub mod plugins;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use fjall::Database;
use tokio::{
    fs,
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};
use uuid::Uuid;
use wasm_pkg_client::ContentDigest;

use crate::{
    registry::plugins::AvailablePlugin,
    runtime::plugins::{
        RuntimePlugin, RuntimePluginIndices, RuntimePluginIndicesServices, RuntimePluginMetadata,
        RuntimePluginStatePre,
        bindings::{
            core::CoreIndices,
            services::{
                discord::{
                    DiscordIndices,
                    exports::wpbs_services::discord::discord_export_functions::DiscordEvents,
                    wpbs_services::discord::discord_types::DiscordRegistrationsResultApplicationCommands,
                },
                job_scheduler::JobSchedulerIndices,
            },
        },
        builder::PluginBuilder,
    },
    utils::channels::{
        CoreMessages, RuntimeMessages, RuntimeMessagesServices, RuntimeMessagesServicesDiscord,
        RuntimeMessagesServicesJobScheduler,
    },
};

pub struct Runtime {
    plugins: Arc<RwLock<HashMap<Uuid, Arc<RuntimePlugin>>>>,
    plugin_builder: Arc<PluginBuilder>,
    rx: UnboundedReceiver<RuntimeMessages>,
}

impl Runtime {
    pub fn new(rx: UnboundedReceiver<RuntimeMessages>) -> Self {
        info!("Creating the WASI runtime");

        let plugin_builder = Arc::new(PluginBuilder::new());

        Runtime {
            plugins: Arc::new(RwLock::new(HashMap::new())),
            plugin_builder,
            rx,
        }
    }

    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    #[hotpath::measure]
    pub async fn initialize_plugins(
        &self,
        plugin_directory_path: PathBuf,
        config_name: Arc<String>,
        core_tx: UnboundedSender<CoreMessages>,
        database: Database,
        available_plugins: Vec<AvailablePlugin>,
    ) -> Result<()> {
        info!("Initializing the plugins");

        let plugin_directory_path = Arc::new(plugin_directory_path);

        let task_tracker = TaskTracker::new();

        for available_plugin in available_plugins {
            let plugin_directory_path = plugin_directory_path.clone();
            let config_name = config_name.clone();
            let database = database.clone();
            let core_tx = core_tx.clone();
            let plugins = self.plugins.clone();
            let plugin_builder = self.plugin_builder.clone();

            task_tracker.spawn(async move {
                let plugin_binary_path = if let Some(content_digest) = &available_plugin.content_digest {
                    match content_digest {
                        ContentDigest::Sha256 { hex } => plugin_directory_path.join("binaries").join("remote").join(format!("sha256:{hex}"))
                    }

                } else {
                    plugin_directory_path
                        .join("binaries")
                        .join(&available_plugin.namespace_id)
                        .join(&available_plugin.plugin_id)
                        .join(available_plugin.version.to_string()).join("plugin.wasm")
                };

                // TODO: Make workspaces configurable
                let plugin_workspace_path = plugin_directory_path
                    .join("workspaces")
                    .join(&*config_name)
                    .join(&available_plugin.user_id);

                let bytes = match fs::read(plugin_binary_path).await {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        error!(
                            "An error occurred while reading the {} plugin file: {err}",
                            available_plugin.user_id
                        );
                        return;
                    }
                };

                if let Err(err) = fs::create_dir_all(&plugin_workspace_path).await {
                    error!(
                        "Something went wrong while creating the workspace directory for the {} plugin, error: {err}",
                        available_plugin.user_id
                    );
                    return;
                }

                let instance_pre = match plugin_builder.pre_instantiate(&available_plugin.user_id, &bytes) {
                    Ok(instance_pre) => instance_pre,
                    Err(err) => {
                        error!("{err}");
                        return;
                    }
                };

                let state_pre = RuntimePluginStatePre {
                    environment: available_plugin
                        .environment
                        .into_iter()
                        .collect::<Box<[(String, String)]>>(),
                    workspace_directory_path: plugin_workspace_path,
                    metadata: Arc::new(RuntimePluginMetadata {
                        plugin_uuid: available_plugin.plugin_uuid,
                        user_id: available_plugin.user_id,
                        permissions: available_plugin.permissions,
                    }),
                    database,
                    core_tx,
                };

                let core_indices = match CoreIndices::new(&instance_pre) {
                    Ok(core_indices) => core_indices,
                    Err(err) => {
                        error!(
                            "Core indices error for the {} plugin: {err}",
                            state_pre.metadata.user_id
                        );
                        return;
                    }
                };

                let indices = RuntimePluginIndices {
                    core: core_indices,
                    services: RuntimePluginIndicesServices {
                        job_scheduler: JobSchedulerIndices::new(&instance_pre).ok(),
                        discord: DiscordIndices::new(&instance_pre).ok(),
                    }
                };

                {
                    let mut store = plugin_builder.store_builder(&state_pre);


                    let (instance, mut store) = match instance_pre.instantiate_async(&mut store).await {
                        Ok(instance) => (instance, store),
                        Err(err) => {
                            error!(
                                "Failed to instantiate the {} plugin (core), error: {err}",
                                state_pre.metadata.user_id
                            );
                            return;
                        }
                    };

                    let core_instance = match indices.core.load(&mut store, &instance) {
            Ok(core_instance) => core_instance,
            Err(err) => {
                error!("Plugin typed instance loading error: {err}");
                return;
            }
        };

                    match core_instance.wpbs_core_core_export_functions()
                        .call_initialization(&mut store, &sonic_rs::to_vec(&available_plugin.settings).unwrap())
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
                    instance_pre,
                    state_pre,
                    indices,
                });

                plugins.write().await.insert(available_plugin.plugin_uuid, plugin_context);
            });
        }

        task_tracker.close();
        task_tracker.wait().await;

        if self.plugins.read().await.is_empty() {
            bail!("No plugin initialized successfully")
        }

        Ok(())
    }

    #[hotpath::measure]
    pub fn run(mut self) -> JoinHandle<()> {
        info!("Starting the WASI runtime");

        tokio::spawn(async move {
            let task_tracker = TaskTracker::new();

            while let Some(message) = self.rx.recv().await {
                match message {
                    RuntimeMessages::Services(service_message) => match service_message {
                        RuntimeMessagesServices::JobScheduler(job_scheduler_message) => {
                            match job_scheduler_message {
                                RuntimeMessagesServicesJobScheduler::CallScheduledJob(
                                    plugin_uuid,
                                    cron,
                                ) => {
                                    let plugins = self.plugins.clone();
                                    let plugin_builder = self.plugin_builder.clone();

                                    task_tracker.spawn(async move {
                                        if let Some(plugin) = plugins
                                            .read()
                                            .await
                                            .get(&plugin_uuid)
                                            .map(|p| (*p).clone())
                                        {
                                            Self::call_scheduled_job(
                                                plugin_builder,
                                                plugin,
                                                cron,
                                            )
                                            .await;
                                        }
                                    });
                                }
                            }
                        }
                        RuntimeMessagesServices::Discord(discord_message) => {
                            match discord_message {
                                RuntimeMessagesServicesDiscord::CallDiscordApplicationCommandsResult(
                                    plugin_uuid,
                                    results,
                                ) => {
                                    let plugins = self.plugins.clone();
                                    let plugin_builder = self.plugin_builder.clone();

                                    task_tracker.spawn(async move {
                                        if let Some(plugin) = plugins
                                            .read()
                                            .await
                                            .get(&plugin_uuid)
                                            .map(|p| (*p).clone())
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
                                RuntimeMessagesServicesDiscord::CallDiscordEvent(plugin_uuid, event) => {
                                    let plugins = self.plugins.clone();
                                    let plugin_builder = self.plugin_builder.clone();

                                    task_tracker.spawn(async move {
                                        if let Some(plugin) = plugins
                                            .read()
                                            .await
                                            .get(&plugin_uuid)
                                            .map(|p| (*p).clone())
                                        {
                                            Self::call_discord_event(plugin_builder, plugin, event)
                                                .await;
                                        }
                                    });
                                }
                            }
                        }
                    },
                }
            }

            task_tracker.close();
            task_tracker.wait().await;

            self.shutdown().await;
        })
    }

    async fn call_scheduled_job(
        plugin_builder: Arc<PluginBuilder>,
        plugin: Arc<RuntimePlugin>,
        cron: Arc<String>,
    ) {
        debug!(
            "Calling the {cron} scheduled job of the {} plugin",
            plugin.state_pre.metadata.user_id
        );

        let (instance, mut store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
                return;
            }
        };

        let job_scheduler_instance = match plugin
            .indices
            .services
            .job_scheduler
            .as_ref()
            .unwrap()
            .load(&mut store, &instance)
        {
            Ok(job_scheduler_instance) => job_scheduler_instance,
            Err(err) => {
                error!("Plugin typed instance loading error: {err}");
                return;
            }
        };

        match job_scheduler_instance
            .wpbs_services_job_scheduler_job_scheduler_export_functions()
            .call_scheduled_job(store, &cron)
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
        debug!(
            "Calling the {} plugin to inform them about their Discord application command registration results",
            plugin.state_pre.metadata.user_id
        );

        let (instance, mut store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
                return;
            }
        };

        let discord_instance = match plugin
            .indices
            .services
            .discord
            .as_ref()
            .unwrap()
            .load(&mut store, &instance)
        {
            Ok(discord_instance) => discord_instance,
            Err(err) => {
                error!("Plugin typed instance loading error: {err}");
                return;
            }
        };

        if let Err(err) = discord_instance
            .wpbs_services_discord_discord_export_functions()
            .call_discord_application_commands_result(store, &results)
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
        debug!(
            "Calling the {} plugin to inform them about a Discord event",
            plugin.state_pre.metadata.user_id
        );

        let (instance, mut store) = match plugin_builder.instantiate(plugin.clone()).await {
            Ok((instance, store)) => (instance, store),
            Err(err) => {
                error!("Plugin instantiation error: {err}");
                return;
            }
        };

        let discord_instance = match plugin
            .indices
            .services
            .discord
            .as_ref()
            .unwrap()
            .load(&mut store, &instance)
        {
            Ok(discord_instance) => discord_instance,
            Err(err) => {
                error!("Plugin typed instance loading error: {err}");
                return;
            }
        };

        match discord_instance
            .wpbs_services_discord_discord_export_functions()
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
        debug!(
            "Calling shutdown for the {} plugin",
            plugin.state_pre.metadata.user_id
        );

        let mut store = plugin_builder.store_builder(&plugin.state_pre);

        let instance = match plugin.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                error!(
                    "Failed to instantiate the {} plugin, error: {err}",
                    plugin.state_pre.metadata.user_id
                );
                return;
            }
        };

        let core_instance = match plugin.indices.core.load(&mut store, &instance) {
            Ok(core_instance) => core_instance,
            Err(err) => {
                error!("Plugin typed instance loading error: {err}");
                return;
            }
        };

        match core_instance
            .wpbs_core_core_export_functions()
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
        info!("Shutting the WASI runtime down");

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
