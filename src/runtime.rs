/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

mod builder;
mod internal;
pub mod plugins;

use std::{collections::HashMap, fs, path::Path};

use anyhow::Result;
use tokio::{
    sync::{
        Mutex, RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tracing::{error, info};
use uuid::Uuid;
use wasmtime::{Store, component::Component};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::{
    registry::plugins::AvailablePlugin,
    runtime::{
        builder::PluginBuilder,
        internal::InternalRuntime,
        plugins::{Plugin, wbps::plugin::discord_export_types::DiscordEvents},
    },
    utils::channels::{
        CoreMessages, RuntimeMessages, RuntimeMessagesDiscord, RuntimeMessagesJobScheduler,
    },
};

pub struct Runtime {
    plugins: RwLock<HashMap<Uuid, RuntimePlugin>>,
    rx: UnboundedReceiver<RuntimeMessages>,
}

pub struct RuntimePlugin {
    instance: Plugin,
    store: Mutex<Store<InternalRuntime>>, // TODO: Add async support
}

impl Runtime {
    pub fn new(rx: UnboundedReceiver<RuntimeMessages>) -> Self {
        info!("Creating the WASI runtime");

        Runtime {
            plugins: RwLock::new(HashMap::new()),
            rx,
        }
    }

    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    RuntimeMessages::JobScheduler(job_scheduler_message) => {
                        match job_scheduler_message {
                            RuntimeMessagesJobScheduler::CallScheduledJob(plugin_id, job_id) => {
                                self.call_scheduled_job(plugin_id, job_id).await;
                            }
                        }
                    }
                    RuntimeMessages::Discord(discord_message) => match discord_message {
                        RuntimeMessagesDiscord::CallDiscordEvent(plugin_id, event) => {
                            self.call_discord_event(plugin_id, &event).await;
                        }
                    },
                }
            }

            self.shutdown().await;
        })
    }

    pub async fn initialize_plugins(
        &self,
        available_plugins: Vec<(Uuid, AvailablePlugin)>,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_directory: &Path,
    ) -> Result<(), ()> {
        info!("Creating the WASI plugin builder");
        let plugin_builder = PluginBuilder::new();

        info!("Initializing the plugins");

        for (plugin_id, plugin) in available_plugins {
            let plugin_user_id = plugin.user_id.clone();
            let plugin_settings = plugin.settings.clone();

            let plugin_directory = plugin_directory
                .join(&plugin.registry_id)
                .join(&plugin.id)
                .join(plugin.version.to_string());

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

            let component = match Component::new(&plugin_builder.engine, bytes) {
                Ok(component) => component,
                Err(err) => {
                    error!(
                        "An error occured while creating a WASI component from the {} plugin: {err}",
                        plugin_user_id
                    );
                    continue;
                }
            };

            let env: Box<[(&str, &str)]> = plugin
                .environment
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            let workspace_plugin_dir = plugin_directory.join("workspace");

            match fs::exists(&workspace_plugin_dir) {
                Ok(exists) => {
                    if !exists && let Err(err) = fs::create_dir(&workspace_plugin_dir) {
                        error!(
                            "Something went wrong while creating the workspace directory for the {} plugin, error: {err}",
                            plugin_user_id
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "Something went wrong while checking if the workspace directory of the {} plugin exists, error: {err}",
                        plugin_user_id
                    );
                    return Err(());
                }
            }

            let wasi = WasiCtxBuilder::new()
                .envs(&env)
                .preopened_dir(workspace_plugin_dir, "/", DirPerms::all(), FilePerms::all())
                .unwrap()
                .build();

            let mut store = Store::<InternalRuntime>::new(
                &plugin_builder.engine,
                InternalRuntime::new(
                    plugin_id,
                    plugin,
                    wasi,
                    WasiHttpCtx::new(),
                    ResourceTable::new(),
                    core_tx.clone(),
                ),
            );

            let instance =
                match Plugin::instantiate_async(&mut store, &component, &plugin_builder.linker)
                    .await
                {
                    Ok(instance) => instance,
                    Err(err) => {
                        error!(
                            "Failed to instantiate the {} plugin, error: {err}",
                            plugin_user_id
                        );
                        continue;
                    }
                };

            match instance
                .wbps_plugin_core_export_functions()
                .call_initialization(&mut store, &sonic_rs::to_vec(&plugin_settings).unwrap())
                .await
            {
                Ok(init_result) => {
                    if let Err(err) = init_result {
                        error!(
                            "the {} plugin returned an error while intiializing: {err}",
                            plugin_user_id
                        );
                        continue;
                    }
                }
                Err(err) => {
                    error!(
                        "The {} plugin exprienced a critical error: {err}",
                        plugin_user_id
                    );
                    continue;
                }
            };

            let plugin_context = RuntimePlugin {
                instance,
                store: Mutex::new(store),
            };

            self.plugins.write().await.insert(plugin_id, plugin_context);
        }

        Ok(())
    }

    // TODO: Remove trapped plugins

    async fn call_discord_event(&self, plugin_id: Uuid, event: &DiscordEvents) {
        let plugins = self.plugins.read().await;
        let plugin = plugins.get(&plugin_id).unwrap();

        match plugin
            .instance
            .wbps_plugin_discord_export_functions()
            .call_discord_event(&mut *plugin.store.lock().await, event)
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

    async fn call_scheduled_job(&self, plugin_id: Uuid, uuid: Uuid) {
        let plugins = self.plugins.read().await;
        let plugin = plugins.get(&plugin_id).unwrap();

        match plugin
            .instance
            .wbps_plugin_job_scheduler_export_functions()
            .call_scheduled_job(&mut *plugin.store.lock().await, &uuid.to_string())
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

    async fn call_shutdown(&self, plugin_id: Uuid) {
        let plugins = self.plugins.read().await;
        let plugin = plugins.get(&plugin_id).unwrap();

        match plugin
            .instance
            .wbps_plugin_core_export_functions()
            .call_shutdown(&mut *plugin.store.lock().await)
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

    async fn shutdown(&self) {
        // TODO: Allow all plugin calls to finish and then call the shutdown methods
        // This will be achieved by closing the plugin call channel tasks which then will call
        // shutdown one more time before returning
    }
}
