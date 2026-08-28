/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{sync::Arc, time::Duration};

use anyhow::{Result, bail};
use tokio::task::JoinHandle;
use tracing::debug;
use wasmtime::{
    Config, Engine, EngineWeak, Store,
    component::{Component, HasSelf, Instance, InstancePre, Linker},
};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::runtime::{
    RuntimePlugin,
    internal::InternalRuntime,
    plugins::{
        RuntimePluginStatePre,
        bindings::{
            core::Core,
            services::{discord::Discord, job_scheduler::JobScheduler},
        },
    },
};

static EPOCH_DEADLINE: u64 = 6;
static EPOCH_DEADLINE_ASYNC_YIELD_AND_UPDATE: u64 = 2;
static INCREMENT_EPOCH_INTERVAL_SECS: u64 = 5;

pub struct PluginBuilder {
    pub engine: Engine,
    pub linker: Linker<InternalRuntime>,
    epoch_handler: JoinHandle<()>,
}

impl PluginBuilder {
    pub fn new() -> Self {
        debug!("Creating the WASI plugin builder");

        let mut config = Config::new();
        config.epoch_interruption(true);
        config.wasm_component_model_map(true);

        let engine = Engine::new(&config).unwrap();

        let epoch_handler = Self::engine_increment_epoch(engine.weak());

        // NOTE: Possible linker improvements
        // - Better tracing/logging support (WASI-tracing)
        // - Better key-value store support (WASI-keyvalue)
        // - Better runtime config support (WASI-config)
        let mut linker = Linker::<InternalRuntime>::new(&engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();

        Core::add_to_linker::<InternalRuntime, HasSelf<InternalRuntime>>(
            &mut linker,
            |internal_runtime| internal_runtime,
        )
        .unwrap();

        JobScheduler::add_to_linker::<InternalRuntime, HasSelf<InternalRuntime>>(
            &mut linker,
            |internal_runtime| internal_runtime,
        )
        .unwrap();

        Discord::add_to_linker::<InternalRuntime, HasSelf<InternalRuntime>>(
            &mut linker,
            |internal_runtime| internal_runtime,
        )
        .unwrap();

        PluginBuilder {
            engine,
            linker,
            epoch_handler,
        }
    }

    pub fn store_builder(&self, state_pre: &RuntimePluginStatePre) -> Store<InternalRuntime> {
        let wasi = WasiCtxBuilder::new()
            .envs(&state_pre.environment)
            .preopened_dir(
                &*state_pre.workspace_directory_path,
                ".",
                DirPerms::all(),
                FilePerms::all(),
            )
            .unwrap()
            .build();

        let mut store = Store::<InternalRuntime>::new(
            &self.engine,
            InternalRuntime {
                wasi,
                wasi_http: WasiHttpCtx::new(),
                table: ResourceTable::new(),
                metadata: state_pre.metadata.clone(),
                database: state_pre.database.clone(),
                core_tx: state_pre.core_tx.clone(),
            },
        );

        store.set_epoch_deadline(EPOCH_DEADLINE);
        store.epoch_deadline_async_yield_and_update(EPOCH_DEADLINE_ASYNC_YIELD_AND_UPDATE);

        store
    }

    #[hotpath::measure]
    pub fn pre_instantiate(
        &self,
        plugin_user_id: &str,
        bytes: &[u8],
    ) -> Result<InstancePre<InternalRuntime>> {
        let component = match Component::new(&self.engine, bytes) {
            Ok(component) => component,
            Err(err) => {
                bail!(
                    "An error occurred while creating a WASI component from the {plugin_user_id} plugin: {err}"
                );
            }
        };

        match self.linker.instantiate_pre(&component) {
            Ok(instance_pre) => Ok(instance_pre),
            Err(err) => {
                bail!(
                    "The {plugin_user_id} plugin returned an error while pre-instantiating: {err}"
                );
            }
        }
    }

    #[hotpath::measure]
    pub async fn instantiate(
        &self,
        plugin: Arc<RuntimePlugin>,
    ) -> Result<(Instance, Store<InternalRuntime>)> {
        let mut store = self.store_builder(&plugin.state_pre);

        let instance = plugin.instance_pre.instantiate_async(&mut store).await?;

        Ok((instance, store))
    }

    fn engine_increment_epoch(engine_weak: EngineWeak) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Some(engine) = engine_weak.upgrade() {
                    engine.increment_epoch();
                }

                tokio::time::sleep(Duration::from_secs(INCREMENT_EPOCH_INTERVAL_SECS)).await;
            }
        })
    }

    pub async fn shutdown(self) {
        self.epoch_handler.abort();
        let _ = self.epoch_handler.await;
    }
}
