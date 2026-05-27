/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use tokio::{sync::RwLock, task::JoinHandle};
use uuid::Uuid;
use wasmtime::{
    Config, Engine, EngineWeak, Store,
    component::{HasSelf, Linker},
};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::runtime::{
    RuntimePlugin, RuntimePluginStatePre,
    internal::{InternalRuntime, InternalRuntimeMetadata},
    plugins::Plugin,
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
        let mut config = Config::new();
        config.epoch_interruption(true);
        config.wasm_component_model_map(true);

        let engine = Engine::new(&config).unwrap();

        let epoch_handler = Self::engine_increment_epoch(engine.weak());

        // NOTE: Linker notes
        // - Better way to link dependency plugins (not yet supported with the component model)
        // - Better way to add logging support
        let mut linker = Linker::<InternalRuntime>::new(&engine);

        wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();

        Plugin::add_to_linker::<InternalRuntime, HasSelf<InternalRuntime>>(
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

    pub fn store_builder(
        &self,
        plugin_uuid: Uuid,
        state_pre: &RuntimePluginStatePre,
    ) -> Store<InternalRuntime> {
        let wasi = WasiCtxBuilder::new()
            .envs(&state_pre.environment)
            .preopened_dir(
                &*state_pre.workspace_directory_path,
                "/",
                DirPerms::all(),
                FilePerms::all(),
            )
            .unwrap()
            .build();

        let mut store = Store::<InternalRuntime>::new(
            &self.engine,
            InternalRuntime {
                metadata: InternalRuntimeMetadata {
                    plugin_uuid,
                    registry_id: state_pre.registry_id.clone(),
                    plugin_id: state_pre.id.clone(),
                    user_id: state_pre.user_id.clone(),
                    version: state_pre.version.clone(),
                    permissions: state_pre.permissions.clone(),
                },
                wasi,
                wasi_http: WasiHttpCtx::new(),
                table: ResourceTable::new(),
                core_tx: state_pre.core_tx.clone(),
            },
        );

        store.set_epoch_deadline(EPOCH_DEADLINE);
        store.epoch_deadline_async_yield_and_update(EPOCH_DEADLINE_ASYNC_YIELD_AND_UPDATE);

        store
    }

    #[hotpath::measure]
    pub async fn instantiate(
        &self,
        plugins: Arc<RwLock<HashMap<Uuid, RuntimePlugin>>>,
        plugin_id: Uuid,
    ) -> Result<(Plugin, Store<InternalRuntime>)> {
        if let Some(plugin) = plugins.read().await.get(&plugin_id) {
            let mut store = self.store_builder(plugin_id, &plugin.state_pre);

            let instance = plugin.plugin_pre.instantiate_async(&mut store).await?;

            Ok((instance, store))
        } else {
            bail!("Runtime has no plugin with the provided ID: {plugin_id}");
        }
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
