/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::time::Duration;

use uuid::Uuid;
use wasmtime::{
    Config, Engine, EngineWeak, Store,
    component::{HasSelf, Linker},
};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtxBuilder};
use wasmtime_wasi_http::WasiHttpCtx;

use crate::runtime::{
    RuntimePluginStatePre,
    internal::{InternalRuntime, InternalRuntimeMetadata},
    plugins::Plugin,
};

pub struct PluginBuilder {
    pub engine: Engine,
    pub linker: Linker<InternalRuntime>,
}

impl PluginBuilder {
    pub fn new() -> Self {
        let mut config = Config::new();
        config.epoch_interruption(true);
        config.wasm_component_model_map(true);

        let engine = Engine::new(&config).unwrap();

        // NOTE: The need for this can be discussed
        Self::engine_increment_epoch(engine.weak());

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

        PluginBuilder { engine, linker }
    }

    pub fn store_builder(
        &self,
        plugin_id: Uuid,
        state_pre: &RuntimePluginStatePre,
    ) -> Store<InternalRuntime> {
        let wasi = WasiCtxBuilder::new()
            .envs(&state_pre.environment)
            .preopened_dir(
                &*state_pre.workspace_directory,
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
                    plugin_id,
                    registry_id: state_pre.registry_id.clone(),
                    id: state_pre.id.clone(),
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

        store.set_epoch_deadline(6);
        store.epoch_deadline_async_yield_and_update(2);

        store
    }

    fn engine_increment_epoch(engine_weak: EngineWeak) {
        tokio::spawn(async move {
            loop {
                if let Some(engine) = engine_weak.upgrade() {
                    engine.increment_epoch();
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
}
