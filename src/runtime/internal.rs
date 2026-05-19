/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    WasiHttpCtx,
    p2::{WasiHttpCtxView, WasiHttpView},
};

mod core;
mod services;

use crate::{registry::plugins::AvailablePlugin, utils::channels::CoreMessages};

pub struct InternalRuntime {
    plugin_id: Uuid,
    plugin_metadata: AvailablePlugin,
    wasi: WasiCtx,
    wasi_http: WasiHttpCtx,
    table: ResourceTable,
    core_tx: UnboundedSender<CoreMessages>,
}

impl WasiView for InternalRuntime {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for InternalRuntime {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.wasi_http,
            table: &mut self.table,
            hooks: Default::default(),
        }
    }
}

impl InternalRuntime {
    pub fn new(
        plugin_id: Uuid,
        plugin_metadata: AvailablePlugin,
        wasi: WasiCtx,
        wasi_http: WasiHttpCtx,
        table: ResourceTable,
        core_tx: UnboundedSender<CoreMessages>,
    ) -> Self {
        InternalRuntime {
            plugin_id,
            plugin_metadata,
            wasi,
            wasi_http,
            table,
            core_tx,
        }
    }
}
