/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

mod core;
mod discord;
mod job_scheduler;

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
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
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
