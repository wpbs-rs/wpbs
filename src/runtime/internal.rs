/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    WasiHttpCtx,
    p2::{WasiHttpCtxView, WasiHttpView},
};

mod core;
mod services;

use crate::{runtime::plugins::RuntimePluginMetadata, utils::channels::CoreMessages};

pub struct InternalRuntime {
    pub metadata: Arc<RuntimePluginMetadata>,
    pub wasi: WasiCtx,
    pub wasi_http: WasiHttpCtx,
    pub table: ResourceTable,
    pub core_tx: UnboundedSender<CoreMessages>,
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
