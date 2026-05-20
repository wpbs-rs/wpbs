/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use semver::Version;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    WasiHttpCtx,
    p2::{WasiHttpCtxView, WasiHttpView},
};

mod core;
mod services;

use crate::{config::plugins::permissions::PluginPermissions, utils::channels::CoreMessages};

pub struct InternalRuntime {
    pub metadata: InternalRuntimeMetadata,
    pub wasi: WasiCtx,
    pub wasi_http: WasiHttpCtx,
    pub table: ResourceTable,
    pub core_tx: UnboundedSender<CoreMessages>,
}

pub struct InternalRuntimeMetadata {
    pub plugin_id: Uuid,
    pub registry_id: Arc<String>,
    pub id: Arc<String>,
    pub user_id: Arc<String>,
    pub version: Arc<Version>,
    pub permissions: Arc<PluginPermissions>,
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
