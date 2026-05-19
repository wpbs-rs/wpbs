/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use wasmtime::{
    Config, Engine,
    component::{HasSelf, Linker},
};

use crate::runtime::{internal::InternalRuntime, plugins::Plugin};

pub struct PluginBuilder {
    pub engine: Engine,
    pub linker: Linker<InternalRuntime>,
}

impl PluginBuilder {
    pub fn new() -> Self {
        // NOTE: Config notes
        // - Look into epoch interruption
        let config = Config::new();

        let engine = Engine::new(&config).unwrap();

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
}
