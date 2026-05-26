/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

pub mod builder;

wasmtime::component::bindgen!({ imports: { default: async }, exports: { default: async } });
