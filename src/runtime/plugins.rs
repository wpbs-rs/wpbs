/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{path::PathBuf, sync::Arc};

use semver::Version;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::{
    config::plugins::permissions::PluginPermissions, runtime::internal::InternalRuntime,
    utils::channels::CoreMessages,
};

pub mod builder;

wasmtime::component::bindgen!({ imports: { default: async }, exports: { default: async } });

pub struct RuntimePlugin {
    pub plugin_pre: PluginPre<InternalRuntime>,
    pub state_pre: RuntimePluginStatePre,
}

pub struct RuntimePluginStatePre {
    pub metadata: Arc<RuntimePluginMetadata>,
    pub environment: Box<[(String, String)]>,
    pub workspace_directory_path: PathBuf,
    pub core_tx: UnboundedSender<CoreMessages>,
}

pub struct RuntimePluginMetadata {
    pub plugin_uuid: Uuid,
    pub registry_id: String,
    pub plugin_id: String,
    pub user_id: String,
    pub version: Version,
    pub permissions: PluginPermissions,
}
