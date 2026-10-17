/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{path::PathBuf, sync::Arc};

use fjall::Database;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use wasmtime::component::InstancePre;

use crate::{
    config::plugins::permissions::PluginPermissions,
    runtime::{
        internal::InternalRuntime,
        plugins::bindings::{
            core::CoreIndices,
            services::{discord::DiscordIndices, scheduler::SchedulerIndices},
        },
    },
    utils::channels::CoreMessages,
};

pub mod builder;

pub mod bindings {
    pub mod core {
        wasmtime::component::bindgen!({ path: "./wit/core/" });
    }

    pub mod services {
        pub mod scheduler {
            wasmtime::component::bindgen!({ path: "./wit/services/scheduler", with: { "wpbs:shared/shared-types@0.1.0": crate::runtime::plugins::bindings::core::wpbs::shared::shared_types }});
        }

        pub mod discord {
            wasmtime::component::bindgen!({ path: "./wit/services/discord", with: { "wpbs:shared/shared-types@0.1.0": crate::runtime::plugins::bindings::core::wpbs::shared::shared_types }});
        }
    }
}

pub struct RuntimePlugin {
    pub instance_pre: InstancePre<InternalRuntime>,
    pub state_pre: RuntimePluginStatePre,
    pub indices: RuntimePluginIndices,
}

pub struct RuntimePluginIndices {
    pub core: CoreIndices,
    pub services: RuntimePluginIndicesServices,
}

pub struct RuntimePluginIndicesServices {
    pub scheduler: Option<SchedulerIndices>,
    pub discord: Option<DiscordIndices>,
}

pub struct RuntimePluginStatePre {
    pub environment: Box<[(String, String)]>,
    pub workspace_directory_path: PathBuf,
    pub metadata: Arc<RuntimePluginMetadata>,
    pub database: Database,
    pub core_tx: UnboundedSender<CoreMessages>,
}

pub struct RuntimePluginMetadata {
    pub plugin_uuid: Uuid,
    pub user_id: String,
    pub permissions: PluginPermissions,
}
