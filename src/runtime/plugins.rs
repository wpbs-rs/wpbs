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
            services::{discord::DiscordIndices, job_scheduler::JobSchedulerIndices},
        },
    },
    utils::channels::CoreMessages,
};

pub mod builder;

pub mod bindings {
    pub mod core {
        wasmtime::component::bindgen!({ path: "./wit/core/", imports: { default: async }, exports: { default: async } });
    }

    pub mod services {
        pub mod job_scheduler {
            wasmtime::component::bindgen!({ path: "./wit/services/job-scheduler/", imports: { default: async }, exports: { default: async } });
        }

        pub mod discord {
            wasmtime::component::bindgen!({ path: "./wit/services/discord/", imports: { default: async }, exports: { default: async } });
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
    pub job_scheduler: Option<JobSchedulerIndices>,
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
