/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use anyhow::Result;
use fjall::{Iter, Slice};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot::Sender as OSSender,
};
use uuid::Uuid;

use crate::{
    Shutdown,
    database::Keyspaces,
    runtime::plugins::wpbs::plugin::{
        core_types::PluginError,
        discord_export_types::{DiscordEvents, DiscordRegistrationsResultApplicationCommands},
        discord_import_types::{DiscordRequests, DiscordResponses},
    },
};

pub enum CoreMessages {
    DatabaseModule(DatabaseMessages),

    Runtime(RuntimeMessages),

    JobScheduler(JobSchedulerMessages),
    Discord(DiscordMessages),

    Shutdown(Shutdown),
}

pub enum DatabaseMessages {
    Get(Keyspaces, Vec<u8>, OSSender<Result<Option<Slice>>>),
    #[allow(unused)]
    Range(Keyspaces, Vec<u8>, Vec<u8>, bool, OSSender<Result<Iter>>),
    #[allow(unused)]
    Prefix(Keyspaces, Vec<u8>, OSSender<Result<Iter>>),
    GetAllEntries(Keyspaces, OSSender<Result<Vec<(Slice, Slice)>>>),
    #[allow(unused)]
    GetAllKeys(Keyspaces, OSSender<Result<Vec<Slice>>>),
    #[allow(unused)]
    GetAllValues(Keyspaces, OSSender<Result<Vec<Slice>>>),
    Insert(Keyspaces, Vec<u8>, Vec<u8>, OSSender<Result<()>>),
    #[allow(unused)]
    Remove(Keyspaces, Vec<u8>, OSSender<Result<()>>),
    #[allow(unused)]
    ContainsKey(Keyspaces, Vec<u8>, OSSender<Result<bool>>),
    Clear(Keyspaces, OSSender<Result<()>>),
}

pub enum RuntimeMessages {
    Core(RuntimeMessagesCore),
    JobScheduler(RuntimeMessagesJobScheduler),
    Discord(RuntimeMessagesDiscord),
}

pub enum RuntimeMessagesCore {
    CallDependencyFunction(
        Uuid,
        String,
        Vec<u8>,
        OSSender<Result<Vec<u8>, PluginError>>,
    ),
    RemovePlugin(Uuid),
}

pub enum RuntimeMessagesJobScheduler {
    CallScheduledJob(Uuid, Uuid),
}

pub enum RuntimeMessagesDiscord {
    CallDiscordApplicationCommands(Uuid, DiscordRegistrationsResultApplicationCommands),
    CallDiscordEvent(Uuid, DiscordEvents),
}

pub enum JobSchedulerMessages {
    AddJob(Uuid, String, OSSender<Result<Uuid>>),
    RemoveJob(Uuid, OSSender<Result<()>>),
}

pub enum DiscordMessages {
    RegisterApplicationCommands,
    Request(
        DiscordRequests,
        OSSender<Result<Option<DiscordResponses>, PluginError>>,
    ),
}

pub struct Channels {
    pub core: ChannelsCore,
    pub runtime: ChannelsRuntime,
    pub services: ChannelsServices,
}

pub struct ChannelsCore {
    pub post_start: UnboundedSender<CoreMessages>,
    pub shutdown: UnboundedSender<CoreMessages>,
    pub job_scheduler_tx: Option<UnboundedSender<JobSchedulerMessages>>,
    pub discord_tx: Option<UnboundedSender<DiscordMessages>>,
    pub runtime_tx: UnboundedSender<RuntimeMessages>,
    pub rx: UnboundedReceiver<CoreMessages>,
}

pub struct ChannelsRuntime {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<RuntimeMessages>,
}

pub struct ChannelsServices {
    pub job_scheduler: Option<ChannelsJobScheduler>,
    pub discord: Option<ChannelsDiscord>,
}

pub struct ChannelsJobScheduler {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<JobSchedulerMessages>,
}

pub struct ChannelsDiscord {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<DiscordMessages>,
}

pub fn new(job_scheduler_enabled: bool, discord_enabled: bool) -> Channels {
    let (core_tx, core_rx) = unbounded_channel::<CoreMessages>();

    let (runtime_tx, runtime_rx) = unbounded_channel::<RuntimeMessages>();

    let (job_scheduler_tx, job_scheduler_channels) = if job_scheduler_enabled {
        let mpsc = unbounded_channel::<JobSchedulerMessages>();

        (
            Some(mpsc.0),
            Some(ChannelsJobScheduler {
                core_tx: core_tx.clone(),
                rx: mpsc.1,
            }),
        )
    } else {
        (None, None)
    };

    let (discord_tx, discord_channels) = if discord_enabled {
        let mpsc = unbounded_channel::<DiscordMessages>();

        (
            Some(mpsc.0),
            Some(ChannelsDiscord {
                core_tx: core_tx.clone(),
                rx: mpsc.1,
            }),
        )
    } else {
        (None, None)
    };

    Channels {
        core: ChannelsCore {
            post_start: core_tx.clone(),
            shutdown: core_tx.clone(),
            job_scheduler_tx,
            discord_tx,
            runtime_tx,
            rx: core_rx,
        },
        runtime: ChannelsRuntime {
            core_tx,
            rx: runtime_rx,
        },
        services: ChannelsServices {
            job_scheduler: job_scheduler_channels,
            discord: discord_channels,
        },
    }
}
