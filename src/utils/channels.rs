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
    UnloadPlugin(Uuid),
    Shutdown,
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
    Shutdown,
}

pub enum DiscordMessages {
    RegisterApplicationCommands,
    Request(
        DiscordRequests,
        OSSender<Result<Option<DiscordResponses>, PluginError>>,
    ),
    Shutdown,
}

pub struct Channels {
    pub core: ChannelsCore,
    pub job_scheduler: ChannelsJobScheduler,
    pub discord: ChannelsDiscord,
    pub runtime: ChannelsRuntime,
}

pub struct ChannelsCore {
    pub main: UnboundedSender<CoreMessages>,
    pub shutdown: UnboundedSender<CoreMessages>,
    pub job_scheduler_tx: UnboundedSender<JobSchedulerMessages>,
    pub discord_tx: UnboundedSender<DiscordMessages>,
    pub runtime_tx: UnboundedSender<RuntimeMessages>,
    pub rx: UnboundedReceiver<CoreMessages>,
}

pub struct ChannelsJobScheduler {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<JobSchedulerMessages>,
}

pub struct ChannelsDiscord {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<DiscordMessages>,
}

pub struct ChannelsRuntime {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<RuntimeMessages>,
}

pub fn new() -> Channels {
    let (core_tx, core_rx) = unbounded_channel::<CoreMessages>();
    let (job_scheduler_tx, job_scheduler_rx) = unbounded_channel::<JobSchedulerMessages>();
    let (discord_tx, discord_rx) = unbounded_channel::<DiscordMessages>();
    let (runtime_tx, runtime_rx) = unbounded_channel::<RuntimeMessages>();

    Channels {
        core: ChannelsCore {
            main: core_tx.clone(),
            shutdown: core_tx.clone(),
            job_scheduler_tx,
            discord_tx,
            runtime_tx,
            rx: core_rx,
        },
        job_scheduler: ChannelsJobScheduler {
            core_tx: core_tx.clone(),
            rx: job_scheduler_rx,
        },
        discord: ChannelsDiscord {
            core_tx: core_tx.clone(),
            rx: discord_rx,
        },
        runtime: ChannelsRuntime {
            core_tx,
            rx: runtime_rx,
        },
    }
}
