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
        discord_export_types::{
            DiscordEvents, DiscordRegistrationsResultApplicationCommands, Error,
        },
        discord_import_functions::DiscordRequests,
        discord_import_types::DiscordResponses,
    },
};

pub enum CoreMessages {
    DatabaseModule(DatabaseMessages),

    JobScheduler(JobSchedulerMessages),
    DiscordBotClient(DiscordBotClientMessages),

    Runtime(RuntimeMessages),

    Shutdown(Shutdown),
}

pub enum DatabaseMessages {
    Get(Keyspaces, Vec<u8>, OSSender<Result<Option<Slice>>>),
    Range(Keyspaces, Vec<u8>, Vec<u8>, bool, OSSender<Result<Iter>>),
    Prefix(Keyspaces, Vec<u8>, OSSender<Result<Iter>>),
    GetAllEntries(Keyspaces, OSSender<Result<Vec<(Slice, Slice)>>>),
    GetAllKeys(Keyspaces, OSSender<Result<Vec<Slice>>>),
    GetAllValues(Keyspaces, OSSender<Result<Vec<Slice>>>),
    Insert(Keyspaces, Vec<u8>, Vec<u8>, OSSender<Result<()>>),
    Remove(Keyspaces, Vec<u8>, OSSender<Result<()>>),
    ContainsKey(Keyspaces, Vec<u8>, OSSender<Result<bool>>),
    Clear(Keyspaces, OSSender<Result<()>>),
}

pub enum JobSchedulerMessages {
    AddJob(Uuid, String, OSSender<Result<Uuid>>),
    RemoveJob(Uuid, OSSender<Result<()>>),
    Shutdown,
}

pub enum DiscordBotClientMessages {
    RegisterApplicationCommands,
    Request(
        DiscordRequests,
        OSSender<Result<Option<DiscordResponses>, Error>>,
    ),
    Shutdown,
}

pub enum RuntimeMessages {
    Core(RuntimeMessagesCore),
    JobScheduler(RuntimeMessagesJobScheduler),
    Discord(RuntimeMessagesDiscord),
}

pub enum RuntimeMessagesCore {
    CallDependencyFunction(Uuid, String, Vec<u8>, OSSender<Result<Vec<u8>, Error>>),
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

pub struct Channels {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub core: ChannelsCore,
    pub job_scheduler: ChannelsJobScheduler,
    pub discord_bot_client: ChannelsDiscordBotClient,
    pub runtime: ChannelsRuntime,
}

pub struct ChannelsCore {
    pub job_scheduler_tx: UnboundedSender<JobSchedulerMessages>,
    pub discord_bot_client_tx: UnboundedSender<DiscordBotClientMessages>,
    pub runtime_tx: UnboundedSender<RuntimeMessages>,
    pub rx: UnboundedReceiver<CoreMessages>,
}

pub struct ChannelsJobScheduler {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<JobSchedulerMessages>,
}

pub struct ChannelsDiscordBotClient {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<DiscordBotClientMessages>,
}

pub struct ChannelsRuntime {
    pub core_tx: UnboundedSender<CoreMessages>,
    pub rx: UnboundedReceiver<RuntimeMessages>,
}

pub fn new() -> Channels {
    let (core_tx, core_rx) = unbounded_channel::<CoreMessages>();
    let (job_scheduler_tx, job_scheduler_rx) = unbounded_channel::<JobSchedulerMessages>();
    let (discord_bot_client_tx, discord_bot_client_rx) =
        unbounded_channel::<DiscordBotClientMessages>();
    let (runtime_tx, runtime_rx) = unbounded_channel::<RuntimeMessages>();

    Channels {
        core_tx: core_tx.clone(),
        core: ChannelsCore {
            job_scheduler_tx,
            discord_bot_client_tx,
            runtime_tx,
            rx: core_rx,
        },
        job_scheduler: ChannelsJobScheduler {
            core_tx: core_tx.clone(),
            rx: job_scheduler_rx,
        },
        discord_bot_client: ChannelsDiscordBotClient {
            core_tx: core_tx.clone(),
            rx: discord_bot_client_rx,
        },
        runtime: ChannelsRuntime {
            core_tx,
            rx: runtime_rx,
        },
    }
}
