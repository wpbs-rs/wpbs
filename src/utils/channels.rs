/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use anyhow::Result;
use fjall::Slice;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot::Sender as OSSender,
};
use uuid::Uuid;

use crate::{
    Shutdown,
    database::Keyspaces,
    plugins::{
        PluginRegistrationRequestsApplicationCommand,
        discord_bot::plugin::{
            discord_export_types::DiscordEvents,
            discord_import_types::{DiscordRequests, DiscordResponses},
        },
    },
};

pub enum CoreMessages {
    DatabaseModule(DatabaseMessages),

    JobSchedulerModule(JobSchedulerMessages),
    DiscordBotClientModule(DiscordBotClientMessages),

    RuntimeModule(RuntimeMessages),

    Shutdown(Shutdown),
}

pub enum DatabaseMessages {
    GetState(Keyspaces, Vec<u8>, OSSender<Result<Option<Slice>>>),
    InsertState(Keyspaces, Vec<u8>, Vec<u8>, OSSender<Result<()>>),
    DeleteState(Keyspaces, Vec<u8>, OSSender<Result<()>>),
    ContainsKey(Keyspaces, Vec<u8>, OSSender<Result<bool>>),
}

pub enum JobSchedulerMessages {
    AddJob(Uuid, String, OSSender<Result<Uuid>>),
    RemoveJob(Uuid, OSSender<Result<()>>),
}

pub enum DiscordBotClientMessages {
    RegisterApplicationCommands(
        Vec<PluginRegistrationRequestsApplicationCommand>,
        OSSender<Result<(Vec<String>, Vec<String>)>>,
    ),
    Request(DiscordRequests, OSSender<Result<Option<DiscordResponses>>>),
}

pub enum RuntimeMessages {
    JobScheduler(RuntimeMessagesJobScheduler),
    Discord(RuntimeMessagesDiscord),
}

pub enum RuntimeMessagesJobScheduler {
    CallScheduledJob(Uuid, Uuid),
}

pub enum RuntimeMessagesDiscord {
    CallDiscordEvent(Uuid, DiscordEvents),
}

pub struct Channels {
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
