/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot::Sender as OSSender,
};
use tracing::debug;
use uuid::Uuid;

use crate::{
    Shutdown,
    runtime::plugins::bindings::services::{
        discord::wpbs_services::discord::discord_types::{
            DiscordEvents, DiscordRegistrationsResultApplicationCommands, DiscordRequests,
            DiscordResponses,
        },
        job_scheduler::wpbs_services::job_scheduler::job_scheduler_types::Cron,
    },
};

pub enum CoreMessages {
    Runtime(RuntimeMessages),

    Services(CoreMessagesServices),

    Shutdown(Shutdown),
}

pub enum RuntimeMessages {
    Services(RuntimeMessagesServices),
}

pub enum RuntimeMessagesServices {
    JobScheduler(RuntimeMessagesServicesJobScheduler),
    Discord(RuntimeMessagesServicesDiscord),
}

pub enum RuntimeMessagesServicesJobScheduler {
    CallScheduledJob(Uuid, Arc<Cron>),
}

pub enum RuntimeMessagesServicesDiscord {
    CallDiscordApplicationCommandsResult(Uuid, DiscordRegistrationsResultApplicationCommands),
    CallDiscordEvent(Uuid, DiscordEvents),
}

pub enum CoreMessagesServices {
    JobScheduler(JobSchedulerMessages),
    Discord(DiscordMessages),
}

pub enum JobSchedulerMessages {
    AddJob(Uuid, String, OSSender<Result<()>>),
    RemoveJob(Uuid, String, OSSender<Result<()>>),
}

pub enum DiscordMessages {
    RegisterApplicationCommands,
    Request(DiscordRequests, OSSender<Result<Option<DiscordResponses>>>),
}

pub struct Channels {
    pub core: ChannelsCore,
    pub runtime: ChannelsRuntime,
    pub services: ChannelsServices,
}

pub struct ChannelsCore {
    pub post_setup: UnboundedSender<CoreMessages>,
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
    debug!("Creating the channels");

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
            post_setup: core_tx.clone(),
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
