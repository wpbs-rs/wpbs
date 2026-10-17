/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use anyhow::Result;
use fjall::Database;
use tokio::sync::{RwLock, mpsc::UnboundedSender};

use crate::{
    TASKS,
    config::services::ConfigServices,
    services::{discord::Discord, job_scheduler::JobScheduler},
    utils::{
        channels::{
            ChannelsServices, CoreMessages, CoreMessagesServices, DiscordMessages,
            JobSchedulerMessages,
        },
        env::SecretsServices,
    },
};

pub mod discord;
pub mod job_scheduler;

pub struct ServicesTx {
    pub job_scheduler: Arc<RwLock<Option<UnboundedSender<JobSchedulerMessages>>>>,
    pub discord: Arc<RwLock<Option<UnboundedSender<DiscordMessages>>>>,
}

pub async fn setup(
    config: ConfigServices,
    secrets: SecretsServices,
    channels: ChannelsServices,
    database: Database,
) -> Result<()> {
    // TODO:
    // - Make service starts concurrent
    // - Bail if all services are disabled

    if let Some(job_scheduler_channels) = channels.job_scheduler {
        let job_scheduler =
            JobScheduler::new(job_scheduler_channels.core_tx, job_scheduler_channels.rx);

        TASKS.write().await.services.job_scheduler = Some(job_scheduler.run());
    }

    if let Some(discord_channels) = channels.discord {
        let discord = Discord::new(
            config.discord.settings,
            secrets.discord.unwrap(),
            discord_channels.core_tx,
            discord_channels.rx,
            database,
        )
        .await?;

        TASKS.write().await.services.discord = Some(discord.run());
    }

    Ok(())
}

pub async fn post_setup(core_tx: &UnboundedSender<CoreMessages>) {
    if TASKS.read().await.services.discord.is_some() {
        let _ = core_tx.send(CoreMessages::Services(CoreMessagesServices::Discord(
            DiscordMessages::RegisterApplicationCommands,
        )));
    }
}

pub async fn message_handler(services_tx: Arc<ServicesTx>, message: CoreMessagesServices) {
    match message {
        CoreMessagesServices::JobScheduler(job_scheduler_message) => {
            if let Some(job_scheduler_tx) = services_tx.job_scheduler.read().await.as_ref() {
                job_scheduler_tx.send(job_scheduler_message).unwrap();
            }
        }
        CoreMessagesServices::Discord(discord_message) => {
            if let Some(discord_tx) = services_tx.discord.read().await.as_ref() {
                discord_tx.send(discord_message).unwrap();
            }
        }
    }
}
