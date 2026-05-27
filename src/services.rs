/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use anyhow::Result;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    SHUTDOWN, TASKS,
    config::services::ConfigServices,
    services::{discord::Discord, job_scheduler::JobScheduler},
    utils::{
        channels::{ChannelsServices, CoreMessages, DiscordMessages},
        env::SecretsServices,
    },
};

pub mod discord;
pub mod job_scheduler;

pub async fn start(
    config: ConfigServices,
    secrets: SecretsServices,
    channels: ChannelsServices,
) -> Result<()> {
    if let Some(job_scheduler_channels) = channels.job_scheduler {
        let job_scheduler =
            JobScheduler::new(job_scheduler_channels.core_tx, job_scheduler_channels.rx);

        if SHUTDOWN.read().await.is_none() {
            TASKS.write().await.services.job_scheduler = Some(job_scheduler.run());
        } else {
            drop(job_scheduler);
        }
    }

    if let Some(discord_channels) = channels.discord {
        let discord = Discord::new(
            config.discord.settings,
            secrets.discord.unwrap(),
            discord_channels.core_tx,
            discord_channels.rx,
        )
        .await?;

        if SHUTDOWN.read().await.is_none() {
            TASKS.write().await.services.discord = Some(discord.run());
        } else {
            drop(discord);
        }
    }

    Ok(())
}

pub async fn post_start(core_tx: &UnboundedSender<CoreMessages>) {
    if TASKS.read().await.services.discord.is_some() {
        let _ = core_tx.send(CoreMessages::Discord(
            DiscordMessages::RegisterApplicationCommands,
        ));
    }
}
