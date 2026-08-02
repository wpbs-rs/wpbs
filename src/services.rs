/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use anyhow::Result;
use fjall::Database;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    TASKS,
    config::services::ConfigServices,
    services::{discord::Discord, job_scheduler::JobScheduler},
    utils::{
        channels::{ChannelsServices, CoreMessages, DiscordMessages},
        env::SecretsServices,
    },
};

pub mod discord;
pub mod job_scheduler;

pub async fn setup(
    config: ConfigServices,
    secrets: SecretsServices,
    database: Database,
    channels: ChannelsServices,
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
            database,
            discord_channels.core_tx,
            discord_channels.rx,
        )
        .await?;

        TASKS.write().await.services.discord = Some(discord.run());
    }

    Ok(())
}

pub async fn post_setup(core_tx: &UnboundedSender<CoreMessages>) {
    if TASKS.read().await.services.discord.is_some() {
        let _ = core_tx.send(CoreMessages::Discord(
            DiscordMessages::RegisterApplicationCommands,
        ));
    }
}
