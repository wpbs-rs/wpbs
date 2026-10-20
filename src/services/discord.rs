/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{iter, sync::Arc};

use anyhow::Result;
use fjall::{Database, KeyspaceCreateOptions};
use songbird::{Songbird, shards::TwilightMap as ShardMessageSenders};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::{self, JoinHandle},
};
use tokio_util::task::TaskTracker;
use tracing::{error, info};
use twilight_cache_inmemory::{InMemoryCache, ResourceType};
use twilight_gateway::{CloseFrame, Config, EventType, EventTypeFlags, Intents, Shard, StreamExt};
use twilight_http::Client;

use crate::{
    SHUTDOWN,
    config::services::discord::{ConfigDiscordSettings, InternalIntents},
    utils::{
        channels::{CoreMessages, DiscordMessages},
        env::SecretsDiscord,
    },
};

mod events;
mod interactions;
mod requests;

pub struct Discord {
    http_client: Arc<Client>,
    shards: Vec<(Shard, Intents)>,
    shard_message_senders: Arc<ShardMessageSenders>,
    // TODO: Implement:
    // - Voice Channel specific gateway events
    // - Plugin endpoints to play and listen to audio in voice channels
    #[allow(unused)]
    songbird: Songbird,
    cache: Arc<InMemoryCache>,
    database: Database,
    core_tx: Arc<UnboundedSender<CoreMessages>>,
    rx: UnboundedReceiver<DiscordMessages>,
}

// SAFETY: `Shard` contains a Tokio websocket stream which is not send and sync. This is safe as
// all shards have been moved out of the struct at the moment it is send or shared across threads.
unsafe impl Send for Discord {}
unsafe impl Sync for Discord {}

impl Discord {
    pub async fn new(
        config: ConfigDiscordSettings,
        secrets: SecretsDiscord,
        core_tx: UnboundedSender<CoreMessages>,
        rx: UnboundedReceiver<DiscordMessages>,
        database: Database,
    ) -> Result<Self> {
        info!("Creating the Discord service");

        database
            .keyspace("discord_events", KeyspaceCreateOptions::default)?
            .clear()?;
        database
            .keyspace(
                "discord_application_commands",
                KeyspaceCreateOptions::default,
            )?
            .clear()?;

        let intents = InternalIntents::from(config.intents).0;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let http_client = Client::new(secrets.bot_token.clone());

        let config = Config::new(secrets.bot_token, intents);

        let connection_info = http_client.gateway().authed().await?.model().await?;

        let shards = twilight_gateway::bucket(0, 1, connection_info.shards)
            .zip(iter::repeat_n(
                config.clone(),
                connection_info.shards as usize,
            ))
            .map(|(shard_id, config)| Shard::with_config(shard_id, config))
            .map(|shard| (shard, intents))
            .collect::<Vec<_>>();

        let shard_message_senders = Arc::new(ShardMessageSenders::new(
            shards
                .iter()
                .map(|s| (s.0.id().number(), s.0.sender()))
                .collect(),
        ));

        let current_user = http_client.current_user().await?.model().await?;

        let songbird = Songbird::twilight(shard_message_senders.clone(), current_user.id);

        let cache_resource_type = ResourceType::USER_CURRENT | ResourceType::GUILD;

        let cache = Arc::new(
            InMemoryCache::builder()
                .resource_types(cache_resource_type)
                .message_cache_size(0)
                .build(),
        );

        Ok(Self {
            http_client: Arc::new(http_client),
            shards,
            shard_message_senders,
            songbird,
            cache,
            database,
            core_tx: Arc::new(core_tx),
            rx,
        })
    }

    #[hotpath::measure]
    pub fn run(mut self) -> JoinHandle<()> {
        info!("Starting the Discord service");

        let shard_task_tracker = TaskTracker::new();
        let http_task_tracker = TaskTracker::new();

        for shard in self.shards.drain(..) {
            shard_task_tracker.spawn(Self::shard_runner(
                self.database.clone(),
                self.cache.clone(),
                self.core_tx.clone(),
                shard.0,
                shard.1,
            ));
        }

        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    DiscordMessages::RegisterApplicationCommands => {
                        http_task_tracker.spawn(Self::register_application_commands(
                            self.database.clone(),
                            self.http_client.clone(),
                            self.cache.clone(),
                            self.core_tx.clone(),
                        ));
                    }
                    DiscordMessages::DeregisterApplicationCommands(_) => todo!(),
                    DiscordMessages::Request(request, sender) => {
                        let http_client = self.http_client.clone();
                        let shard_message_senders = self.shard_message_senders.clone();

                        http_task_tracker.spawn(async {
                            sender
                                .send(
                                    Self::request(http_client, shard_message_senders, request)
                                        .await,
                                )
                                .unwrap();
                        });
                    }
                }
            }

            http_task_tracker.close();
            http_task_tracker.wait().await;

            self.shutdown(shard_task_tracker).await;
        })
    }

    async fn shard_runner(
        database: Database,
        cache: Arc<InMemoryCache>,
        core_tx: Arc<UnboundedSender<CoreMessages>>,
        mut shard: Shard,
        intents: Intents,
    ) {
        let event_type_flags = EventTypeFlags::BASE | intents.into();

        while let Some(item) = shard.next_event(event_type_flags).await {
            let Ok(event) = item else {
                error!(
                    "Something went wrong while receiving the next gateway event: {}",
                    item.as_ref().unwrap_err()
                );

                continue;
            };

            if event.kind() == EventType::GatewayClose && SHUTDOWN.read().await.is_some() {
                break;
            }

            cache.update(&event);

            let database = database.clone();
            let core_tx = core_tx.clone();
            task::spawn_blocking(move || Self::handle_event(&database, &core_tx, &event));
        }
    }

    async fn shutdown(&self, shard_task_tracker: TaskTracker) {
        info!("Shutting the Discord service down");

        for shard_id in 0..self.shard_message_senders.shard_count() {
            let _ = self
                .shard_message_senders
                .get(u32::try_from(shard_id).unwrap())
                .unwrap()
                .close(CloseFrame::NORMAL);
        }

        shard_task_tracker.close();
        shard_task_tracker.wait().await;
    }
}
