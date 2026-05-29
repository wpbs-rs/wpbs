/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use anyhow::Result;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use tracing::{error, info};
//use twilight_cache_inmemory::{DefaultInMemoryCache, InMemoryCache};
use twilight_gateway::{CloseFrame, Config, EventType, Intents, MessageSender, Shard, StreamExt};
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
    shard_message_senders: Arc<Vec<MessageSender>>,
    //cache: Arc<InMemoryCache>,
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
    ) -> Result<Self> {
        info!("Creating the Discord service");

        let intents = InternalIntents::from(config.intents).0;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let http_client = Client::new(secrets.bot_token.clone());

        let config = Config::new(secrets.bot_token, intents);

        let shard_iterator =
            twilight_gateway::create_recommended(&http_client, config, |_, builder| {
                builder.build()
            })
            .await?;

        let (shards, shard_message_senders) =
            Self::get_shard_message_senders(Box::new(shard_iterator), intents);

        // TODO: Use the in memory cache
        //let cache = Arc::new(DefaultInMemoryCache::default());

        Ok(Self {
            http_client: Arc::new(http_client),
            shards,
            shard_message_senders: Arc::new(shard_message_senders),
            //cache,
            core_tx: Arc::new(core_tx),
            rx,
        })
    }

    #[hotpath::measure]
    pub fn run(mut self) -> JoinHandle<()> {
        let mut shard_tasks = Vec::with_capacity(self.shards.len());
        let http_task_tracker = TaskTracker::new();

        for shard in self.shards.drain(..) {
            shard_tasks.push(tokio::spawn(Self::shard_runner(
                shard.0,
                shard.1,
                //self.cache.clone(),
                self.core_tx.clone(),
            )));
        }

        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    DiscordMessages::RegisterApplicationCommands => {
                        http_task_tracker.spawn(Self::application_command_registrations(
                            self.http_client.clone(),
                            self.core_tx.clone(),
                        ));
                    }
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

            self.shutdown(shard_tasks).await;
        })
    }

    async fn shard_runner(
        mut shard: Shard,
        intents: Intents,
        //cache: Arc<InMemoryCache>,
        core_tx: Arc<UnboundedSender<CoreMessages>>,
    ) {
        while let Some(item) = shard.next_event(intents.into()).await {
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

            //cache.update(&event);

            tokio::spawn(Self::handle_event(core_tx.clone(), event));
        }
    }

    fn get_shard_message_senders(
        shard_iterator: Box<dyn ExactSizeIterator<Item = Shard>>,
        intents: Intents,
    ) -> (Vec<(Shard, Intents)>, Vec<MessageSender>) {
        let mut shards = Vec::new();
        let mut shard_message_senders = Vec::new();

        for shard in shard_iterator {
            shard_message_senders.push(shard.sender());
            shards.push((shard, intents));
        }

        (shards, shard_message_senders)
    }

    async fn shutdown(&self, tasks: Vec<JoinHandle<()>>) {
        for shard_message_sender in self.shard_message_senders.iter() {
            _ = shard_message_sender.close(CloseFrame::NORMAL);
        }

        for task in tasks {
            task.await.unwrap();
        }
    }
}
