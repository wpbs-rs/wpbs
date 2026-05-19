/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use anyhow::{Result, bail};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{error, info};
use twilight_cache_inmemory::{DefaultInMemoryCache, InMemoryCache};
use twilight_gateway::{
    CloseFrame, Config, EventType, EventTypeFlags, MessageSender, Shard, StreamExt,
};
use twilight_http::Client;

use crate::{
    SHUTDOWN,
    config::services::discord::{ConfigDiscord, InternalIntents},
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
    shards: Vec<Shard>,
    shard_message_senders: Arc<Vec<MessageSender>>,
    cache: Arc<InMemoryCache>,
    core_tx: Arc<UnboundedSender<CoreMessages>>,
    rx: UnboundedReceiver<DiscordMessages>,
}

impl Discord {
    pub async fn new(
        config: ConfigDiscord,
        secrets: SecretsDiscord,
        core_tx: UnboundedSender<CoreMessages>,
        rx: UnboundedReceiver<DiscordMessages>,
    ) -> Result<Self> {
        info!("Creating the Discord service");

        let intents = InternalIntents::from(config.intents).0;

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap();

        let http_client = Client::new(secrets.bot_token.clone());

        let config = Config::new(secrets.bot_token, intents);

        let (shards, shard_message_senders) = match twilight_gateway::create_recommended(
            &http_client,
            config,
            |_, builder| builder.build(),
        )
        .await
        {
            Ok(shard_iterator) => Self::shard_message_senders(Box::new(shard_iterator)),
            Err(err) => {
                bail!(
                    "Something went wrong while getting the recommended amount of shards from Discord, error: {}",
                    &err
                );
            }
        };

        // TODO: Use the in memory cache
        let cache = Arc::new(DefaultInMemoryCache::default());

        Ok(Self {
            http_client: Arc::new(http_client),
            shards,
            shard_message_senders: Arc::new(shard_message_senders),
            cache,
            core_tx: Arc::new(core_tx),
            rx,
        })
    }

    pub fn start(mut self) -> JoinHandle<()> {
        let mut tasks = Vec::with_capacity(self.shards.len());

        for shard in self.shards.drain(..) {
            tasks.push(tokio::spawn(Self::shard_runner(
                self.cache.clone(),
                self.core_tx.clone(),
                shard,
            )));
        }

        tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    DiscordMessages::RegisterApplicationCommands => {
                        tokio::spawn(Self::application_command_registrations(
                            self.http_client.clone(),
                            self.core_tx.clone(),
                        ));
                    }
                    DiscordMessages::Request(request, response_sender) => {
                        let http_client = self.http_client.clone();
                        let shard_message_senders = self.shard_message_senders.clone();

                        tokio::spawn(async {
                            let _ = response_sender.send(
                                Self::request(http_client, shard_message_senders, request).await,
                            );
                        });
                    }
                    DiscordMessages::Shutdown => {
                        self.rx.close();
                    }
                }
            }

            Self::shutdown(self.shard_message_senders.clone(), tasks).await;
        })
    }

    async fn shard_runner(
        cache: Arc<InMemoryCache>,
        core_tx: Arc<UnboundedSender<CoreMessages>>,
        mut shard: Shard,
    ) {
        while let Some(item) = shard.next_event(EventTypeFlags::all()).await {
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

            tokio::spawn(Self::handle_event(core_tx.clone(), event));
        }
    }

    fn shard_message_senders(
        shard_iterator: Box<dyn ExactSizeIterator<Item = Shard>>,
    ) -> (Vec<Shard>, Vec<MessageSender>) {
        let mut shards = vec![];
        let mut shard_message_senders = vec![];

        for shard in shard_iterator {
            shard_message_senders.push(shard.sender());
            shards.push(shard);
        }

        (shards, shard_message_senders)
    }

    async fn shutdown(
        shard_message_senders: Arc<Vec<MessageSender>>,
        mut tasks: Vec<JoinHandle<()>>,
    ) {
        for shard_message_sender in shard_message_senders.iter() {
            _ = shard_message_sender.close(CloseFrame::NORMAL);
        }

        for task in tasks.drain(..) {
            let _ = task.await;
        }
    }
}
