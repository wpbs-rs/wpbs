/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{error, info};
use twilight_cache_inmemory::{DefaultInMemoryCache, InMemoryCache};
use twilight_gateway::{
    CloseFrame, Config, EventType, EventTypeFlags, Intents, MessageSender, Shard, StreamExt,
};
use twilight_http::Client;

use crate::{
    SHUTDOWN,
    utils::channels::{CoreMessages, DiscordBotClientMessages},
};

mod events;
mod interactions;
mod requests;

pub struct DiscordBotClient {
    http_client: Arc<Client>,
    shards: Vec<Shard>,
    shard_message_senders: Arc<Vec<MessageSender>>,
    cache: Arc<InMemoryCache>,
    core_tx: Arc<UnboundedSender<CoreMessages>>,
    rx: UnboundedReceiver<DiscordBotClientMessages>,
}

impl DiscordBotClient {
    pub async fn new(
        token: String,
        core_tx: UnboundedSender<CoreMessages>,
        rx: UnboundedReceiver<DiscordBotClientMessages>,
    ) -> Result<Self, ()> {
        info!("Creating the Discord bot client");

        let intents = Intents::all(); // TODO: Make this configurable

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap();

        let http_client = Client::new(token.clone());

        let config = Config::new(token, intents);

        let (shards, shard_message_senders) = match twilight_gateway::create_recommended(
            &http_client,
            config,
            |_, builder| builder.build(),
        )
        .await
        {
            Ok(shard_iterator) => Self::shard_message_senders(Box::new(shard_iterator)),
            Err(err) => {
                error!(
                    "Something went wrong while getting the recommended amount of shards from Discord, error: {}",
                    &err
                );
                return Err(());
            }
        };

        let cache = Arc::new(DefaultInMemoryCache::default()); // TODO: Make this configurable

        Ok(DiscordBotClient {
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
                    DiscordBotClientMessages::RegisterApplicationCommands(
                        commands,
                        response_sender,
                    ) => {
                        let http_client = self.http_client.clone();
                        tokio::spawn(async {
                            response_sender.send(
                                Self::application_command_registrations(http_client, commands)
                                    .await,
                            );
                        });
                    }
                    DiscordBotClientMessages::Request(request, response_sender) => {
                        let http_client = self.http_client.clone();
                        let shard_message_senders = self.shard_message_senders.clone();

                        tokio::spawn(async {
                            response_sender.send(
                                Self::request(http_client, shard_message_senders, request).await,
                            );
                        });
                    }
                }
            }

            self.shutdown(tasks);
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

    async fn shutdown(&self, mut tasks: Vec<JoinHandle<()>>) {
        for shard_message_sender in self.shard_message_senders.iter() {
            _ = shard_message_sender.close(CloseFrame::NORMAL);
        }

        for task in tasks.drain(..) {
            let _ = task.await;
        }
    }
}
