/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{str::FromStr, sync::Arc};

use serde::Serialize;
use tokio::sync::{mpsc::UnboundedSender, oneshot::channel};
use tracing::{debug, error};
use twilight_gateway::Event;
use twilight_model::application::interaction::InteractionData;
use uuid::Uuid;

use crate::{
    database::Keyspaces,
    runtime::plugins::exports::wpbs::plugin::discord_export_functions::DiscordEvents,
    services::discord::DiscordBotClient,
    utils::channels::{CoreMessages, DatabaseMessages, RuntimeMessages, RuntimeMessagesDiscord},
};

impl DiscordBotClient {
    pub async fn handle_event(core_tx: Arc<UnboundedSender<CoreMessages>>, event: Event) {
        match event {
            Event::InteractionCreate(interaction_create) => {
                match interaction_create.data.as_ref() {
                    Some(InteractionData::ApplicationCommand(command_data)) => {
                        let (sender, receiver) = channel();

                        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                            Keyspaces::DiscordApplicationCommands,
                            sonic_rs::to_vec(&command_data.id.get()).unwrap(),
                            sender,
                        )));

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                Uuid::from_slice(&response_bytes).unwrap(),
                                DiscordEvents::InteractionCreate(
                                    sonic_rs::to_vec(&interaction_create).unwrap(),
                                ),
                            ),
                        )));
                    }
                    Some(InteractionData::MessageComponent(message_component_interaction_data)) => {
                        let (sender, receiver) = channel();

                        let Ok(message_component_id) =
                            Uuid::from_str(&message_component_interaction_data.custom_id)
                        else {
                            return;
                        };

                        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                            Keyspaces::DiscordMessageComponents,
                            message_component_id.as_bytes().to_vec(),
                            sender,
                        )));

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                Uuid::from_slice(&response_bytes).unwrap(),
                                DiscordEvents::InteractionCreate(
                                    sonic_rs::to_vec(&interaction_create).unwrap(),
                                ),
                            ),
                        )));
                    }
                    Some(InteractionData::ModalSubmit(modal_interaction_data)) => {
                        let (sender, receiver) = channel();

                        let Ok(modal_id) = Uuid::from_str(&modal_interaction_data.custom_id) else {
                            return;
                        };

                        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                            Keyspaces::DiscordMessageComponents,
                            modal_id.as_bytes().to_vec(),
                            sender,
                        )));

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                Uuid::from_slice(&response_bytes).unwrap(),
                                DiscordEvents::InteractionCreate(
                                    sonic_rs::to_vec(&interaction_create).unwrap(),
                                ),
                            ),
                        )));
                    }
                    _ => error!(
                        "Received unsupported interaction event: {}",
                        interaction_create.kind.kind()
                    ),
                }
            }
            Event::MessageCreate(message_create) => {
                Self::handle_basic_event(core_tx, "MESSAGE_CREATE", message_create);
            }
            Event::ThreadCreate(thread_create) => {
                Self::handle_basic_event(core_tx, "THREAD_CREATE", thread_create);
            }
            Event::ThreadDelete(thread_delete) => {
                Self::handle_basic_event(core_tx, "THREAD_DELETE", thread_delete);
            }
            Event::ThreadMemberUpdate(thread_member_update) => {
                Self::handle_basic_event(core_tx, "THREAD_MEMBER_UPDATE", thread_member_update);
            }
            Event::ThreadMembersUpdate(thread_members_update) => {
                Self::handle_basic_event(core_tx, "THREAD_MEMBERS_UPDATE", thread_members_update);
            }
            Event::ThreadUpdate(thread_update) => {
                Self::handle_basic_event(core_tx, "THREAD_UPDATE", thread_update);
            }
            _ => debug!(
                "Received unsupported event: {}",
                &event.kind().name().unwrap_or("[No event kind name]")
            ),
        }
    }

    pub async fn handle_basic_event<D>(
        core_tx: Arc<UnboundedSender<CoreMessages>>,
        key: &str,
        data: D,
    ) where
        D: Serialize,
    {
        let (sender, receiver) = channel();

        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
            Keyspaces::DiscordEvents,
            key.as_bytes().to_vec(),
            sender,
        )));

        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
            return;
        };

        let plugin_ids_bytes = sonic_rs::from_slice::<Vec<Vec<u8>>>(&response_bytes).unwrap();

        for plugin_id_bytes in plugin_ids_bytes {
            let plugin_id = Uuid::from_slice(&plugin_id_bytes).unwrap();

            core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                RuntimeMessagesDiscord::CallDiscordEvent(
                    plugin_id,
                    DiscordEvents::MessageCreate(sonic_rs::to_vec(&data).unwrap()),
                ),
            )));
        }
    }
}
