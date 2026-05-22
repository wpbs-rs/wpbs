/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{str::FromStr, sync::Arc};

use tokio::sync::{mpsc::UnboundedSender, oneshot::channel};
use tracing::{debug, error};
use twilight_gateway::Event;
use twilight_model::application::interaction::InteractionData;
use uuid::Uuid;

use crate::{
    database::Keyspaces,
    runtime::plugins::wpbs::plugin::discord_export_types::DiscordEvents,
    services::discord::Discord,
    utils::channels::{CoreMessages, DatabaseMessages, RuntimeMessages, RuntimeMessagesDiscord},
};

impl Discord {
    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    pub async fn handle_event(core_tx: Arc<UnboundedSender<CoreMessages>>, event: Event) {
        match event {
            Event::InteractionCreate(interaction_create) => {
                match interaction_create.data.as_ref() {
                    Some(InteractionData::ApplicationCommand(command_data)) => {
                        let (sender, receiver) = channel();

                        core_tx
                            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                                Keyspaces::DiscordApplicationCommands,
                                command_data.id.to_string().into_bytes(),
                                sender,
                            )))
                            .unwrap();

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx
                            .send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&response_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )))
                            .unwrap();
                    }
                    Some(InteractionData::MessageComponent(message_component_interaction_data)) => {
                        let (sender, receiver) = channel();

                        let Ok(message_component_id) =
                            Uuid::from_str(&message_component_interaction_data.custom_id)
                        else {
                            return;
                        };

                        core_tx
                            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                                Keyspaces::DiscordMessageComponents,
                                message_component_id.as_bytes().to_vec(),
                                sender,
                            )))
                            .unwrap();

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx
                            .send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&response_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )))
                            .unwrap();
                    }
                    Some(InteractionData::ModalSubmit(modal_interaction_data)) => {
                        let (sender, receiver) = channel();

                        let Ok(modal_id) = Uuid::from_str(&modal_interaction_data.custom_id) else {
                            return;
                        };

                        core_tx
                            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                                Keyspaces::DiscordModals,
                                modal_id.as_bytes().to_vec(),
                                sender,
                            )))
                            .unwrap();

                        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
                            return;
                        };

                        core_tx
                            .send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&response_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )))
                            .unwrap();
                    }
                    _ => error!(
                        "Received unsupported interaction event: {}",
                        interaction_create.kind.kind()
                    ),
                }
            }
            Event::MessageCreate(message_create) => {
                Self::handle_basic_event(
                    core_tx,
                    "MESSAGE_CREATE",
                    DiscordEvents::MessageCreate(sonic_rs::to_string(&message_create).unwrap()),
                )
                .await;
            }
            Event::ThreadCreate(thread_create) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_CREATE",
                    DiscordEvents::ThreadCreate(sonic_rs::to_string(&thread_create).unwrap()),
                )
                .await;
            }
            Event::ThreadDelete(thread_delete) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_DELETE",
                    DiscordEvents::ThreadDelete(sonic_rs::to_string(&thread_delete).unwrap()),
                )
                .await;
            }
            Event::ThreadListSync(thread_list_sync) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_LIST_SYNC",
                    DiscordEvents::ThreadListSync(sonic_rs::to_string(&thread_list_sync).unwrap()),
                )
                .await;
            }
            Event::ThreadMemberUpdate(thread_member_update) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_MEMBER_UPDATE",
                    DiscordEvents::ThreadMemberUpdate(
                        sonic_rs::to_string(&thread_member_update).unwrap(),
                    ),
                )
                .await;
            }
            Event::ThreadMembersUpdate(thread_members_update) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_MEMBERS_UPDATE",
                    DiscordEvents::ThreadMembersUpdate(
                        sonic_rs::to_string(&thread_members_update).unwrap(),
                    ),
                )
                .await;
            }
            Event::ThreadUpdate(thread_update) => {
                Self::handle_basic_event(
                    core_tx,
                    "THREAD_UPDATE",
                    DiscordEvents::ThreadUpdate(sonic_rs::to_string(&thread_update).unwrap()),
                )
                .await;
            }
            _ => debug!(
                "Received unsupported event: {}",
                &event.kind().name().unwrap_or("[No event kind name]")
            ),
        }
    }

    pub async fn handle_basic_event(
        core_tx: Arc<UnboundedSender<CoreMessages>>,
        key: &str,
        event: DiscordEvents,
    ) {
        let (sender, receiver) = channel();

        core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::DiscordEvents,
                key.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
            return;
        };

        let plugin_ids_bytes = sonic_rs::from_slice::<Vec<Vec<u8>>>(&response_bytes).unwrap();

        for plugin_id_bytes in plugin_ids_bytes {
            let plugin_id = Uuid::from_slice(&plugin_id_bytes).unwrap();

            core_tx
                .send(CoreMessages::Runtime(RuntimeMessages::Discord(
                    RuntimeMessagesDiscord::CallDiscordEvent(plugin_id, event.clone()),
                )))
                .unwrap();
        }
    }
}
