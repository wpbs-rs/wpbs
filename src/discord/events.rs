/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error};
use twilight_gateway::Event;
use twilight_model::application::interaction::InteractionData;

use crate::{
    discord::DiscordBotClient,
    plugins::discord_bot::plugin::discord_export_types::DiscordEvents,
    utils::channels::{CoreMessages, RuntimeMessages, RuntimeMessagesDiscord},
};

impl DiscordBotClient {
    #[allow(clippy::too_many_lines)]
    pub async fn handle_event(core_tx: Arc<UnboundedSender<CoreMessages>>, event: Event) {
        match event {
            Event::InteractionCreate(interaction_create) => {
                match interaction_create.data.as_ref() {
                    Some(InteractionData::ApplicationCommand(command_data)) => {
                        // TODO: get value

                        let _ = core_tx
                            .send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    plugin_uuid,
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_vec(&interaction_create).unwrap(),
                                    ),
                                ),
                            )))
                            .await;
                    }
                    Some(InteractionData::MessageComponent(message_component_interaction_data)) => {
                        let _ = discord_bot_client
                            .runtime_tx
                            .send(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    plugin.clone(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_vec(&interaction_create).unwrap(),
                                    ),
                                ),
                            ))
                            .await;
                    }
                    Some(InteractionData::ModalSubmit(modal_interaction_data)) => {
                        let initialized_plugins =
                            discord_bot_client.plugin_registrations.read().await;

                        let Some(plugin) = initialized_plugins
                            .discord_events
                            .interaction_create
                            .modals
                            .get(&modal_interaction_data.custom_id)
                        else {
                            return;
                        };

                        let _ = discord_bot_client
                            .runtime_tx
                            .send(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    plugin.clone(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_vec(&interaction_create).unwrap(),
                                    ),
                                ),
                            ))
                            .await;
                    }
                    None => error!("Interaction data is required."),
                    _ => error!(
                        "This interaction create data type does not have support yet, interaction data type: {:#?}",
                        &interaction_create.data.as_ref().unwrap().type_id()
                    ),
                }
            }
            Event::MessageCreate(message_create) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .message_create
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::MessageCreate(
                                    sonic_rs::to_vec(&message_create).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadCreate(thread_create) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_create
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadCreate(
                                    sonic_rs::to_vec(&thread_create).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadDelete(thread_delete) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_delete
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadDelete(
                                    sonic_rs::to_vec(&thread_delete).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadListSync(thread_list_sync) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_list_sync
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadListSync(
                                    sonic_rs::to_vec(&thread_list_sync).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadMemberUpdate(thread_member_update) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_member_update
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadMemberUpdate(
                                    sonic_rs::to_vec(&thread_member_update).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadMembersUpdate(thread_members_update) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_members_update
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadMembersUpdate(
                                    sonic_rs::to_vec(&thread_members_update).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            Event::ThreadUpdate(thread_update) => {
                for plugin in &discord_bot_client
                    .plugin_registrations
                    .read()
                    .await
                    .discord_events
                    .thread_update
                {
                    let _ = discord_bot_client
                        .runtime_tx
                        .send(RuntimeMessages::Discord(
                            RuntimeMessagesDiscord::CallDiscordEvent(
                                plugin.clone(),
                                DiscordEvents::ThreadUpdate(
                                    sonic_rs::to_vec(&thread_update).unwrap(),
                                ),
                            ),
                        ))
                        .await;
                }
            }
            _ => debug!(
                "Received an unhandled event: {}",
                &event.kind().name().unwrap_or("[No event kind name]")
            ),
        }
    }
}
