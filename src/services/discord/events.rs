/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use fjall::{Database, KeyspaceCreateOptions};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error};
use twilight_gateway::Event;
use twilight_model::application::interaction::InteractionData;
use uuid::Uuid;

use crate::{
    runtime::plugins::wpbs::plugin::{
        discord_export_types::DiscordEvents, discord_import_types::DiscordEventKinds,
    },
    services::discord::Discord,
    utils::channels::{CoreMessages, RuntimeMessages, RuntimeMessagesDiscord},
};

impl Discord {
    // TODO:
    // - Split up in sub functions
    // - Rework to prevent unneeded deserialization
    #[allow(clippy::too_many_lines)]
    pub fn handle_event(
        database: &Database,
        core_tx: &Arc<UnboundedSender<CoreMessages>>,
        event: &Event,
    ) -> Result<()> {
        match event {
            Event::InteractionCreate(interaction_create) => {
                match interaction_create.data.as_ref() {
                    Some(InteractionData::ApplicationCommand(command_data)) => {
                        let application_command_keyspace = database.keyspace(
                            "discord_application_commands",
                            KeyspaceCreateOptions::default,
                        )?;

                        if let Some(plugin_id_bytes) =
                            application_command_keyspace.get(command_data.id.get().to_ne_bytes())?
                        {
                            let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&plugin_id_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )));
                        }
                    }
                    Some(InteractionData::MessageComponent(message_component_interaction_data)) => {
                        let Ok(message_component_id) =
                            Uuid::from_str(&message_component_interaction_data.custom_id)
                        else {
                            return Ok(());
                        };

                        let message_components_keyspace = database.keyspace(
                            "discord_message_components",
                            KeyspaceCreateOptions::default,
                        )?;

                        if let Some(plugin_id_bytes) =
                            message_components_keyspace.get(message_component_id.as_bytes())?
                        {
                            let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&plugin_id_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )));
                        }
                    }
                    Some(InteractionData::ModalSubmit(modal_interaction_data)) => {
                        let Ok(modal_id) = Uuid::from_str(&modal_interaction_data.custom_id) else {
                            return Ok(());
                        };

                        let modals_keyspace =
                            database.keyspace("discord_modals", KeyspaceCreateOptions::default)?;

                        if let Some(plugin_id_bytes) = modals_keyspace.get(modal_id.as_bytes())? {
                            let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                                RuntimeMessagesDiscord::CallDiscordEvent(
                                    Uuid::from_slice(&plugin_id_bytes).unwrap(),
                                    DiscordEvents::InteractionCreate(
                                        sonic_rs::to_string(&interaction_create).unwrap(),
                                    ),
                                ),
                            )));
                        }
                    }
                    _ => error!(
                        "Received unsupported interaction event: {}",
                        interaction_create.kind.kind()
                    ),
                }
            }
            Event::MessageCreate(message_create) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::MessageCreate,
                    &DiscordEvents::MessageCreate(sonic_rs::to_string(&message_create).unwrap()),
                )?;
            }
            Event::ThreadCreate(thread_create) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadCreate,
                    &DiscordEvents::ThreadCreate(sonic_rs::to_string(&thread_create).unwrap()),
                )?;
            }
            Event::ThreadDelete(thread_delete) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadDelete,
                    &DiscordEvents::ThreadDelete(sonic_rs::to_string(&thread_delete).unwrap()),
                )?;
            }
            Event::ThreadListSync(thread_list_sync) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadListSync,
                    &DiscordEvents::ThreadListSync(sonic_rs::to_string(&thread_list_sync).unwrap()),
                )?;
            }
            Event::ThreadMemberUpdate(thread_member_update) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadMemberUpdate,
                    &DiscordEvents::ThreadMemberUpdate(
                        sonic_rs::to_string(&thread_member_update).unwrap(),
                    ),
                )?;
            }
            Event::ThreadMembersUpdate(thread_members_update) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadMembersUpdate,
                    &DiscordEvents::ThreadMembersUpdate(
                        sonic_rs::to_string(&thread_members_update).unwrap(),
                    ),
                )?;
            }
            Event::ThreadUpdate(thread_update) => {
                Self::handle_basic_event(
                    database,
                    core_tx,
                    DiscordEventKinds::ThreadUpdate,
                    &DiscordEvents::ThreadUpdate(sonic_rs::to_string(&thread_update).unwrap()),
                )?;
            }
            _ => debug!("Received unsupported event: {:?}", event.kind()),
        }

        Ok(())
    }

    pub fn handle_basic_event(
        database: &Database,
        core_tx: &Arc<UnboundedSender<CoreMessages>>,
        key: DiscordEventKinds,
        event: &DiscordEvents,
    ) -> Result<()> {
        let events_keyspace =
            database.keyspace("discord_events", KeyspaceCreateOptions::default)?;

        let entries = events_keyspace.prefix(key.as_str());

        for entry in entries {
            let plugin_id = Uuid::from_slice(&entry.value().unwrap()).unwrap();

            let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                RuntimeMessagesDiscord::CallDiscordEvent(plugin_id, event.clone()),
            )));
        }

        Ok(())
    }
}
