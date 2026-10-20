/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, sync::Arc};

use fjall::{Database, KeyspaceCreateOptions};
use tokio::sync::oneshot::channel;
use uuid::Uuid;

use crate::{
    TASKS,
    config::plugins::permissions::services::discord::PluginPermissionsDiscordInteractions,
    runtime::{
        internal::InternalRuntime,
        plugins::{
            RuntimePluginMetadata,
            bindings::services::discord::{
                wpbs::shared::shared_types::{Host as SharedTypesHost, HostError},
                wpbs_services::discord::{
                    discord_import_functions::Host as DiscordImportFunctionsHost,
                    discord_types::{
                        DiscordDeregistrations, DiscordDeregistrationsInteractionsResult,
                        DiscordDeregistrationsResult, DiscordEventKinds, DiscordRegistrations,
                        DiscordRegistrationsInteractionsResult, DiscordRegistrationsResult,
                        DiscordRequests, DiscordResponses, Host as DiscordTypesHost,
                    },
                },
            },
        },
    },
    utils::channels::{CoreMessages, CoreMessagesServices, DiscordMessages},
};

type DiscordEventRegistrationsResult =
    Result<Vec<(DiscordEventKinds, Result<(), HostError>)>, HostError>;

impl SharedTypesHost for InternalRuntime {}

impl DiscordTypesHost for InternalRuntime {}

impl DiscordImportFunctionsHost for InternalRuntime {
    async fn register(
        &mut self,
        registrations: DiscordRegistrations,
    ) -> Result<DiscordRegistrationsResult, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let event_registrations_result = registrations
            .events
            .map(|er| Self::register_discord_events(&self.database, &self.metadata, er));

        let interaction_registrations_result = if let Some(interaction_registrations) =
            registrations.interactions
        {
            let application_command_registrations_result =
                interaction_registrations.application_commands.map(|acr| {
                    Self::register_discord_application_commands(&self.database, &self.metadata, acr)
                });

            let message_component_registrations_result =
                interaction_registrations.message_components.map(|mcr| {
                    Self::register_discord_message_components(&self.database, &self.metadata, mcr)
                });

            let modal_registrations_result = interaction_registrations
                .modals
                .map(|mr| Self::register_discord_modals(&self.database, &self.metadata, mr));

            Some(DiscordRegistrationsInteractionsResult {
                application_commands: application_command_registrations_result,
                message_components: message_component_registrations_result,
                modals: modal_registrations_result,
            })
        } else {
            None
        };

        Ok(DiscordRegistrationsResult {
            events: event_registrations_result,
            interactions: interaction_registrations_result,
        })
    }

    async fn deregister(
        &mut self,
        deregistrations: DiscordDeregistrations,
    ) -> Result<DiscordDeregistrationsResult, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let event_deregistrations_result = deregistrations
            .events
            .map(|edr| Self::deregister_discord_events(&self.database, &self.metadata, edr));

        let interaction_deregistrations_result =
            if let Some(interaction_deregistrations) = deregistrations.interactions {
                let application_command_deregistrations_result = interaction_deregistrations
                    .application_commands
                    .map(|acdr| {
                        Self::deregister_discord_application_commands(
                            &self.database,
                            &self.metadata,
                            acdr,
                        )
                    });

                let message_component_deregistrations_result =
                    interaction_deregistrations.message_components.map(|mcdr| {
                        Self::deregister_discord_message_components(
                            &self.database,
                            &self.metadata,
                            mcdr,
                        )
                    });

                let modal_deregistrations_result = interaction_deregistrations.modals.map(|mdr| {
                    Self::deregister_discord_modals(&self.database, &self.metadata, mdr)
                });

                Some(DiscordDeregistrationsInteractionsResult {
                    application_commands: application_command_deregistrations_result,
                    message_components: message_component_deregistrations_result,
                    modals: modal_deregistrations_result,
                })
            } else {
                None
            };

        Ok(DiscordDeregistrationsResult {
            events: event_deregistrations_result,
            interactions: interaction_deregistrations_result,
        })
    }

    async fn get_discord_message_component_id(
        &mut self,
        name: String,
    ) -> Result<Option<String>, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let message_components_keyspace = self
            .database
            .keyspace("discord_message_components", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        let key = format!("{}:{name}", self.metadata.plugin_uuid);

        Ok(message_components_keyspace
            .get(&key)
            .map_err(|err| err.to_string())?
            .map(|v| Uuid::from_slice(&v).unwrap().to_string()))
    }

    async fn get_discord_modal_id(&mut self, name: String) -> Result<Option<String>, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let modals_keyspace = self
            .database
            .keyspace("discord_modals", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        let key = format!("{}:{name}", self.metadata.plugin_uuid);

        Ok(modals_keyspace
            .get(&key)
            .map_err(|err| err.to_string())?
            .map(|v| Uuid::from_slice(&v).unwrap().to_string()))
    }

    async fn discord_request(
        &mut self,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let (sender, receiver) = channel();

        if !self
            .metadata
            .permissions
            .services
            .discord
            .requests
            .contains(&(&request).into())
        {
            return Err(HostError::from(
                "Plugin does not have the permission to make this Discord request",
            ));
        }

        self.core_tx
            .send(CoreMessages::Services(CoreMessagesServices::Discord(
                DiscordMessages::Request(request, sender),
            )))
            .unwrap();

        receiver.await.unwrap().map_err(|err| err.to_string())
    }
}

impl InternalRuntime {
    fn register_discord_events(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        event_registrations: Vec<DiscordEventKinds>,
    ) -> DiscordEventRegistrationsResult {
        let mut event_registrations_result = Vec::new();

        let events_keyspace = database
            .keyspace("discord_events", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for event_registration in event_registrations {
            if !plugin_metadata
                .permissions
                .services
                .discord
                .events
                .contains(&event_registration.into())
            {
                event_registrations_result.push((
                    event_registration,
                    Err(HostError::from(
                        "Plugin does not have the permission to register for this event",
                    )),
                ));
                continue;
            }

            let key = format!(
                "{}:{}",
                event_registration.as_str(),
                plugin_metadata.plugin_uuid
            );

            if let Err(err) = events_keyspace.insert(&key, plugin_metadata.plugin_uuid.as_bytes()) {
                event_registrations_result.push((event_registration, Err(err.to_string())));
                continue;
            }

            event_registrations_result.push((event_registration, Ok(())));
        }

        Ok(event_registrations_result)
    }

    fn deregister_discord_events(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        event_deregistrations: Vec<DiscordEventKinds>,
    ) -> DiscordEventRegistrationsResult {
        let mut event_deregistrations_result = Vec::new();

        let events_keyspace = database
            .keyspace("discord_events", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for event_deregistration in event_deregistrations {
            if !plugin_metadata
                .permissions
                .services
                .discord
                .events
                .contains(&event_deregistration.into())
            {
                event_deregistrations_result.push((
                    event_deregistration,
                    Err(HostError::from(
                        "Plugin does not have the permission to deregister for this event",
                    )),
                ));
                continue;
            }

            let key = format!(
                "{}:{}",
                event_deregistration.as_str(),
                plugin_metadata.plugin_uuid
            );

            if let Err(err) = events_keyspace.remove(&key) {
                event_deregistrations_result.push((event_deregistration, Err(err.to_string())));
                continue;
            }

            event_deregistrations_result.push((event_deregistration, Ok(())));
        }

        Ok(event_deregistrations_result)
    }

    fn register_discord_application_commands(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        application_command_registrations: Vec<Vec<u8>>,
    ) -> Result<(), HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::ApplicationCommands)
        {
            return Err(HostError::from(
                "Plugin does not have the permission to register application command interactions",
            ));
        }

        let application_commands_keyspace = database
            .keyspace(
                "discord_application_commands",
                KeyspaceCreateOptions::default,
            )
            .map_err(|err| err.to_string())?;

        for application_command_registration in application_command_registrations {
            let application_command_uuid = Uuid::new_v4();

            let key = format!(
                "{}:{}",
                plugin_metadata.plugin_uuid, application_command_uuid
            );

            application_commands_keyspace
                .insert(&key, application_command_registration)
                .map_err(|err| err.to_string())?;
        }

        Ok(())
    }

    fn register_discord_message_components(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        message_component_registrations: Vec<String>,
    ) -> Result<HashMap<String, String>, HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::MessageComponents)
        {
            return Err(HostError::from(
                "Plugin does not have the permission to register message component interactions",
            ));
        }

        let mut message_component_registrations_result = HashMap::new();

        let message_components_keyspace = database
            .keyspace("discord_message_components", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for message_component_registration in message_component_registrations {
            let message_component_uuid = Uuid::new_v4();

            message_components_keyspace
                .insert(
                    message_component_uuid.as_bytes(),
                    plugin_metadata.plugin_uuid.as_bytes(),
                )
                .map_err(|err| err.to_string())?;

            let key = format!(
                "{}:{}",
                plugin_metadata.plugin_uuid, message_component_registration
            );

            message_components_keyspace
                .insert(&key, message_component_uuid.as_bytes())
                .map_err(|err| err.to_string())?;

            message_component_registrations_result.insert(
                message_component_registration,
                message_component_uuid.to_string(),
            );
        }

        Ok(message_component_registrations_result)
    }

    fn register_discord_modals(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        modal_registrations: Vec<String>,
    ) -> Result<HashMap<String, String>, HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::Modals)
        {
            return Err(HostError::from(
                "Plugin does not have the permission to register modal interactions",
            ));
        }

        let mut modal_registrations_result = HashMap::new();

        let modals_keyspace = database
            .keyspace("discord_modals", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for modal_registration in modal_registrations {
            let modal_uuid = Uuid::new_v4();

            modals_keyspace
                .insert(
                    modal_uuid.as_bytes(),
                    plugin_metadata.plugin_uuid.as_bytes(),
                )
                .map_err(|err| err.to_string())?;

            let key = format!("{}:{}", plugin_metadata.plugin_uuid, modal_registration);

            modals_keyspace
                .insert(&key, modal_uuid.as_bytes())
                .map_err(|err| err.to_string())?;

            modal_registrations_result.insert(modal_registration, modal_uuid.to_string());
        }

        Ok(modal_registrations_result)
    }
}
