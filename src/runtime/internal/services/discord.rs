/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;
use uuid::Uuid;

use crate::{
    config::plugins::permissions::{
        PluginPermissions,
        services::discord::{PluginPermissionsDiscordEvents, PluginPermissionsDiscordInteractions},
    },
    database::Keyspaces,
    runtime::{
        internal::InternalRuntime,
        plugins::wpbs::plugin::{
            core_import_types::Error,
            discord_export_types::Host as DiscordExportTypesHost,
            discord_import_functions::Host as DiscordImportFunctionsHost,
            discord_import_types::{
                DiscordRegistrations, DiscordRegistrationsResult,
                DiscordRegistrationsResultInteractions, DiscordRequests, DiscordResponses,
                Host as DiscordImportTypesHost, SupportedDiscordRegistrations,
            },
        },
    },
    utils::channels::{CoreMessages, DatabaseMessages, DiscordMessages},
};

impl DiscordImportTypesHost for InternalRuntime {}
impl DiscordExportTypesHost for InternalRuntime {}

impl DiscordImportFunctionsHost for InternalRuntime {
    async fn get_supported_discord_registrations(&mut self) -> SupportedDiscordRegistrations {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        SupportedDiscordRegistrations {
            events: plugin_permissions.services.discord.events.into(),
            interactions: plugin_permissions.services.discord.interactions.into(),
        }
    }

    async fn discord_register(
        &mut self,
        registrations: DiscordRegistrations,
    ) -> DiscordRegistrationsResult {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        let mut result = DiscordRegistrationsResult {
            events: None,
            interactions: None,
        };

        if let Some(registered_events_flags) = registrations.events {
            let registered_events: Vec<PluginPermissionsDiscordEvents> =
                registered_events_flags.into();

            for registered_event in registered_events {
                if !plugin_permissions
                    .services
                    .discord
                    .events
                    .contains(&registered_event)
                {
                    result.events = Some(Err(format!(
                        "Plugin is not allowed to register for the {registered_event:?} event"
                    )));
                    break;
                }
            }

            result.events = Some(Ok(()));
        }

        if let Some(interactions) = registrations.interactions {
            result.interactions = Some(DiscordRegistrationsResultInteractions {
                application_commands: None,
                message_components: None,
                modals: None,
            });

            if let Some(application_commands) = interactions.application_commands {
                if !plugin_permissions
                    .services
                    .discord
                    .interactions
                    .contains(&PluginPermissionsDiscordInteractions::ApplicationCommands)
                {
                    result.interactions.as_mut().unwrap().application_commands = Some(Err(
                        Error::from(
                            "Plugin is not allowed to register the application commands interaction",
                        ),
                    ));
                } else {
                    result.interactions.as_mut().unwrap().application_commands = Some(Ok(()));
                }

                for (count, application_command) in application_commands.into_iter().enumerate() {
                    let (sender, receiver) = channel();

                    self.core_tx
                        .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                            Keyspaces::DiscordApplicationCommands,
                            format!("{}:{count}", self.plugin_id).as_bytes().to_vec(),
                            application_command,
                            sender,
                        )))
                        .unwrap();

                    receiver.await.unwrap().unwrap();
                }
            }

            if let Some(message_component_count) = interactions.message_components {
                if !plugin_permissions
                    .services
                    .discord
                    .interactions
                    .contains(&PluginPermissionsDiscordInteractions::MessageComponents)
                {
                    result.interactions.as_mut().unwrap().message_components =
                        Some(Err(Error::from(
                            "Plugin is not allowed to register the message component interaction",
                        )));
                } else {
                    result.interactions.as_mut().unwrap().message_components = Some(Ok(vec![]));
                }

                for _ in 0..message_component_count {
                    let uuid = Uuid::new_v4();

                    let (sender, receiver) = channel();

                    self.core_tx
                        .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                            Keyspaces::DiscordMessageComponents,
                            uuid.as_bytes().to_vec(),
                            self.plugin_id.as_bytes().to_vec(),
                            sender,
                        )))
                        .unwrap();

                    receiver.await.unwrap().unwrap();

                    result
                        .interactions
                        .as_mut()
                        .unwrap()
                        .message_components
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .push(uuid.to_string());
                }
            }

            if let Some(modal_count) = interactions.modals {
                if !plugin_permissions
                    .services
                    .discord
                    .interactions
                    .contains(&PluginPermissionsDiscordInteractions::Modals)
                {
                    result.interactions.as_mut().unwrap().modals = Some(Err(Error::from(
                        "Plugin is not allowed to register the modal interaction",
                    )));
                } else {
                    result.interactions.as_mut().unwrap().modals = Some(Ok(vec![]));
                }

                for _ in 0..modal_count {
                    let uuid = Uuid::new_v4();

                    let (sender, receiver) = channel();

                    self.core_tx
                        .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                            Keyspaces::DiscordModals,
                            uuid.as_bytes().to_vec(),
                            self.plugin_id.as_bytes().to_vec(),
                            sender,
                        )))
                        .unwrap();

                    receiver.await.unwrap().unwrap();

                    result
                        .interactions
                        .as_mut()
                        .unwrap()
                        .modals
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .push(uuid.to_string());
                }
            }
        }

        result
    }

    async fn discord_request(
        &mut self,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, Error> {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::Discord(DiscordMessages::Request(
                request, sender,
            )))
            .unwrap();

        receiver.await.unwrap()
    }
}
