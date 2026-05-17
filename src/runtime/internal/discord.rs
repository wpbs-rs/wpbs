/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;

use crate::{
    config::plugins::permissions::{PluginPermissions, PluginPermissionsDiscordEvents},
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
    utils::channels::{CoreMessages, DatabaseMessages, DiscordBotClientMessages},
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
            )));

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        SupportedDiscordRegistrations {
            events: plugin_permissions.discord.events.into(),
            interactions: plugin_permissions.discord.interactions.into(),
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
            )));

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
            // TODO:
            // Check permissions

            if let Some(application_commands) = interactions.application_commands {
                // TODO:
                // Store in db
            }

            result.interactions = Some(Ok(DiscordRegistrationsResultInteractions {
                message_components: None,
                modals: None,
            }));

            if let Some(message_components) = interactions.message_components {
                // TODO:
                // Create UUID entry in db
                // Add UUID to result
            }

            if let Some(modals) = interactions.modals {
                // TODO:
                // Create UUID entry in db
                // Add UUID to result
            }
        }

        result
    }

    async fn discord_request(
        &mut self,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, Error> {
        let (sender, receiver) = channel();

        self.core_tx.send(CoreMessages::DiscordBotClientModule(
            DiscordBotClientMessages::Request(request, sender),
        ));

        receiver.await.unwrap()
    }
}
