/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;

use crate::{
    TASKS,
    runtime::{
        internal::InternalRuntime,
        plugins::wpbs::plugin::{
            core_types::HostError,
            discord_export_types::Host as DiscordExportTypesHost,
            discord_import_functions::Host as DiscordImportFunctionsHost,
            discord_import_types::{
                DiscordRequests, DiscordResponses, Host as DiscordImportTypesHost,
            },
            discord_types::Host as DiscordTypesHost,
        },
    },
    utils::channels::{CoreMessages, DiscordMessages},
};

impl DiscordTypesHost for InternalRuntime {}
impl DiscordImportTypesHost for InternalRuntime {}
impl DiscordExportTypesHost for InternalRuntime {}

impl DiscordImportFunctionsHost for InternalRuntime {
    async fn discord_request(
        &mut self,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, HostError> {
        let (sender, receiver) = channel();

        if TASKS.read().await.services.discord.is_some() {
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
                .send(CoreMessages::Discord(DiscordMessages::Request(
                    request, sender,
                )))
                .unwrap();

            receiver.await.unwrap()
        } else {
            Err(HostError::from("The Discord service is disabled"))
        }
    }
}
