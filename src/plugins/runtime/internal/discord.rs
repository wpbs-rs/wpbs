/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use crate::plugins::{
    discord_bot::plugin::{
        core_import_types::Error,
        discord_export_types::Host as DiscordExportTypesHost,
        discord_import_functions::Host as DiscordImportFunctionsHost,
        discord_import_types::{
            DiscordRegistrations, DiscordRegistrationsResult, DiscordRequests, DiscordResponses,
            Host as DiscordImportTypesHost, SupportedDiscordRegistrations,
        },
    },
    runtime::internal::InternalRuntime,
};

impl DiscordImportTypesHost for InternalRuntime {}
impl DiscordExportTypesHost for InternalRuntime {}

impl DiscordImportFunctionsHost for InternalRuntime {
    async fn get_supported_discord_registrations(&mut self) -> SupportedDiscordRegistrations {
        todo!()
    }

    async fn discord_register(
        &mut self,
        registrations: DiscordRegistrations,
    ) -> DiscordRegistrationsResult {
        todo!()
    }

    async fn discord_request(
        &mut self,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, Error> {
        todo!()
    }
}
