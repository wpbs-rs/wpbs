/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::discord_import_types::DiscordEventKinds;

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissionsDiscord {
    #[serde(default)]
    pub events: Vec<PluginPermissionsDiscordEvents>,
    #[serde(default)]
    pub interactions: Vec<PluginPermissionsDiscordInteractions>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsDiscordEvents {
    MessageCreate,
    InteractionCreate,
    ThreadCreate,
    ThreadDelete,
    ThreadListSync,
    ThreadMemberUpdate,
    ThreadMembersUpdate,
    ThreadUpdate,
}

impl From<DiscordEventKinds> for Vec<PluginPermissionsDiscordEvents> {
    fn from(requested_discord_registrations: DiscordEventKinds) -> Self {
        let mut plugin_permissions_discord_events = Vec::new();

        if requested_discord_registrations.contains(DiscordEventKinds::MESSAGE_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::MessageCreate);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::INTERACTION_CREATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::InteractionCreate);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadCreate);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_DELETE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadDelete);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_LIST_SYNC) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadListSync);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_MEMBER_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMemberUpdate);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_MEMBERS_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMembersUpdate);
        }

        if requested_discord_registrations.contains(DiscordEventKinds::THREAD_UPDATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadUpdate);
        }

        plugin_permissions_discord_events
    }
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsDiscordInteractions {
    ApplicationCommands,
    MessageComponents,
    Modals,
}
