/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::discord_import_types::{
    DiscordEvents, SupportedDiscordRegistrationsInteractions,
};

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

// The `DiscordEvents` flags is retyped several times in the plugin API.
impl From<Vec<PluginPermissionsDiscordEvents>> for DiscordEvents {
    fn from(plugin_permissions_discord_events: Vec<PluginPermissionsDiscordEvents>) -> Self {
        let mut supported_discord_registrations_events = Self::empty();

        for plugin_permission_discord_events in &plugin_permissions_discord_events {
            match plugin_permission_discord_events {
                PluginPermissionsDiscordEvents::MessageCreate => {
                    supported_discord_registrations_events |= Self::MESSAGE_CREATE;
                }
                PluginPermissionsDiscordEvents::InteractionCreate => {
                    supported_discord_registrations_events |= Self::INTERACTION_CREATE;
                }
                PluginPermissionsDiscordEvents::ThreadCreate => {
                    supported_discord_registrations_events |= Self::THREAD_CREATE;
                }
                PluginPermissionsDiscordEvents::ThreadDelete => {
                    supported_discord_registrations_events |= Self::THREAD_DELETE;
                }
                PluginPermissionsDiscordEvents::ThreadListSync => {
                    supported_discord_registrations_events |= Self::THREAD_LIST_SYNC;
                }
                PluginPermissionsDiscordEvents::ThreadMemberUpdate => {
                    supported_discord_registrations_events |= Self::THREAD_MEMBER_UPDATE;
                }
                PluginPermissionsDiscordEvents::ThreadMembersUpdate => {
                    supported_discord_registrations_events |= Self::THREAD_MEMBERS_UPDATE;
                }
                PluginPermissionsDiscordEvents::ThreadUpdate => {
                    supported_discord_registrations_events |= Self::THREAD_UPDATE;
                }
            }
        }

        supported_discord_registrations_events
    }
}

impl From<DiscordEvents> for Vec<PluginPermissionsDiscordEvents> {
    fn from(requested_discord_registrations: DiscordEvents) -> Self {
        let mut plugin_permissions_discord_events = Vec::new();

        if requested_discord_registrations.contains(DiscordEvents::MESSAGE_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::MessageCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::INTERACTION_CREATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::InteractionCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_DELETE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadDelete);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_LIST_SYNC) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadListSync);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_MEMBER_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMemberUpdate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_MEMBERS_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMembersUpdate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_UPDATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadUpdate);
        }

        plugin_permissions_discord_events
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsDiscordInteractions {
    ApplicationCommands,
    MessageComponents,
    Modals,
}

impl From<Vec<PluginPermissionsDiscordInteractions>> for SupportedDiscordRegistrationsInteractions {
    fn from(
        plugin_permissions_discord_interactions: Vec<PluginPermissionsDiscordInteractions>,
    ) -> Self {
        let mut supported_discord_registrations_interactions = Self::empty();

        for plugin_permission_discord_interactions in &plugin_permissions_discord_interactions {
            match plugin_permission_discord_interactions {
                PluginPermissionsDiscordInteractions::ApplicationCommands => {
                    supported_discord_registrations_interactions |= Self::APPLICATION_COMMANDS;
                }
                PluginPermissionsDiscordInteractions::MessageComponents => {
                    supported_discord_registrations_interactions |= Self::MESSAGE_COMPONENTS;
                }
                PluginPermissionsDiscordInteractions::Modals => {
                    supported_discord_registrations_interactions |= Self::MODALS;
                }
            }
        }

        supported_discord_registrations_interactions
    }
}
