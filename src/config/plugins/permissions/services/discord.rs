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
pub enum PluginPermissionsDiscordEvents {
    All,
    MessageCreate,
    InteractionCreate,
    ThreadCreate,
    ThreadDelete,
    ThreadListSync,
    ThreadMemberUpdate,
    ThreadMembersUpdate,
    ThreadUpdate,
}

impl From<DiscordEventKinds> for PluginPermissionsDiscordEvents {
    fn from(requested_discord_registration: DiscordEventKinds) -> Self {
        match requested_discord_registration {
            DiscordEventKinds::MessageCreate => PluginPermissionsDiscordEvents::MessageCreate,
            DiscordEventKinds::InteractionCreate => {
                PluginPermissionsDiscordEvents::InteractionCreate
            }
            DiscordEventKinds::ThreadCreate => PluginPermissionsDiscordEvents::ThreadCreate,
            DiscordEventKinds::ThreadDelete => PluginPermissionsDiscordEvents::ThreadDelete,
            DiscordEventKinds::ThreadListSync => PluginPermissionsDiscordEvents::ThreadListSync,
            DiscordEventKinds::ThreadMemberUpdate => {
                PluginPermissionsDiscordEvents::ThreadMemberUpdate
            }
            DiscordEventKinds::ThreadMembersUpdate => {
                PluginPermissionsDiscordEvents::ThreadMembersUpdate
            }
            DiscordEventKinds::ThreadUpdate => PluginPermissionsDiscordEvents::ThreadUpdate,
        }
    }
}

impl From<DiscordEventKinds> for Vec<u8> {
    fn from(requested_discord_registration: DiscordEventKinds) -> Self {
        match requested_discord_registration {
            DiscordEventKinds::MessageCreate => "MESSAGE_CREATE".as_bytes().to_vec(),
            DiscordEventKinds::InteractionCreate => "INTERACTION_CREATE".as_bytes().to_vec(),
            DiscordEventKinds::ThreadCreate => "THREAD-CREATE".as_bytes().to_vec(),
            DiscordEventKinds::ThreadDelete => "THREAD_DELETE".as_bytes().to_vec(),
            DiscordEventKinds::ThreadListSync => "THREAD_LIST_SYNC".as_bytes().to_vec(),
            DiscordEventKinds::ThreadMemberUpdate => "THREAD_MEMBER_UPDATE".as_bytes().to_vec(),
            DiscordEventKinds::ThreadMembersUpdate => "THREAD_MEMBERS_UPDATE".as_bytes().to_vec(),
            DiscordEventKinds::ThreadUpdate => "THREAD_UPDATE".as_bytes().to_vec(),
        }
    }
}

#[derive(Deserialize, PartialEq, Serialize)]
pub enum PluginPermissionsDiscordInteractions {
    All,
    ApplicationCommands,
    MessageComponents,
    Modals,
}
