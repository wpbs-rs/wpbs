/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::discord_import_types::DiscordEventKinds;

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissionsDiscord {
    #[serde(default)]
    pub events: Vec<PluginPermissionsDiscordEvents>,
    #[serde(default)]
    pub interactions: Vec<PluginPermissionsDiscordInteractions>,
}

impl PluginPermissionsDiscord {
    pub fn calculate(&mut self) {
        if self.events.contains(&PluginPermissionsDiscordEvents::All) {
            self.events = vec![
                PluginPermissionsDiscordEvents::MessageCreate,
                PluginPermissionsDiscordEvents::InteractionCreate,
                PluginPermissionsDiscordEvents::ThreadCreate,
                PluginPermissionsDiscordEvents::ThreadDelete,
                PluginPermissionsDiscordEvents::ThreadListSync,
                PluginPermissionsDiscordEvents::ThreadMemberUpdate,
                PluginPermissionsDiscordEvents::ThreadMembersUpdate,
                PluginPermissionsDiscordEvents::ThreadUpdate,
            ];
        }

        if self
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::All)
        {
            self.interactions = vec![
                PluginPermissionsDiscordInteractions::ApplicationCommands,
                PluginPermissionsDiscordInteractions::MessageComponents,
                PluginPermissionsDiscordInteractions::Modals,
            ];
        }
    }
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

impl Display for DiscordEventKinds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscordEventKinds::MessageCreate => write!(f, "MESSAGE_CREATE"),
            DiscordEventKinds::InteractionCreate => write!(f, "INTERACTION_CREATE"),
            DiscordEventKinds::ThreadCreate => write!(f, "THREAD_CREATE"),
            DiscordEventKinds::ThreadDelete => write!(f, "THREAD_DELETE"),
            DiscordEventKinds::ThreadListSync => write!(f, "THREAD_LIST_SYNC"),
            DiscordEventKinds::ThreadMemberUpdate => write!(f, "THREAD_MEMBER_UPDATE"),
            DiscordEventKinds::ThreadMembersUpdate => write!(f, "THREAD_MEMBERS_UPDATE"),
            DiscordEventKinds::ThreadUpdate => write!(f, "THREAD_UPDATE"),
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
