/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::discord_import_types::{
    DiscordEventKinds, DiscordRequests,
};

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissionsDiscord {
    #[serde(default)]
    pub requests: Vec<PluginPermissionsDiscordRequests>,
    #[serde(default)]
    pub events: Vec<PluginPermissionsDiscordEvents>,
    #[serde(default)]
    pub interactions: Vec<PluginPermissionsDiscordInteractions>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum PluginPermissionsDiscordRequests {
    All,
    RequestGuildMembers,
    RequestSoundboardSounds,
    UpdateVoiceState,
    UpdatePresence,
    AddThreadMember,
    CreateBan,
    CreateForumThread,
    CreateMessage,
    CreateThread,
    CreateThreadFromMessage,
    DeleteMessage,
    GetActiveThreads,
    GetChannel,
    GetJoinedPrivateArchivedThreads,
    GetPrivateArchivedThreads,
    GetPublicArchivedThreads,
    GetThreadMember,
    GetThreadMembers,
    InteractionCallback,
    JoinThread,
    LeaveThread,
    RemoveThreadMember,
    UpdateMember,
    UpdateInteractionOriginal,
}

impl From<&DiscordRequests> for PluginPermissionsDiscordRequests {
    fn from(discord_requests: &DiscordRequests) -> Self {
        match discord_requests {
            DiscordRequests::RequestGuildMembers(_) => {
                PluginPermissionsDiscordRequests::RequestGuildMembers
            }
            DiscordRequests::RequestSoundboardSounds(_) => {
                PluginPermissionsDiscordRequests::RequestSoundboardSounds
            }
            DiscordRequests::UpdateVoiceState(_) => {
                PluginPermissionsDiscordRequests::UpdateVoiceState
            }
            DiscordRequests::UpdatePresence(_) => PluginPermissionsDiscordRequests::UpdatePresence,
            DiscordRequests::AddThreadMember(_) => {
                PluginPermissionsDiscordRequests::AddThreadMember
            }
            DiscordRequests::CreateBan(_) => PluginPermissionsDiscordRequests::CreateBan,
            DiscordRequests::CreateForumThread(_) => {
                PluginPermissionsDiscordRequests::CreateForumThread
            }
            DiscordRequests::CreateMessage(_) => PluginPermissionsDiscordRequests::CreateMessage,
            DiscordRequests::CreateThread(_) => PluginPermissionsDiscordRequests::CreateThread,
            DiscordRequests::CreateThreadFromMessage(_) => {
                PluginPermissionsDiscordRequests::CreateThreadFromMessage
            }
            DiscordRequests::DeleteMessage(_) => PluginPermissionsDiscordRequests::DeleteMessage,
            DiscordRequests::GetActiveThreads(_) => {
                PluginPermissionsDiscordRequests::GetActiveThreads
            }
            DiscordRequests::GetChannel(_) => PluginPermissionsDiscordRequests::GetChannel,
            DiscordRequests::GetJoinedPrivateArchivedThreads(_) => {
                PluginPermissionsDiscordRequests::GetJoinedPrivateArchivedThreads
            }
            DiscordRequests::GetPrivateArchivedThreads(_) => {
                PluginPermissionsDiscordRequests::GetPrivateArchivedThreads
            }
            DiscordRequests::GetPublicArchivedThreads(_) => {
                PluginPermissionsDiscordRequests::GetPublicArchivedThreads
            }
            DiscordRequests::GetThreadMember(_) => {
                PluginPermissionsDiscordRequests::GetThreadMember
            }
            DiscordRequests::GetThreadMembers(_) => {
                PluginPermissionsDiscordRequests::GetThreadMembers
            }
            DiscordRequests::InteractionCallback(_) => {
                PluginPermissionsDiscordRequests::InteractionCallback
            }
            DiscordRequests::JoinThread(_) => PluginPermissionsDiscordRequests::JoinThread,
            DiscordRequests::LeaveThread(_) => PluginPermissionsDiscordRequests::LeaveThread,
            DiscordRequests::RemoveThreadMember(_) => {
                PluginPermissionsDiscordRequests::RemoveThreadMember
            }
            DiscordRequests::UpdateMember(_) => PluginPermissionsDiscordRequests::UpdateMember,
            DiscordRequests::UpdateInteractionOriginal(_) => {
                PluginPermissionsDiscordRequests::UpdateInteractionOriginal
            }
        }
    }
}

impl PluginPermissionsDiscord {
    pub fn calculate(&mut self) {
        if self
            .requests
            .contains(&PluginPermissionsDiscordRequests::All)
        {
            self.requests = vec![
                PluginPermissionsDiscordRequests::RequestGuildMembers,
                PluginPermissionsDiscordRequests::RequestSoundboardSounds,
                PluginPermissionsDiscordRequests::UpdateVoiceState,
                PluginPermissionsDiscordRequests::UpdatePresence,
                PluginPermissionsDiscordRequests::AddThreadMember,
                PluginPermissionsDiscordRequests::CreateBan,
                PluginPermissionsDiscordRequests::CreateForumThread,
                PluginPermissionsDiscordRequests::CreateMessage,
                PluginPermissionsDiscordRequests::CreateThread,
                PluginPermissionsDiscordRequests::CreateThreadFromMessage,
                PluginPermissionsDiscordRequests::DeleteMessage,
                PluginPermissionsDiscordRequests::GetActiveThreads,
                PluginPermissionsDiscordRequests::GetChannel,
                PluginPermissionsDiscordRequests::GetJoinedPrivateArchivedThreads,
                PluginPermissionsDiscordRequests::GetPrivateArchivedThreads,
                PluginPermissionsDiscordRequests::GetPublicArchivedThreads,
                PluginPermissionsDiscordRequests::GetThreadMember,
                PluginPermissionsDiscordRequests::GetThreadMembers,
                PluginPermissionsDiscordRequests::InteractionCallback,
                PluginPermissionsDiscordRequests::JoinThread,
                PluginPermissionsDiscordRequests::LeaveThread,
                PluginPermissionsDiscordRequests::RemoveThreadMember,
                PluginPermissionsDiscordRequests::UpdateMember,
                PluginPermissionsDiscordRequests::UpdateInteractionOriginal,
            ];
        }

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
    fn from(discord_event_kind: DiscordEventKinds) -> Self {
        match discord_event_kind {
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
