/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::Deserialize;
use twilight_gateway::Intents;

pub struct InternalIntents(pub Intents);

#[derive(Default, Deserialize)]
pub struct ConfigDiscord {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub intents: Vec<ConfigDiscordIntents>,
}

#[derive(Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ConfigDiscordIntents {
    All,
    Guilds,
    GuildMembers,
    GuildModeration,
    GuildEmojisAndStickers,
    GuildIntegrations,
    GuildWebhooks,
    GuildInvites,
    GuildVoiceStates,
    GuildPresences,
    GuildMessages,
    GuildMessageReactions,
    GuildMessageTyping,
    DirectMessages,
    DirectMessageReactions,
    DirectMessageTyping,
    MessageContent,
    GuildScheduledEvents,
    AutoModerationConfiguration,
    AutoModerationExecution,
    GuildMessagePolls,
    DirectMessagePolls,
}

impl From<Vec<ConfigDiscordIntents>> for InternalIntents {
    fn from(values: Vec<ConfigDiscordIntents>) -> Self {
        if values.contains(&ConfigDiscordIntents::All) {
            return Self(Intents::all());
        }

        let mut result = Self(Intents::empty());

        for value in values {
            match value {
                ConfigDiscordIntents::All => unreachable!(),
                ConfigDiscordIntents::Guilds => {
                    result.0 |= Intents::GUILDS;
                }
                ConfigDiscordIntents::GuildMembers => {
                    result.0 |= Intents::GUILD_MEMBERS;
                }
                ConfigDiscordIntents::GuildModeration => {
                    result.0 |= Intents::GUILD_MODERATION;
                }
                ConfigDiscordIntents::GuildEmojisAndStickers => {
                    result.0 |= Intents::GUILD_EMOJIS_AND_STICKERS;
                }
                ConfigDiscordIntents::GuildIntegrations => {
                    result.0 |= Intents::GUILD_INTEGRATIONS;
                }
                ConfigDiscordIntents::GuildWebhooks => {
                    result.0 |= Intents::GUILD_WEBHOOKS;
                }
                ConfigDiscordIntents::GuildInvites => {
                    result.0 |= Intents::GUILD_INVITES;
                }
                ConfigDiscordIntents::GuildVoiceStates => {
                    result.0 |= Intents::GUILD_VOICE_STATES;
                }
                ConfigDiscordIntents::GuildPresences => {
                    result.0 |= Intents::GUILD_PRESENCES;
                }
                ConfigDiscordIntents::GuildMessages => {
                    result.0 |= Intents::GUILD_MESSAGES;
                }
                ConfigDiscordIntents::GuildMessageReactions => {
                    result.0 |= Intents::GUILD_MESSAGE_REACTIONS;
                }
                ConfigDiscordIntents::GuildMessageTyping => {
                    result.0 |= Intents::GUILD_MESSAGE_TYPING;
                }
                ConfigDiscordIntents::DirectMessages => {
                    result.0 |= Intents::DIRECT_MESSAGES;
                }
                ConfigDiscordIntents::DirectMessageReactions => {
                    result.0 |= Intents::DIRECT_MESSAGE_REACTIONS;
                }
                ConfigDiscordIntents::DirectMessageTyping => {
                    result.0 |= Intents::DIRECT_MESSAGE_TYPING;
                }
                ConfigDiscordIntents::MessageContent => {
                    result.0 |= Intents::MESSAGE_CONTENT;
                }
                ConfigDiscordIntents::GuildScheduledEvents => {
                    result.0 |= Intents::GUILD_SCHEDULED_EVENTS;
                }
                ConfigDiscordIntents::AutoModerationConfiguration => {
                    result.0 |= Intents::AUTO_MODERATION_CONFIGURATION;
                }
                ConfigDiscordIntents::AutoModerationExecution => {
                    result.0 |= Intents::AUTO_MODERATION_EXECUTION;
                }
                ConfigDiscordIntents::GuildMessagePolls => {
                    result.0 |= Intents::GUILD_MESSAGE_POLLS;
                }
                ConfigDiscordIntents::DirectMessagePolls => {
                    result.0 |= Intents::DIRECT_MESSAGE_POLLS;
                }
            }
        }

        result
    }
}
