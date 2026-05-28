/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::sync::Arc;

use twilight_gateway::MessageSender;
use twilight_http::{Client, request::Request, routing::Route};
use twilight_model::gateway::{
    OpCode,
    payload::outgoing::{
        RequestGuildMembers, UpdatePresence, UpdateVoiceState,
        request_guild_members::RequestGuildMembersInfo, update_presence::UpdatePresencePayload,
        update_voice_state::UpdateVoiceStateInfo,
    },
};

use crate::{
    runtime::plugins::wpbs::plugin::{
        core_types::HostError,
        discord_import_types::{Body, DiscordRequests, DiscordResponses},
    },
    services::discord::Discord,
};

impl Discord {
    #[allow(clippy::too_many_lines)]
    pub async fn request(
        http_client: Arc<Client>,
        shard_message_senders: Arc<Vec<MessageSender>>,
        request: DiscordRequests,
    ) -> Result<Option<DiscordResponses>, HostError> {
        let request = match request {
            // Shard message sender commands
            DiscordRequests::RequestGuildMembers((guild_id, body)) => {
                let guild_shard_message_sender =
                    Self::get_guild_shard_message_sender(&shard_message_senders, guild_id);

                let d = match sonic_rs::from_str::<RequestGuildMembersInfo>(&body) {
                    Ok(d) => d,
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while deserializing RequestGuildMembersInfo, error: {err}",
                        ));
                    }
                };

                let request_guild_members = RequestGuildMembers {
                    d,
                    op: OpCode::RequestGuildMembers,
                };

                guild_shard_message_sender
                    .command(&request_guild_members)
                    .unwrap();

                None
            }
            DiscordRequests::RequestSoundboardSounds(_guild_ids) => {
                return Err(HostError::from(
                    "RequestSoundboardSounds has not yet been implemented in Twilight.",
                ));
            }
            DiscordRequests::UpdateVoiceState((guild_id, body)) => {
                let guild_shard_message_sender =
                    Self::get_guild_shard_message_sender(&shard_message_senders, guild_id);

                let d = match sonic_rs::from_str::<UpdateVoiceStateInfo>(&body) {
                    Ok(d) => d,
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while deserializing UpdateVoiceStateInfo, error: {err}",
                        ));
                    }
                };

                let update_voice_state = UpdateVoiceState {
                    d,
                    op: OpCode::VoiceStateUpdate,
                };

                guild_shard_message_sender
                    .command(&update_voice_state)
                    .unwrap();

                None
            }
            DiscordRequests::UpdatePresence(body) => {
                let guild_shard_message_sender = shard_message_senders.first().unwrap();

                let d = match sonic_rs::from_str::<UpdatePresencePayload>(&body) {
                    Ok(d) => d,
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while deserializing UpdatePresencePayload, error: {err}",
                        ));
                    }
                };

                let update_presence = UpdatePresence {
                    d,
                    op: OpCode::PresenceUpdate,
                };

                guild_shard_message_sender
                    .command(&update_presence)
                    .unwrap();

                None
            }

            // HTTP requests
            DiscordRequests::AddThreadMember((channel_id, user_id)) => {
                match Request::builder(&Route::AddThreadMember {
                    channel_id,
                    user_id,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::CreateBan((guild_id, user_id, body)) => {
                match Request::builder(&Route::CreateBan { guild_id, user_id })
                    .body(body.into_bytes())
                    .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::CreateForumThread((channel_id, body)) => {
                let request_builder = Request::builder(&Route::CreateForumThread { channel_id });

                let request_builder = match body {
                    Body::Json(bytes) => request_builder.body(bytes.into_bytes()),
                    Body::Form(buffer) => match request_builder.multipart(buffer) {
                        Ok(request) => request,
                        Err(err) => {
                            return Err(format!(
                                "Something went wrong while building a Discord request, error: {err}"
                            ));
                        }
                    },
                };

                match request_builder.build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::CreateMessage((channel_id, body)) => {
                let request_builder = Request::builder(&Route::CreateMessage { channel_id });

                let request_builder = match body {
                    Body::Json(bytes) => request_builder.body(bytes.into_bytes()),
                    // TODO: Fix, Twilight implementation is broken at this moment
                    Body::Form(buffer) => match request_builder.multipart(buffer) {
                        Ok(request) => request,
                        Err(err) => {
                            return Err(format!(
                                "Something went wrong while building a Discord request, error: {err}"
                            ));
                        }
                    },
                };

                match request_builder.build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::CreateThread((channel_id, body)) => {
                match Request::builder(&Route::CreateThread { channel_id })
                    .body(body.into_bytes())
                    .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::CreateThreadFromMessage((channel_id, message_id, body)) => {
                match Request::builder(&Route::CreateThreadFromMessage {
                    channel_id,
                    message_id,
                })
                .body(body.into_bytes())
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::DeleteMessage((channel_id, message_id)) => {
                match Request::builder(&Route::DeleteMessage {
                    channel_id,
                    message_id,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetActiveThreads(guild_id) => {
                match Request::builder(&Route::GetActiveThreads { guild_id }).build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetChannel(channel_id) => {
                match Request::builder(&Route::GetChannel { channel_id }).build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetJoinedPrivateArchivedThreads((before, channel_id, limit)) => {
                match Request::builder(&Route::GetJoinedPrivateArchivedThreads {
                    before,
                    channel_id,
                    limit,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetPrivateArchivedThreads((before, channel_id, limit)) => {
                match Request::builder(&Route::GetPrivateArchivedThreads {
                    before: before.as_deref(),
                    channel_id,
                    limit,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetPublicArchivedThreads((before, channel_id, limit)) => {
                match Request::builder(&Route::GetPublicArchivedThreads {
                    before: before.as_deref(),
                    channel_id,
                    limit,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetThreadMember((channel_id, user_id)) => {
                match Request::builder(&Route::GetThreadMember {
                    channel_id,
                    user_id,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::GetThreadMembers((after, channel_id, limit, with_member)) => {
                match Request::builder(&Route::GetThreadMembers {
                    after,
                    channel_id,
                    limit,
                    with_member,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::InteractionCallback((
                interaction_id,
                interaction_token,
                with_response,
                body,
            )) => {
                match Request::builder(&Route::InteractionCallback {
                    interaction_id,
                    interaction_token: &interaction_token,
                    with_response,
                })
                .body(body.into_bytes())
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::JoinThread(channel_id) => {
                match Request::builder(&Route::JoinThread { channel_id }).build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::LeaveThread(channel_id) => {
                match Request::builder(&Route::LeaveThread { channel_id }).build() {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::RemoveThreadMember((channel_id, user_id)) => {
                match Request::builder(&Route::RemoveThreadMember {
                    channel_id,
                    user_id,
                })
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::UpdateMember((guild_id, user_id, body)) => {
                match Request::builder(&Route::UpdateMember { guild_id, user_id })
                    .body(body.into_bytes())
                    .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
            DiscordRequests::UpdateInteractionOriginal((
                application_id,
                interaction_token,
                body,
            )) => {
                match Request::builder(&Route::UpdateInteractionOriginal {
                    application_id,
                    interaction_token: &interaction_token,
                })
                .body(body.into_bytes())
                .build()
                {
                    Ok(request) => Some(request),
                    Err(err) => {
                        return Err(format!(
                            "Something went wrong while building a Discord request, error: {err}"
                        ));
                    }
                }
            }
        };

        if let Some(request) = request {
            match http_client.request::<Vec<u8>>(request).await {
                Ok(response) => match response.text().await {
                    Ok(response_string) => Ok(Some(response_string)),
                    Err(err) => Err(format!(
                        "Something went wrong while deserializing the Discord response, error: {err}"
                    )),
                },
                Err(err) => Err(format!(
                    "Something went wrong while making the Discord request, error: {err}"
                )),
            }
        } else {
            Ok(None)
        }
    }

    fn get_guild_shard_message_sender(
        shard_message_senders: &Arc<Vec<MessageSender>>,
        guild_id: u64,
    ) -> &MessageSender {
        shard_message_senders
            .get((guild_id >> 22) as usize % shard_message_senders.len())
            .unwrap()
    }
}
