/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap as StdHashMap, str::FromStr, sync::Arc};

use anyhow::Result;
use fjall::Slice;
use hashbrown::{Equivalent, HashMap};
use tokio::sync::{mpsc::UnboundedSender, oneshot::channel};
use tracing::{error, info};
use twilight_http::{Client, request::Request, routing::Route};
use twilight_model::{
    application::command::Command,
    id::{
        Id,
        marker::{ApplicationMarker, CommandMarker, GuildMarker},
    },
};
use uuid::Uuid;

use crate::{
    database::Keyspaces,
    services::discord::Discord,
    utils::channels::{CoreMessages, DatabaseMessages, RuntimeMessages, RuntimeMessagesDiscord},
};

#[derive(Eq, Hash, PartialEq)]
struct CommandKey {
    name: String,
    guild_id: Option<Id<GuildMarker>>,
}

impl Equivalent<CommandKey> for (&str, Option<Id<GuildMarker>>) {
    fn equivalent(&self, key: &CommandKey) -> bool {
        self.0 == key.name && self.1 == key.guild_id
    }
}

impl Discord {
    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    pub async fn application_command_registrations(
        http_client: Arc<Client>,
        core_tx: Arc<UnboundedSender<CoreMessages>>,
    ) {
        info!("Managing Discord application command registrations");

        let (entries_sender, entries_receiver) = channel();

        core_tx
            .send(CoreMessages::DatabaseModule(
                DatabaseMessages::GetAllEntries(
                    Keyspaces::DiscordApplicationCommands,
                    entries_sender,
                ),
            ))
            .unwrap();

        let entries: Vec<(Slice, Slice)> = entries_receiver.await.unwrap().unwrap();

        let (clear_sender, clear_receiver) = channel();

        core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Clear(
                Keyspaces::DiscordApplicationCommands,
                clear_sender,
            )))
            .unwrap();

        clear_receiver.await.unwrap().unwrap();

        let mut discord_commands = HashMap::new();

        let mut commands = HashMap::new();

        let mut results = HashMap::new();

        for (key, value) in entries {
            let key_string = String::from_utf8(key.to_vec()).unwrap();
            let (plugin_uuid_str, _) = key_string.split_once(':').unwrap();

            let plugin_uuid = Uuid::from_str(plugin_uuid_str).unwrap();

            match sonic_rs::from_slice::<Command>(&value) {
                Ok(command_data) => commands
                    .entry(CommandKey {
                        name: command_data.name.clone(),
                        guild_id: command_data.guild_id,
                    })
                    .or_insert(Vec::new())
                    .push((plugin_uuid, command_data)),
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing a command from the {plugin_uuid} plugin requested to register, error: {err}"
                    );
                }
            }
        }

        let application_id = match http_client.current_user_application().await {
            Ok(response) => match response.model().await {
                Ok(application) => application.id,
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the application data, error: {}",
                        &err
                    );
                    return;
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting the application data, error: {}",
                    &err
                );
                return;
            }
        };

        let global_discord_commands_request = match Request::builder(&Route::GetGlobalCommands {
            application_id: application_id.get(),
            with_localizations: Some(true),
        })
        .build()
        {
            Ok(global_discord_commands_request) => global_discord_commands_request,
            Err(err) => {
                error!(
                    "Failed to build the get global commands request, error: {}",
                    &err
                );
                return;
            }
        };

        // TODO: Continue logic checking and fixing from here (review: 5./6.)
        match http_client
            .request::<Vec<Command>>(global_discord_commands_request)
            .await
        {
            Ok(response) => match response.model().await {
                Ok(global_discord_commands) => {
                    for global_discord_command in global_discord_commands {
                        discord_commands.insert(
                            CommandKey {
                                name: global_discord_command.name,
                                guild_id: None,
                            },
                            global_discord_command.id.unwrap(),
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the global application commands, error: {}",
                        &err
                    );
                    return;
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting the global application commands, error: {}",
                    &err
                );
                return;
            }
        }

        // TODO: Rework to a gateway based retrieval system (using `GuildCreate`).
        let mut current_user_guilds = Vec::new();

        let mut request = http_client.current_user_guilds();
        loop {
            match request.await {
                Ok(response) => match response.model().await {
                    Ok(mut user_guilds) => {
                        if user_guilds.is_empty() {
                            break;
                        }

                        request = http_client
                            .current_user_guilds()
                            .after(user_guilds.last().unwrap().id);

                        current_user_guilds.append(&mut user_guilds);
                    }
                    Err(err) => {
                        error!(
                            "Something went wrong while deserializing the guild application commands, error: {}",
                            &err
                        );
                        return;
                    }
                },
                Err(err) => {
                    error!(
                        "Something went wrong while requesting the guild application commands, error: {}",
                        &err
                    );
                    return;
                }
            }
        }

        for current_user_guild in current_user_guilds {
            let guild_commands_request = match Request::builder(&Route::GetGuildCommands {
                application_id: application_id.get(),
                guild_id: current_user_guild.id.get(),
                with_localizations: Some(true),
            })
            .build()
            {
                Ok(guild_commands_request) => guild_commands_request,
                Err(err) => {
                    error!(
                        "Failed to build the get guild commands request, error: {}",
                        &err
                    );
                    continue;
                }
            };

            match http_client
                .request::<Vec<Command>>(guild_commands_request)
                .await
            {
                Ok(response) => match response.model().await {
                    Ok(single_guild_discord_commands) => {
                        for single_guild_discord_command in single_guild_discord_commands {
                            discord_commands.insert(
                                CommandKey {
                                    name: single_guild_discord_command.name,
                                    guild_id: single_guild_discord_command.guild_id,
                                },
                                single_guild_discord_command.id.unwrap(),
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            "Something went wrong while deserializing the guild application commands, error: {}",
                            &err
                        );
                    }
                },
                Err(err) => {
                    error!(
                        "Something went wrong while requesting the guild application commands, error: {}",
                        &err
                    );
                }
            }
        }

        for mut commands_by_name in commands {
            if commands_by_name.1.len() == 1 {
                let command = commands_by_name.1.remove(0);

                let plugin_results = results.entry(command.0).or_insert(StdHashMap::new());

                if let Ok(command_id) = Self::register_application_command(
                    http_client.clone(),
                    application_id,
                    &mut discord_commands,
                    &command.1,
                )
                .await
                {
                    let (sender, receiver) = channel();

                    core_tx
                        .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                            Keyspaces::DiscordApplicationCommands,
                            command_id.to_string().into_bytes(),
                            command.0.as_bytes().to_vec(),
                            sender,
                        )))
                        .unwrap();

                    receiver.await.unwrap().unwrap();

                    plugin_results.insert(command.1.name, Ok(command_id.get()));
                } else {
                    let err = format!(
                        "Failed to register the {} command from the {} plugin",
                        command.1.name, command.0
                    );

                    error!("{err}");

                    plugin_results.insert(command.1.name, Err(err));
                }
            } else {
                for (index, mut command) in commands_by_name.1.into_iter().enumerate() {
                    command.1.name += format!("-{}", index + 1).as_str();

                    let plugin_results = results.entry(command.0).or_default();

                    if let Ok(command_id) = Self::register_application_command(
                        http_client.clone(),
                        application_id,
                        &mut discord_commands,
                        &command.1,
                    )
                    .await
                    {
                        let (sender, receiver) = channel();

                        core_tx
                            .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                Keyspaces::DiscordApplicationCommands,
                                command_id.to_string().into_bytes(),
                                command.0.as_bytes().to_vec(),
                                sender,
                            )))
                            .unwrap();

                        receiver.await.unwrap().unwrap();

                        plugin_results.insert(command.1.name, Ok(command_id.get()));
                    } else {
                        let err = format!(
                            "Failed to register the {} command from the {} plugin",
                            command.1.name, command.0
                        );

                        error!("{err}");

                        plugin_results.insert(command.1.name, Err(err));
                    }
                }
            }
        }

        Self::delete_old_application_commands(http_client, application_id, &discord_commands).await;

        for result in results {
            let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                RuntimeMessagesDiscord::CallDiscordApplicationCommands(result.0, result.1),
            )));
        }
    }

    async fn register_application_command(
        http_client: Arc<Client>,
        application_id: Id<ApplicationMarker>,
        discord_commands: &mut HashMap<CommandKey, Id<CommandMarker>>,
        command: &Command,
    ) -> Result<Id<CommandMarker>> {
        let request = if let Some(discord_command_id) =
            discord_commands.remove(&(command.name.as_str(), command.guild_id))
        {
            let route = if let Some(guild_id) = command.guild_id {
                Route::UpdateGuildCommand {
                    application_id: application_id.get(),
                    command_id: discord_command_id.get(),
                    guild_id: guild_id.get(),
                }
            } else {
                Route::UpdateGlobalCommand {
                    application_id: application_id.get(),
                    command_id: discord_command_id.get(),
                }
            };

            Request::builder(&route)
                .body(sonic_rs::to_vec(command).unwrap())
                .build()?
        } else {
            let route = if let Some(guild_id) = command.guild_id {
                Route::CreateGuildCommand {
                    application_id: application_id.get(),
                    guild_id: guild_id.get(),
                }
            } else {
                Route::CreateGlobalCommand {
                    application_id: application_id.get(),
                }
            };

            Request::builder(&route)
                .body(sonic_rs::to_vec(command).unwrap())
                .build()?
        };

        let command = http_client
            .request::<Command>(request)
            .await?
            .model()
            .await?;

        Ok(command.id.unwrap())
    }

    async fn delete_old_application_commands(
        http_client: Arc<Client>,
        application_id: Id<ApplicationMarker>,
        discord_commands: &HashMap<CommandKey, Id<CommandMarker>>,
    ) {
        for (discord_command_key, discord_command_id) in discord_commands {
            let route = match discord_command_key.guild_id {
                Some(guild_id) => Route::DeleteGuildCommand {
                    application_id: application_id.get(),
                    command_id: discord_command_id.get(),
                    guild_id: guild_id.get(),
                },
                None => Route::DeleteGlobalCommand {
                    application_id: application_id.get(),
                    command_id: discord_command_id.get(),
                },
            };

            let request = match Request::builder(&route).build() {
                Ok(request) => request,
                Err(err) => {
                    error!(
                        "Failed to build the create global command request, error: {}",
                        &err
                    );
                    continue;
                }
            };

            info!(
                "Deleting the {} command, guild id: {:?}",
                &discord_command_key.name, &discord_command_key.guild_id
            );
            match http_client.request::<()>(request).await {
                Ok(_) => (),
                Err(err) => {
                    error!(
                        "Something went wrong while requesting a command deletion, error: {}",
                        &err
                    );
                }
            }
        }
    }
}
