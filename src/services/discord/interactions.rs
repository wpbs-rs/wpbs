/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::Result;
use fjall::Slice;
use tokio::sync::{mpsc::UnboundedSender, oneshot::channel};
use tracing::{error, info};
use twilight_http::{Client, request::Request, routing::Route};
use twilight_model::{
    application::command::Command,
    id::{
        Id,
        marker::{ApplicationMarker, CommandMarker},
    },
};
use uuid::Uuid;

use crate::{
    database::Keyspaces,
    services::discord::DiscordBotClient,
    utils::channels::{CoreMessages, DatabaseMessages, RuntimeMessages},
};

impl DiscordBotClient {
    pub async fn application_command_registrations(
        http_client: Arc<Client>,
        core_tx: Arc<UnboundedSender<CoreMessages>>,
    ) -> Result<(), ()> {
        let (entries_sender, entries_receiver) = channel();

        core_tx.send(CoreMessages::DatabaseModule(
            DatabaseMessages::GetAllEntries(Keyspaces::DiscordApplicationCommands, entries_sender),
        ));

        let entries: Vec<(Slice, Slice)> = entries_receiver.await.unwrap().unwrap();

        let (clear_sender, clear_receiver) = channel();

        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Clear(
            Keyspaces::DiscordApplicationCommands,
            clear_sender,
        )));

        clear_receiver.await.unwrap().unwrap();

        let mut discord_commands = HashMap::new();

        let mut commands = HashMap::new();

        let mut results = HashMap::new();

        for (key, value) in entries {
            let key_string = String::from_utf8(key.to_vec()).unwrap();
            let (plugin_id_str, name) = key_string.split_at(36);

            let plugin_id = Uuid::from_str(plugin_id_str).unwrap();

            match sonic_rs::from_slice::<Command>(&value) {
                Ok(command_data) => commands
                    .entry(name.to_string())
                    .or_insert(vec![])
                    .push((plugin_id, command_data)),
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing a command from the {plugin_id} plugin requested to register, error: {err}"
                    );
                    continue;
                }
            };
        }

        let application_id = match http_client.current_user_application().await {
            Ok(response) => match response.model().await {
                Ok(application) => application.id,
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the application data, error: {}",
                        &err
                    );
                    return Err(());
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting the application data, error: {}",
                    &err
                );
                return Err(());
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
                return Err(());
            }
        };

        match http_client
            .request::<Vec<Command>>(global_discord_commands_request)
            .await
        {
            Ok(response) => match response.model().await {
                Ok(global_discord_commands) => {
                    for global_discord_command in global_discord_commands {
                        discord_commands
                            .insert(global_discord_command.name.clone(), global_discord_command);
                    }
                }
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the global application commands, error: {}",
                        &err
                    );
                    return Err(());
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting the global application commands, error: {}",
                    &err
                );
                return Err(());
            }
        }

        // TODO: Endpoint is limited to 200 guilds per request, pagination needs to be implemented.
        let current_user_guilds = match http_client.current_user_guilds().await {
            Ok(response) => match response.model().await {
                Ok(current_user_guilds) => current_user_guilds,
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the global application commands, error: {}",
                        &err
                    );
                    return Err(());
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting the global application commands, error: {}",
                    &err
                );
                return Err(());
            }
        };

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
                                single_guild_discord_command.name.clone(),
                                single_guild_discord_command,
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

                let plugin_results = results.entry(command.0).or_insert(vec![]);

                match Self::register_application_command(
                    http_client.clone(),
                    application_id,
                    &mut discord_commands,
                    &command.1,
                )
                .await
                {
                    Ok(command_id) => {
                        let (result_sender, result_receiver) = channel();

                        core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                            Keyspaces::DiscordApplicationCommands,
                            command.1.name.as_bytes().to_vec(),
                            sonic_rs::to_vec(&command_id).unwrap(),
                            result_sender,
                        )));

                        result_receiver.await.unwrap();

                        plugin_results.push((command.1.name, Ok(command_id.get())));
                    }
                    Err(()) => {
                        let err = format!(
                            "Failed to register the {} command from the {} plugin",
                            command.1.name, command.0
                        );

                        error!("{err}");

                        plugin_results.push((command.1.name, Err(err)));
                    }
                }
            } else {
                let mut command_name_occurence_count = 1;

                for mut command in commands_by_name.1 {
                    command.1.name += format!("~{command_name_occurence_count}").as_str();

                    let plugin_results = results.entry(command.0).or_insert(vec![]);

                    match Self::register_application_command(
                        http_client.clone(),
                        application_id,
                        &mut discord_commands,
                        &command.1,
                    )
                    .await
                    {
                        Ok(command_id) => {
                            let (result_sender, result_receiver) = channel();

                            core_tx.send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                Keyspaces::DiscordApplicationCommands,
                                command.1.name.as_bytes().to_vec(),
                                sonic_rs::to_vec(&command_id).unwrap(),
                                result_sender,
                            )));

                            result_receiver.await.unwrap();

                            plugin_results.push((command.1.name, Ok(command_id.get())));
                        }
                        Err(()) => {
                            let err = format!(
                                "Failed to register the {} command from the {} plugin",
                                command.1.name, command.0
                            );

                            error!("{err}");

                            plugin_results.push((command.1.name, Err(err)));
                        }
                    }

                    command_name_occurence_count += 1;
                }
            }
        }

        Self::delete_old_application_commands(http_client, application_id, &discord_commands)
            .await?;

        for result in results {
            core_tx.send(CoreMessages::Runtime(RuntimeMessages::Discord(
                crate::utils::channels::RuntimeMessagesDiscord::CallDiscordApplicationCommands(
                    result.0, result.1,
                ),
            )));
        }

        Ok(())
    }

    async fn register_application_command(
        http_client: Arc<Client>,
        application_id: Id<ApplicationMarker>,
        discord_commands: &mut HashMap<String, Command>,
        command: &Command,
    ) -> Result<Id<CommandMarker>, ()> {
        let request = if let Some(discord_command) = discord_commands.remove(&command.name) {
            let route = if let Some(guild_id) = command.guild_id {
                Route::UpdateGuildCommand {
                    application_id: application_id.get(),
                    command_id: discord_command.id.unwrap().get(),
                    guild_id: guild_id.get(),
                }
            } else {
                Route::UpdateGlobalCommand {
                    application_id: application_id.get(),
                    command_id: discord_command.id.unwrap().get(),
                }
            };

            match Request::builder(&route)
                .body(sonic_rs::to_vec(command).unwrap())
                .build()
            {
                Ok(request) => request,
                Err(err) => {
                    error!(
                        "Failed to build the create global command request, error: {}",
                        &err
                    );
                    return Err(());
                }
            }
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

            match Request::builder(&route)
                .body(sonic_rs::to_vec(command).unwrap())
                .build()
            {
                Ok(request) => request,
                Err(err) => {
                    error!(
                        "Failed to build the create global command request, error: {}",
                        &err
                    );
                    return Err(());
                }
            }
        };

        match http_client.request::<Command>(request).await {
            Ok(response) => match response.model().await {
                Ok(command) => Ok(command.id.unwrap()),
                Err(err) => {
                    error!(
                        "Something went wrong while deserializing the create global command response, error: {}",
                        &err
                    );
                    Err(())
                }
            },
            Err(err) => {
                error!(
                    "Something went wrong while requesting a global command creation, error: {}",
                    &err
                );
                Err(())
            }
        }
    }

    async fn delete_old_application_commands(
        http_client: Arc<Client>,
        application_id: Id<ApplicationMarker>,
        discord_commands: &HashMap<String, Command>,
    ) -> Result<(), ()> {
        for discord_command in discord_commands.values() {
            let route = match discord_command.guild_id {
                Some(guild_id) => Route::DeleteGuildCommand {
                    application_id: application_id.get(),
                    command_id: discord_command.id.unwrap().get(),
                    guild_id: guild_id.get(),
                },
                None => Route::DeleteGlobalCommand {
                    application_id: application_id.get(),
                    command_id: discord_command.id.unwrap().get(),
                },
            };

            let request = match Request::builder(&route).build() {
                Ok(request) => request,
                Err(err) => {
                    error!(
                        "Failed to build the create global command request, error: {}",
                        &err
                    );
                    return Err(());
                }
            };

            info!(
                "Deleting the {} command, guild id: {:?}",
                &discord_command.name, &discord_command.guild_id
            );
            match http_client.request::<()>(request).await {
                Ok(_) => (),
                Err(err) => {
                    error!(
                        "Something went wrong while requesting a command deletion, error: {}",
                        &err
                    );
                    return Err(());
                }
            }
        }

        Ok(())
    }
}
