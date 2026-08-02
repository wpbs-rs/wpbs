/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, fmt::Write, sync::Arc};

use fjall::{Database, Guard, KeyspaceCreateOptions};
use tokio::sync::{mpsc::UnboundedSender, oneshot::channel};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::{
    Shutdown, TASKS,
    config::plugins::permissions::{
        core::PluginPermissionsCore,
        services::{
            discord::PluginPermissionsDiscordInteractions,
            job_scheduler::PluginPermissionsJobScheduler,
        },
    },
    runtime::{
        internal::InternalRuntime,
        plugins::{
            RuntimePluginMetadata,
            wpbs::plugin::{
                core_import_functions::Host as CoreImportFunctionsHost,
                core_import_types::{
                    CoreDeregistrationsResult, CoreRegistrations, CoreRegistrationsResult,
                    Deregistrations, DeregistrationsResult, DiscordRegistrations,
                    Host as CoreImportTypesHost, JobSchedulerRegistrations, LogLevels,
                    Registrations, RegistrationsResult, ServicesDeregistrationsResult,
                    ServicesRegistrations, ServicesRegistrationsResult,
                },
                core_types::{Host as CoreTypesHost, HostError},
                discord_import_types::{
                    DiscordEventKinds, DiscordRegistrationsInteractionsResult,
                    DiscordRegistrationsResult,
                },
                job_scheduler_import_types::{
                    JobSchedulerDeregistrationsResult, JobSchedulerRegistrationsResult,
                },
            },
        },
    },
    utils::channels::{CoreMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesCore},
};

type DiscordEventRegistrationsResult =
    Result<Vec<(DiscordEventKinds, Result<(), HostError>)>, HostError>;

impl CoreTypesHost for InternalRuntime {}
impl CoreImportTypesHost for InternalRuntime {}

impl CoreImportFunctionsHost for InternalRuntime {
    async fn log(&mut self, level: LogLevels, message: String) {
        match level {
            LogLevels::Trace => trace!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Debug => debug!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Info => info!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Warn => warn!("[{}]: {message}", self.metadata.user_id),
            LogLevels::Error => error!("[{}]: {message}", self.metadata.user_id),
        }
    }

    async fn get_state(&mut self, key: String) -> Result<Option<Vec<u8>>, HostError> {
        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        let plugin_store_keyspace = self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        plugin_store_keyspace
            .get(&key)
            .map_err(|err| err.to_string())
            .map(|r| r.map(|s| s.to_vec()))
    }

    async fn set_state(&mut self, key: String, value: Vec<u8>) -> Result<(), HostError> {
        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        let plugin_store_keyspace = self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        plugin_store_keyspace
            .insert(&key, &value)
            .map_err(|err| err.to_string())
    }

    async fn clear_state(&mut self) -> Result<(), HostError> {
        let plugin_store_keyspace = self
            .database
            .keyspace("plugin_store", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        let entries = plugin_store_keyspace.prefix(self.metadata.plugin_uuid.as_bytes());

        for entry in entries {
            plugin_store_keyspace
                .remove(entry.key().map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?;
        }

        Ok(())
    }

    async fn register(&mut self, registrations: Registrations) -> RegistrationsResult {
        let core_registrations_result = registrations.core.map(|cr| self.register_core(cr));

        let services_registrations_result =
            if let Some(services_registrations) = registrations.services {
                Some(
                    Self::register_services(
                        self.database.clone(),
                        self.core_tx.clone(),
                        self.metadata.clone(),
                        services_registrations,
                    )
                    .await,
                )
            } else {
                None
            };

        RegistrationsResult {
            core: core_registrations_result,
            services: services_registrations_result,
        }
    }

    async fn deregister(&mut self, deregistrations: Deregistrations) -> DeregistrationsResult {
        let mut result = DeregistrationsResult {
            core: None,
            services: None,
        };

        if let Some(core_deregistrations) = deregistrations.core {
            result.core = Some(CoreDeregistrationsResult {
                dependency_functions: None,
            });

            if let Some(_dependency_function_deregistrations) =
                core_deregistrations.dependency_functions
            {
                // TODO: Implement
            }
        }

        if let Some(services_deregistrations) = deregistrations.services {
            result.services = Some(ServicesDeregistrationsResult {
                job_scheduler: None,
                discord: None,
            });

            if let Some(job_scheduler_deregistrations) = services_deregistrations.job_scheduler {
                if TASKS.read().await.services.job_scheduler.is_none() {
                    result.services.as_mut().unwrap().job_scheduler = Some(Err(HostError::from(
                        "The job scheduler service is disabled",
                    )));
                } else {
                    result.services.as_mut().unwrap().job_scheduler =
                        Some(Ok(JobSchedulerDeregistrationsResult {
                            scheduled_jobs: None,
                        }));

                    if let Some(scheduled_job_deregistrations) =
                        job_scheduler_deregistrations.scheduled_jobs
                    {
                        for scheduled_job_deregistration in scheduled_job_deregistrations {
                            let (sender, receiver) = channel();

                            let job_id = match Uuid::parse_str(&scheduled_job_deregistration) {
                                Ok(job_id) => job_id,
                                Err(err) => {
                                    result
                                .services
                                .as_mut()
                                .unwrap()
                                .job_scheduler
                                .as_mut()
                                .unwrap()
                                .as_mut()
                                .unwrap()
                                .scheduled_jobs
                                .as_mut()
                                .unwrap()
                                .insert(
                                    scheduled_job_deregistration,
                                    Err(format!(
                                        "An error occurred while parsing the job id string: {err}"
                                    )),
                                );
                                    continue;
                                }
                            };

                            self.core_tx
                                .send(CoreMessages::JobScheduler(JobSchedulerMessages::RemoveJob(
                                    job_id, sender,
                                )))
                                .unwrap();

                            result
                                .services
                                .as_mut()
                                .unwrap()
                                .job_scheduler
                                .as_mut()
                                .unwrap()
                                .as_mut()
                                .unwrap()
                                .scheduled_jobs
                                .as_mut()
                                .unwrap()
                                .insert(
                                    scheduled_job_deregistration,
                                    receiver.await.unwrap().map_err(|err| err.to_string()),
                                );
                        }
                    }
                }
            }

            if let Some(_discord_deregistrations) = services_deregistrations.discord {
                // TODO: Implement
            }
        }

        result
    }

    async fn remove(&mut self, reason: String) {
        if self
            .core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::RemovePlugin(self.metadata.plugin_uuid),
            )))
            .is_ok()
        {
            info!(
                "The {} plugin has unloaded itself, reason: {reason}",
                self.metadata.user_id
            );
        }
    }

    async fn shutdown(&mut self, restart: bool) -> Result<(), HostError> {
        if !self
            .metadata
            .permissions
            .core
            .contains(&PluginPermissionsCore::Shutdown)
        {
            return Err(HostError::from("Not allowed to call shutdown"));
        }

        let shutdown_kind = if restart {
            Shutdown::Restart
        } else {
            Shutdown::Normal
        };

        self.core_tx
            .send(CoreMessages::Shutdown(shutdown_kind))
            .unwrap();

        Ok(())
    }

    async fn dependency_function(
        &mut self,
        registry_id: String,
        plugin_id: String,
        function_id: String,
        plugin_version: Option<String>,
        params: Vec<u8>,
    ) -> Result<Vec<u8>, HostError> {
        let mut key = format!("{registry_id}:{plugin_id}:{function_id}@");

        if let Some(plugin_version) = plugin_version {
            write!(key, "{plugin_version}").unwrap();
        }

        let dependency_functions_keyspace = self
            .database
            .keyspace("dependency_functions", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        let Some(plugin_id) = dependency_functions_keyspace
            .prefix(&key)
            .next()
            .map(Guard::value)
            .transpose()
            .map_err(|err| err.to_string())?
        else {
            return Err(format!("The {key} dependency function was not found"));
        };

        let (sender, receiver) = channel();

        let _ = self
            .core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::CallDependencyFunction(
                    Uuid::from_slice(&plugin_id).unwrap(),
                    function_id,
                    params,
                    sender,
                ),
            )));

        receiver
            .await
            .unwrap_or(Err(HostError::from("Runtime is shutting down")))
    }
}

impl InternalRuntime {
    fn register_core(&self, core_registrations: CoreRegistrations) -> CoreRegistrationsResult {
        let dependency_functions = core_registrations
            .dependency_functions
            .map(|dfr| self.register_dependency_functions(dfr));

        CoreRegistrationsResult {
            dependency_functions,
        }
    }

    fn register_dependency_functions(
        &self,
        dependency_function_registrations: Vec<String>,
    ) -> Result<HashMap<String, String>, HostError> {
        if !self
            .metadata
            .permissions
            .core
            .contains(&PluginPermissionsCore::DependencyFunctions)
        {
            return Err(HostError::from(
                "Plugin is not allowed to register dependency functions",
            ));
        }

        let mut dependency_function_registrations_result = HashMap::new();

        let dependency_functions_keyspace = self
            .database
            .keyspace("dependency_functions", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for dependency_function_registration in dependency_function_registrations {
            let key = format!(
                "{}:{}:{dependency_function_registration}@{}",
                self.metadata.namespace_id, self.metadata.plugin_id, self.metadata.version
            );

            dependency_functions_keyspace
                .insert(&key, self.metadata.plugin_uuid.as_bytes())
                .map_err(|err| err.to_string())?;

            dependency_function_registrations_result.insert(dependency_function_registration, key);
        }

        Ok(dependency_function_registrations_result)
    }

    async fn register_services(
        database: Database,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_metadata: Arc<RuntimePluginMetadata>,
        services_registrations: ServicesRegistrations,
    ) -> ServicesRegistrationsResult {
        let job_scheduler =
            if let Some(job_scheduler_registrations) = services_registrations.job_scheduler {
                Some(
                    Self::register_job_scheduler(
                        core_tx,
                        plugin_metadata.clone(),
                        job_scheduler_registrations,
                    )
                    .await,
                )
            } else {
                None
            };

        let discord = if let Some(discord_registrations) = services_registrations.discord {
            Some(Self::register_discord(database, plugin_metadata, discord_registrations).await)
        } else {
            None
        };

        ServicesRegistrationsResult {
            job_scheduler,
            discord,
        }
    }

    async fn register_job_scheduler(
        core_tx: UnboundedSender<CoreMessages>,
        plugin_metadata: Arc<RuntimePluginMetadata>,
        job_scheduler_registrations: JobSchedulerRegistrations,
    ) -> Result<JobSchedulerRegistrationsResult, HostError> {
        if TASKS.read().await.services.job_scheduler.is_none() {
            return Err(HostError::from("The job scheduler service is disabled"));
        }

        let scheduled_jobs_registrations_result =
            if let Some(scheduled_job_registrations) = job_scheduler_registrations.scheduled_jobs {
                if plugin_metadata
                    .permissions
                    .services
                    .job_scheduler
                    .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
                {
                    let mut scheduled_job_registrations_result = HashMap::new();

                    for scheduled_job_registration in scheduled_job_registrations {
                        let (sender, receiver) = channel();

                        core_tx
                            .send(CoreMessages::JobScheduler(JobSchedulerMessages::AddJob(
                                plugin_metadata.plugin_uuid,
                                scheduled_job_registration.clone(),
                                sender,
                            )))
                            .unwrap();

                        let job_scheduler_result = receiver
                            .await
                            .unwrap()
                            .map(|uuid| uuid.to_string())
                            .map_err(|err| err.to_string());

                        scheduled_job_registrations_result
                            .insert(scheduled_job_registration, job_scheduler_result);
                    }

                    Some(Ok(scheduled_job_registrations_result))
                } else {
                    Some(Err(HostError::from(
                        "Plugin is not allowed to register scheduled jobs",
                    )))
                }
            } else {
                None
            };

        Ok(JobSchedulerRegistrationsResult {
            scheduled_jobs: scheduled_jobs_registrations_result,
        })
    }

    async fn register_discord(
        database: Database,
        plugin_metadata: Arc<RuntimePluginMetadata>,
        discord_registrations: DiscordRegistrations,
    ) -> Result<DiscordRegistrationsResult, HostError> {
        if TASKS.read().await.services.discord.is_none() {
            return Err(HostError::from("The Discord service is disabled"));
        }

        let event_registrations_result = discord_registrations
            .events
            .map(|er| Self::register_discord_events(&database, &plugin_metadata, er));

        let interaction_registrations_result = if let Some(interaction_registrations) =
            discord_registrations.interactions
        {
            let application_command_registrations_result =
                interaction_registrations.application_commands.map(|acr| {
                    Self::register_discord_application_commands(&database, &plugin_metadata, acr)
                });

            let message_component_registrations_result =
                interaction_registrations.message_components.map(|mcr| {
                    Self::register_discord_message_components(&database, &plugin_metadata, mcr)
                });

            let modal_registrations_result = interaction_registrations
                .modals
                .map(|mr| Self::register_discord_modals(&database, &plugin_metadata, mr));

            Some(DiscordRegistrationsInteractionsResult {
                application_commands: application_command_registrations_result,
                message_components: message_component_registrations_result,
                modals: modal_registrations_result,
            })
        } else {
            None
        };

        Ok(DiscordRegistrationsResult {
            events: event_registrations_result,
            interactions: interaction_registrations_result,
        })
    }

    fn register_discord_events(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        event_registrations: Vec<DiscordEventKinds>,
    ) -> DiscordEventRegistrationsResult {
        let mut event_registrations_result = Vec::new();

        let events_keyspace = database
            .keyspace("discord_events", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for event_registration in event_registrations {
            if !plugin_metadata
                .permissions
                .services
                .discord
                .events
                .contains(&event_registration.into())
            {
                event_registrations_result.push((
                    event_registration,
                    Err(HostError::from(
                        "Plugin is not allowed to register for this event",
                    )),
                ));
            }

            let key = format!(
                "{}:{}",
                event_registration.as_str(),
                plugin_metadata.plugin_uuid
            );

            if let Err(err) = events_keyspace.insert(&key, plugin_metadata.plugin_uuid.as_bytes()) {
                event_registrations_result.push((event_registration, Err(err.to_string())));
                continue;
            }

            event_registrations_result.push((event_registration, Ok(())));
        }

        Ok(event_registrations_result)
    }

    fn register_discord_application_commands(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        application_command_registrations: Vec<String>,
    ) -> Result<(), HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::ApplicationCommands)
        {
            return Err(HostError::from(
                "Plugin is not allowed to register application command interactions",
            ));
        }

        let application_commands_keyspace = database
            .keyspace(
                "discord_application_commands",
                KeyspaceCreateOptions::default,
            )
            .map_err(|err| err.to_string())?;

        for application_command_registration in application_command_registrations {
            let uuid = Uuid::new_v4();

            let key = format!("{}:{}", plugin_metadata.plugin_uuid, uuid);

            application_commands_keyspace
                .insert(&key, application_command_registration.as_bytes())
                .map_err(|err| err.to_string())?;
        }

        Ok(())
    }

    fn register_discord_message_components(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        message_component_registrations: u16,
    ) -> Result<Vec<String>, HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::MessageComponents)
        {
            return Err(HostError::from(
                "Plugin is not allowed to register message component interactions",
            ));
        }

        let mut message_component_registrations_result = Vec::new();

        let message_components_keyspace = database
            .keyspace("discord_message_components", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for _ in 0..message_component_registrations {
            let uuid = Uuid::new_v4();

            message_components_keyspace
                .insert(uuid.as_bytes(), plugin_metadata.plugin_uuid.as_bytes())
                .map_err(|err| err.to_string())?;

            message_component_registrations_result.push(uuid.to_string());
        }

        Ok(message_component_registrations_result)
    }

    fn register_discord_modals(
        database: &Database,
        plugin_metadata: &Arc<RuntimePluginMetadata>,
        modal_registrations: u16,
    ) -> Result<Vec<String>, HostError> {
        if !plugin_metadata
            .permissions
            .services
            .discord
            .interactions
            .contains(&PluginPermissionsDiscordInteractions::Modals)
        {
            return Err(HostError::from(
                "Plugin is not allowed to register modal interactions",
            ));
        }

        let mut modal_registrations_result = Vec::new();

        let modals_keyspace = database
            .keyspace("discord_modals", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;

        for _ in 0..modal_registrations {
            let uuid = Uuid::new_v4();

            modals_keyspace
                .insert(uuid.as_bytes(), plugin_metadata.plugin_uuid.as_bytes())
                .map_err(|err| err.to_string())?;

            modal_registrations_result.push(uuid.to_string());
        }

        Ok(modal_registrations_result)
    }
}
