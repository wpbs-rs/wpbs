/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, fmt::Write};

use tokio::sync::oneshot::channel;
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
    database::Keyspaces,
    runtime::{
        internal::InternalRuntime,
        plugins::wpbs::plugin::{
            core_import_functions::Host as CoreImportFunctionsHost,
            core_import_types::{
                CoreDeregistrationsResult, CoreRegistrationsResult, Deregistrations,
                DeregistrationsResult, Host as CoreImportTypesHost, LogLevels, Registrations,
                RegistrationsResult, ServicesDeregistrationsResult, ServicesRegistrationsResult,
            },
            core_types::{Host as CoreTypesHost, HostError},
            discord_import_types::{
                DiscordRegistrationsInteractionsResult, DiscordRegistrationsResult,
            },
            job_scheduler_import_types::{
                JobSchedulerDeregistrationsResult, JobSchedulerRegistrationsResult,
            },
        },
    },
    utils::channels::{
        CoreMessages, DatabaseMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesCore,
    },
};

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
        let (sender, receiver) = channel();

        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::PluginStore,
                key.into_bytes(),
                sender,
            )))
            .unwrap();

        receiver
            .await
            .unwrap()
            .map(|r| r.map(|s| s.to_vec()))
            .map_err(|err| err.to_string())
    }

    async fn set_state(&mut self, key: String, value: Vec<u8>) -> Result<(), HostError> {
        let (sender, receiver) = channel();

        let key = format!("{}:{key}", self.metadata.plugin_uuid);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                Keyspaces::PluginStore,
                key.into_bytes(),
                value,
                sender,
            )))
            .unwrap();

        receiver.await.unwrap().map_err(|err| err.to_string())
    }

    async fn clear_state(&mut self) -> Result<(), HostError> {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Prefix(
                Keyspaces::PluginStore,
                self.metadata.plugin_uuid.to_string().into_bytes(),
                sender,
            )))
            .unwrap();

        let entries = receiver.await.unwrap().unwrap();

        for entry in entries {
            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::DatabaseModule(DatabaseMessages::Remove(
                    Keyspaces::PluginStore,
                    entry.key().unwrap().to_vec(),
                    sender,
                )))
                .unwrap();

            receiver.await.unwrap().unwrap();
        }

        Ok(())
    }

    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    async fn register(&mut self, registrations: Registrations) -> RegistrationsResult {
        let core_registrations_result = if let Some(core_registrations) = registrations.core {
            let dependency_function_registrations_result =
                if let Some(dependency_function_registrations) =
                    core_registrations.dependency_functions
                {
                    if self
                        .metadata
                        .permissions
                        .core
                        .contains(&PluginPermissionsCore::DependencyFunctions)
                    {
                        let mut dependency_function_registrations_result = HashMap::new();

                        for dependency_function_registration in dependency_function_registrations {
                            let (sender, receiver) = channel();

                            let key = format!(
                                "{}/{}/{dependency_function_registration}:{}",
                                self.metadata.registry_id,
                                self.metadata.plugin_id,
                                self.metadata.version
                            );

                            self.core_tx
                                .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                    Keyspaces::DependencyFunctions,
                                    key.as_bytes().to_vec(),
                                    self.metadata.plugin_uuid.as_bytes().to_vec(),
                                    sender,
                                )))
                                .unwrap();

                            receiver.await.unwrap().unwrap();

                            dependency_function_registrations_result
                                .insert(dependency_function_registration, key);
                        }

                        Some(Ok(dependency_function_registrations_result))
                    } else {
                        Some(Err(HostError::from(
                            "Plugin is not allowed to register dependency functions",
                        )))
                    }
                } else {
                    None
                };

            Some(CoreRegistrationsResult {
                dependency_functions: dependency_function_registrations_result,
            })
        } else {
            None
        };

        let services_registrations_result = if let Some(services_registrations) =
            registrations.services
        {
            let job_scheduler_registrations_result = if let Some(job_scheduler_registrations) =
                services_registrations.job_scheduler
            {
                if TASKS.read().await.services.job_scheduler.is_none() {
                    Some(Err(HostError::from(
                        "The job scheduler service is disabled",
                    )))
                } else {
                    let scheduled_jobs_registrations_result =
                        if let Some(scheduled_job_registrations) =
                            job_scheduler_registrations.scheduled_jobs
                        {
                            if self
                                .metadata
                                .permissions
                                .services
                                .job_scheduler
                                .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
                            {
                                let mut scheduled_job_registrations_result = HashMap::new();

                                for scheduled_job_registration in scheduled_job_registrations {
                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::JobScheduler(
                                            JobSchedulerMessages::AddJob(
                                                self.metadata.plugin_uuid,
                                                scheduled_job_registration.clone(),
                                                sender,
                                            ),
                                        ))
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

                    Some(Ok(JobSchedulerRegistrationsResult {
                        scheduled_jobs: scheduled_jobs_registrations_result,
                    }))
                }
            } else {
                None
            };

            let discord_registrations_result = if let Some(discord_registrations) =
                services_registrations.discord
            {
                if TASKS.read().await.services.discord.is_none() {
                    Some(Err(HostError::from("The Discord service is disabled")))
                } else {
                    let event_registrations_result = if let Some(event_registrations) =
                        discord_registrations.events
                    {
                        let mut event_registrations_result = Vec::new();

                        for event_registration in event_registrations {
                            if self
                                .metadata
                                .permissions
                                .services
                                .discord
                                .events
                                .contains(&event_registration.into())
                            {
                                let (sender, receiver) = channel();

                                self.core_tx
                                    .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                        Keyspaces::DiscordEvents,
                                        format!(
                                            "{}:{}",
                                            event_registration, self.metadata.plugin_uuid
                                        )
                                        .into_bytes(),
                                        self.metadata.plugin_uuid.as_bytes().to_vec(),
                                        sender,
                                    )))
                                    .unwrap();

                                receiver.await.unwrap().unwrap();

                                event_registrations_result.push((event_registration, Ok(())));
                            } else {
                                event_registrations_result.push((
                                    event_registration,
                                    Err(HostError::from(
                                        "Plugin is not allowed to register for this event",
                                    )),
                                ));
                            }
                        }

                        Some(event_registrations_result)
                    } else {
                        None
                    };

                    let interaction_registrations_result = if let Some(interaction_registrations) =
                        discord_registrations.interactions
                    {
                        let application_command_registrations_result = if let Some(
                            application_command_registrations,
                        ) =
                            interaction_registrations.application_commands
                        {
                            if self
                                .metadata
                                .permissions
                                .services
                                .discord
                                .interactions
                                .contains(
                                    &PluginPermissionsDiscordInteractions::ApplicationCommands,
                                )
                            {
                                for application_command_registration in
                                    application_command_registrations
                                {
                                    let uuid = Uuid::new_v4();

                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::DatabaseModule(
                                            DatabaseMessages::Insert(
                                                Keyspaces::DiscordApplicationCommands,
                                                format!("{}:{}", self.metadata.plugin_uuid, uuid)
                                                    .as_bytes()
                                                    .to_vec(),
                                                application_command_registration
                                                    .as_bytes()
                                                    .to_vec(),
                                                sender,
                                            ),
                                        ))
                                        .unwrap();

                                    receiver.await.unwrap().unwrap();
                                }

                                Some(Ok(()))
                            } else {
                                Some(Err(HostError::from(
                                    "Plugin is not allowed to register application command interactions",
                                )))
                            }
                        } else {
                            None
                        };

                        let message_component_registrations_result = if let Some(
                            message_component_registrations,
                        ) =
                            interaction_registrations.message_components
                        {
                            if self
                                .metadata
                                .permissions
                                .services
                                .discord
                                .interactions
                                .contains(&PluginPermissionsDiscordInteractions::MessageComponents)
                            {
                                let mut message_component_registrations_result = Vec::new();

                                for _ in 0..message_component_registrations {
                                    let uuid = Uuid::new_v4();

                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::DatabaseModule(
                                            DatabaseMessages::Insert(
                                                Keyspaces::DiscordMessageComponents,
                                                uuid.as_bytes().to_vec(),
                                                self.metadata.plugin_uuid.as_bytes().to_vec(),
                                                sender,
                                            ),
                                        ))
                                        .unwrap();

                                    receiver.await.unwrap().unwrap();

                                    message_component_registrations_result.push(uuid.to_string());
                                }

                                Some(Ok(message_component_registrations_result))
                            } else {
                                Some(Err(HostError::from(
                                    "Plugin is not allowed to register message component interactions",
                                )))
                            }
                        } else {
                            None
                        };

                        let modal_registrations_result =
                            if let Some(modal_registrations) = interaction_registrations.modals {
                                if self
                                    .metadata
                                    .permissions
                                    .services
                                    .discord
                                    .interactions
                                    .contains(&PluginPermissionsDiscordInteractions::Modals)
                                {
                                    let mut modal_registrations_result = Vec::new();

                                    for _ in 0..modal_registrations {
                                        let uuid = Uuid::new_v4();

                                        let (sender, receiver) = channel();

                                        self.core_tx
                                            .send(CoreMessages::DatabaseModule(
                                                DatabaseMessages::Insert(
                                                    Keyspaces::DiscordModals,
                                                    uuid.as_bytes().to_vec(),
                                                    self.metadata.plugin_uuid.as_bytes().to_vec(),
                                                    sender,
                                                ),
                                            ))
                                            .unwrap();

                                        receiver.await.unwrap().unwrap();

                                        modal_registrations_result.push(uuid.to_string());
                                    }

                                    Some(Ok(modal_registrations_result))
                                } else {
                                    Some(Err(HostError::from(
                                        "Plugin is not allowed to register modal interactions",
                                    )))
                                }
                            } else {
                                None
                            };

                        Some(DiscordRegistrationsInteractionsResult {
                            application_commands: application_command_registrations_result,
                            message_components: message_component_registrations_result,
                            modals: modal_registrations_result,
                        })
                    } else {
                        None
                    };

                    Some(Ok(DiscordRegistrationsResult {
                        events: event_registrations_result,
                        interactions: interaction_registrations_result,
                    }))
                }
            } else {
                None
            };

            Some(ServicesRegistrationsResult {
                job_scheduler: job_scheduler_registrations_result,
                discord: discord_registrations_result,
            })
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
        let mut key = format!("{registry_id}/{plugin_id}/{function_id}:");
        let response = if let Some(plugin_version) = plugin_version {
            write!(key, "{plugin_version}").unwrap();

            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                    Keyspaces::DependencyFunctions,
                    key.as_bytes().to_vec(),
                    sender,
                )))
                .unwrap();

            receiver.await.unwrap().unwrap()
        } else {
            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::DatabaseModule(DatabaseMessages::Prefix(
                    Keyspaces::DependencyFunctions,
                    key.as_bytes().to_vec(),
                    sender,
                )))
                .unwrap();

            receiver
                .await
                .unwrap()
                .unwrap()
                .next()
                .map(|g| g.value().unwrap())
        };

        let Some(response_bytes) = response else {
            return Err(format!("The {key} dependency function was not found"));
        };

        let (sender, receiver) = channel();

        let _ = self
            .core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::CallDependencyFunction(
                    Uuid::from_slice(&response_bytes).unwrap(),
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
