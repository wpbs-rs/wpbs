/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::{HashMap, HashSet};

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
            LogLevels::Trace => trace!(message),
            LogLevels::Debug => debug!(message),
            LogLevels::Info => info!(message),
            LogLevels::Warn => warn!(message),
            LogLevels::Error => error!(message),
        }
    }

    async fn get_state(&mut self, key: String) -> Result<Option<Vec<u8>>, HostError> {
        let (sender, receiver) = channel();

        let key = format!("{}:{key}", self.metadata.plugin_id);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::PluginStore,
                key.as_bytes().to_vec(),
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

        let key = format!("{}:{key}", self.metadata.plugin_id);

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                Keyspaces::PluginStore,
                key.as_bytes().to_vec(),
                value,
                sender,
            )))
            .unwrap();

        receiver.await.unwrap().map_err(|err| err.to_string())
    }

    // TODO: Split up in sub functions
    #[allow(clippy::too_many_lines)]
    async fn register(&mut self, registrations: Registrations) -> RegistrationsResult {
        let mut result = RegistrationsResult {
            core: None,
            services: None,
        };

        if let Some(core_registrations) = registrations.core {
            result.core = Some(CoreRegistrationsResult {
                dependency_functions: None,
            });

            if let Some(dependency_function_registrations) = core_registrations.dependency_functions
            {
                if self
                    .metadata
                    .permissions
                    .core
                    .contains(&PluginPermissionsCore::DependencyFunctions)
                {
                    result.core.as_mut().unwrap().dependency_functions = Some(Ok(HashMap::new()));

                    for dependency_function_registration in dependency_function_registrations {
                        let (sender, receiver) = channel();

                        let key = format!(
                            "{}/{}/{dependency_function_registration}",
                            self.metadata.registry_id, self.metadata.id
                        );

                        self.core_tx
                            .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                Keyspaces::DependencyFunctions,
                                key.as_bytes().to_vec(),
                                self.metadata.plugin_id.as_bytes().to_vec(),
                                sender,
                            )))
                            .unwrap();

                        receiver.await.unwrap().unwrap();

                        result
                            .core
                            .as_mut()
                            .unwrap()
                            .dependency_functions
                            .as_mut()
                            .unwrap()
                            .as_mut()
                            .unwrap()
                            .insert(dependency_function_registration, key);
                    }
                } else {
                    result.core.as_mut().unwrap().dependency_functions = Some(Err(
                        HostError::from("Plugin is not allowed to register dependency functions"),
                    ));
                }
            }
        }

        if let Some(services_registrations) = registrations.services {
            result.services = Some(ServicesRegistrationsResult {
                job_scheduler: None,
                discord: None,
            });

            if let Some(job_scheduler_registrations) = services_registrations.job_scheduler {
                if TASKS.read().await.services.job_scheduler.is_none() {
                    result.services.as_mut().unwrap().job_scheduler = Some(Err(HostError::from(
                        "The job scheduler service is disabled",
                    )));
                } else {
                    result.services.as_mut().unwrap().job_scheduler =
                        Some(Ok(JobSchedulerRegistrationsResult {
                            scheduled_jobs: None,
                        }));

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
                            result
                                .services
                                .as_mut()
                                .unwrap()
                                .job_scheduler
                                .as_mut()
                                .unwrap()
                                .as_mut()
                                .unwrap()
                                .scheduled_jobs = Some(Ok(HashMap::new()));

                            for scheduled_job_registration in scheduled_job_registrations {
                                let (sender, receiver) = channel();

                                self.core_tx
                                    .send(CoreMessages::JobScheduler(JobSchedulerMessages::AddJob(
                                        self.metadata.plugin_id,
                                        scheduled_job_registration.clone(),
                                        sender,
                                    )))
                                    .unwrap();

                                let job_scheduler_result = receiver
                                    .await
                                    .unwrap()
                                    .map(|uuid| uuid.to_string())
                                    .map_err(|err| err.to_string());

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
                                    .as_mut()
                                    .unwrap()
                                    .insert(scheduled_job_registration, job_scheduler_result);
                            }
                        } else {
                            result
                                .services
                                .as_mut()
                                .unwrap()
                                .job_scheduler
                                .as_mut()
                                .unwrap()
                                .as_mut()
                                .unwrap()
                                .scheduled_jobs = Some(Err(HostError::from(
                                "Plugin is not allowed to register scheduled jobs",
                            )));
                        }
                    }
                }
            }

            if let Some(discord_registrations) = services_registrations.discord {
                if TASKS.read().await.services.discord.is_none() {
                    result.services.as_mut().unwrap().discord =
                        Some(Err(HostError::from("The Discord service is disabled")));
                } else {
                    result.services.as_mut().unwrap().discord =
                        Some(Ok(DiscordRegistrationsResult {
                            events: None,
                            interactions: None,
                        }));

                    if let Some(event_registrations) = discord_registrations.events {
                        result
                            .services
                            .as_mut()
                            .unwrap()
                            .discord
                            .as_mut()
                            .unwrap()
                            .as_mut()
                            .unwrap()
                            .events = Some(Vec::new());

                        for event_registration in event_registrations {
                            if self
                                .metadata
                                .permissions
                                .services
                                .discord
                                .events
                                .contains(&event_registration.into())
                            {
                                let (get_sender, get_receiver) = channel();

                                self.core_tx
                                    .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                                        Keyspaces::DiscordEvents,
                                        event_registration.into(),
                                        get_sender,
                                    )))
                                    .unwrap();

                                let mut set = match get_receiver.await.unwrap().unwrap() {
                                    Some(response) => sonic_rs::from_slice(&response).unwrap(),
                                    None => HashSet::new(),
                                };

                                set.insert(self.metadata.plugin_id.to_string());

                                let (insert_sender, insert_receiver) = channel();

                                self.core_tx
                                    .send(CoreMessages::DatabaseModule(DatabaseMessages::Insert(
                                        Keyspaces::DiscordEvents,
                                        event_registration.into(),
                                        sonic_rs::to_vec(&set).unwrap(),
                                        insert_sender,
                                    )))
                                    .unwrap();

                                insert_receiver.await.unwrap().unwrap();

                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .events
                                    .as_mut()
                                    .unwrap()
                                    .push((event_registration, Ok(())));
                            } else {
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .events
                                    .as_mut()
                                    .unwrap()
                                    .push((
                                        event_registration,
                                        Err(HostError::from(
                                            "Plugin is not allowed to register for this event",
                                        )),
                                    ));
                            }
                        }
                    }

                    if let Some(interaction_registrations) = discord_registrations.interactions {
                        result
                            .services
                            .as_mut()
                            .unwrap()
                            .discord
                            .as_mut()
                            .unwrap()
                            .as_mut()
                            .unwrap()
                            .interactions = Some(DiscordRegistrationsInteractionsResult {
                            application_commands: None,
                            message_components: None,
                            modals: None,
                        });

                        if let Some(application_command_registrations) =
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
                                for (index, application_command_registration) in
                                    application_command_registrations.into_iter().enumerate()
                                {
                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::DatabaseModule(
                                            DatabaseMessages::Insert(
                                                Keyspaces::DiscordApplicationCommands,
                                                format!(
                                                    "{}:{}",
                                                    self.metadata.plugin_id,
                                                    index + 1
                                                )
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

                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .application_commands = Some(Ok(()));
                            } else {
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .application_commands = Some(Err(HostError::from(
                                    "Plugin is not allowed to register application command interactions",
                                )));
                            }
                        }

                        if let Some(message_component_registrations) =
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
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .message_components = Some(Ok(Vec::new()));

                                for _ in 0..message_component_registrations {
                                    let uuid = Uuid::new_v4();

                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::DatabaseModule(
                                            DatabaseMessages::Insert(
                                                Keyspaces::DiscordMessageComponents,
                                                uuid.as_bytes().to_vec(),
                                                self.metadata.plugin_id.as_bytes().to_vec(),
                                                sender,
                                            ),
                                        ))
                                        .unwrap();

                                    receiver.await.unwrap().unwrap();

                                    result
                                        .services
                                        .as_mut()
                                        .unwrap()
                                        .discord
                                        .as_mut()
                                        .unwrap()
                                        .as_mut()
                                        .unwrap()
                                        .interactions
                                        .as_mut()
                                        .unwrap()
                                        .message_components
                                        .as_mut()
                                        .unwrap()
                                        .as_mut()
                                        .unwrap()
                                        .push(uuid.to_string());
                                }
                            } else {
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .message_components = Some(Err(HostError::from(
                                    "Plugin is not allowed to register message component interactions",
                                )));
                            }
                        }

                        if let Some(modal_registrations) = interaction_registrations.modals {
                            if self
                                .metadata
                                .permissions
                                .services
                                .discord
                                .interactions
                                .contains(&PluginPermissionsDiscordInteractions::Modals)
                            {
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .modals = Some(Ok(Vec::new()));

                                for _ in 0..modal_registrations {
                                    let uuid = Uuid::new_v4();

                                    let (sender, receiver) = channel();

                                    self.core_tx
                                        .send(CoreMessages::DatabaseModule(
                                            DatabaseMessages::Insert(
                                                Keyspaces::DiscordModals,
                                                uuid.as_bytes().to_vec(),
                                                self.metadata.plugin_id.as_bytes().to_vec(),
                                                sender,
                                            ),
                                        ))
                                        .unwrap();

                                    receiver.await.unwrap().unwrap();

                                    result
                                        .services
                                        .as_mut()
                                        .unwrap()
                                        .discord
                                        .as_mut()
                                        .unwrap()
                                        .as_mut()
                                        .unwrap()
                                        .interactions
                                        .as_mut()
                                        .unwrap()
                                        .modals
                                        .as_mut()
                                        .unwrap()
                                        .as_mut()
                                        .unwrap()
                                        .push(uuid.to_string());
                                }
                            } else {
                                result
                                    .services
                                    .as_mut()
                                    .unwrap()
                                    .discord
                                    .as_mut()
                                    .unwrap()
                                    .as_mut()
                                    .unwrap()
                                    .interactions
                                    .as_mut()
                                    .unwrap()
                                    .modals = Some(Err(HostError::from(
                                    "Plugin is not allowed to register modal interactions",
                                )));
                            }
                        }
                    }
                }
            }
        }

        result
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
        self.core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::UnloadPlugin(self.metadata.plugin_id),
            )))
            .unwrap();

        info!(
            "The {} plugin has unloaded itself, reason: {reason}",
            self.metadata.user_id
        );
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

        let shutdown_type = if restart {
            Shutdown::Restart
        } else {
            Shutdown::Normal
        };

        self.core_tx
            .send(CoreMessages::Shutdown(shutdown_type))
            .unwrap();

        Ok(())
    }

    async fn dependency_function(
        &mut self,
        registry_id: String,
        plugin_id: String,
        function_id: String,
        params: Vec<u8>,
    ) -> Result<Vec<u8>, HostError> {
        let (sender, receiver) = channel();

        let key = format!("{registry_id}/{plugin_id}/{function_id}");

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::DependencyFunctions,
                key.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let Some(response_bytes) = receiver.await.unwrap().unwrap() else {
            return Err(format!("The {key} dependency function was not found"));
        };

        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::Runtime(RuntimeMessages::Core(
                RuntimeMessagesCore::CallDependencyFunction(
                    Uuid::from_slice(&response_bytes).unwrap(),
                    function_id,
                    params,
                    sender,
                ),
            )))
            .unwrap();

        receiver.await.unwrap()
    }
}
