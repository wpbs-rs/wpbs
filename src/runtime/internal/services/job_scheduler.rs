/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;
use uuid::Uuid;

use crate::{
    config::plugins::permissions::{
        PluginPermissions, services::job_scheduler::PluginPermissionsJobScheduler,
    },
    database::Keyspaces,
    runtime::{
        internal::InternalRuntime,
        plugins::{
            exports::wpbs::plugin::core_export_functions::Error,
            wpbs::plugin::{
                job_scheduler_import_functions::Host as JobSchedulerImportFunctionsHost,
                job_scheduler_import_types::{
                    Host as JobSchedulerImportTypesHost, JobSchedulerDeregistrations,
                    JobSchedulerDeregistrationsResult, JobSchedulerRegistrations,
                    JobSchedulerRegistrationsResult, SupportedJobSchedulerRegistrations,
                },
            },
        },
    },
    utils::channels::{CoreMessages, DatabaseMessages, JobSchedulerMessages},
};

impl JobSchedulerImportTypesHost for InternalRuntime {}

impl JobSchedulerImportFunctionsHost for InternalRuntime {
    async fn get_supported_registrations(&mut self) -> SupportedJobSchedulerRegistrations {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.metadata.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
            .services
            .job_scheduler
            .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
        {
            return SupportedJobSchedulerRegistrations::empty();
        }

        SupportedJobSchedulerRegistrations::all()
    }

    async fn register(
        &mut self,
        registrations: JobSchedulerRegistrations,
    ) -> JobSchedulerRegistrationsResult {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.metadata.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
            .services
            .job_scheduler
            .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
        {
            return JobSchedulerRegistrationsResult {
                scheduled_jobs: Err(Error::from(
                    "Plugin is not allowed to register scheduled jobs",
                )),
            };
        }

        let mut result = JobSchedulerRegistrationsResult {
            scheduled_jobs: Ok(vec![]),
        };

        for cron in registrations.scheduled_jobs {
            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::JobScheduler(
                    crate::utils::channels::JobSchedulerMessages::AddJob(
                        self.metadata.plugin_id,
                        cron.clone(),
                        sender,
                    ),
                ))
                .unwrap();

            let job_scheduler_result = receiver
                .await
                .unwrap()
                .map(|uuid| uuid.to_string())
                .map_err(|err| err.to_string());

            result
                .scheduled_jobs
                .as_mut()
                .unwrap()
                .push((cron, job_scheduler_result));
        }

        result
    }

    async fn deregister(
        &mut self,
        deregistrations: JobSchedulerDeregistrations,
    ) -> JobSchedulerDeregistrationsResult {
        let mut result = JobSchedulerDeregistrationsResult {
            scheduled_jobs: vec![],
        };

        for job_id_string in deregistrations.scheduled_jobs {
            let (sender, receiver) = channel();

            let job_id = match Uuid::parse_str(&job_id_string) {
                Ok(job_id) => job_id,
                Err(err) => {
                    result.scheduled_jobs.push((
                        job_id_string,
                        Err(format!(
                            "An error occured while parsing the job id string: {err}"
                        )),
                    ));
                    continue;
                }
            };

            self.core_tx
                .send(CoreMessages::JobScheduler(JobSchedulerMessages::RemoveJob(
                    job_id, sender,
                )))
                .unwrap();

            result.scheduled_jobs.push((
                job_id_string,
                receiver.await.unwrap().map_err(|err| err.to_string()),
            ))
        }

        result
    }
}
