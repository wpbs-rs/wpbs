/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use tokio::sync::oneshot::channel;

use crate::{
    config::plugins::permissions::{PluginPermissions, PluginPermissionsJobScheduler},
    database::Keyspaces,
    runtime::{
        internal::InternalRuntime,
        plugins::{
            exports::wpbs::plugin::core_export_functions::Error,
            wpbs::plugin::{
                job_scheduler_import_functions::Host as JobSchedulerImportFunctionsHost,
                job_scheduler_import_types::{
                    Host as JobSchedulerImportTypesHost, JobSchedulerRegistrations,
                    JobSchedulerRegistrationsResult, SupportedJobSchedulerRegistrations,
                },
            },
        },
    },
    utils::channels::{CoreMessages, DatabaseMessages},
};

impl JobSchedulerImportTypesHost for InternalRuntime {}

impl JobSchedulerImportFunctionsHost for InternalRuntime {
    async fn get_supported_registrations(&mut self) -> SupportedJobSchedulerRegistrations {
        let (sender, receiver) = channel();

        self.core_tx
            .send(CoreMessages::DatabaseModule(DatabaseMessages::Get(
                Keyspaces::Plugins,
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
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
                self.plugin_id.as_bytes().to_vec(),
                sender,
            )))
            .unwrap();

        let response_bytes = receiver.await.unwrap().unwrap().unwrap().to_vec();

        let plugin_permissions =
            sonic_rs::from_slice::<PluginPermissions>(&response_bytes).unwrap();

        if !plugin_permissions
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
                        self.plugin_id,
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
}
