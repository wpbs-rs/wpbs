/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::HashMap;

use tokio::sync::oneshot::channel;

use crate::{
    TASKS,
    config::plugins::permissions::services::job_scheduler::PluginPermissionsJobScheduler,
    runtime::{
        internal::InternalRuntime,
        plugins::bindings::services::job_scheduler::{
            wpbs::shared::shared_types::{Host as SharedTypesHost, HostError},
            wpbs_services::job_scheduler::{
                job_scheduler_import_functions::Host as JobSchedulerImportFunctionsHost,
                job_scheduler_types::{
                    Deregistrations, DeregistrationsResult, Host as JobSchedulerTypesHost,
                    Registrations, RegistrationsResult,
                },
            },
        },
    },
    utils::channels::{CoreMessages, CoreMessagesServices, JobSchedulerMessages},
};

impl SharedTypesHost for InternalRuntime {}

impl JobSchedulerTypesHost for InternalRuntime {}

impl JobSchedulerImportFunctionsHost for InternalRuntime {
    async fn register(
        &mut self,
        registrations: Registrations,
    ) -> Result<RegistrationsResult, HostError> {
        if TASKS.read().await.services.job_scheduler.is_none() {
            return Err(HostError::from("The job scheduler service is disabled"));
        }

        if !self
            .metadata
            .permissions
            .services
            .job_scheduler
            .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
        {
            return Err(HostError::from(
                "Plugin does not have the permission to register scheduled jobs",
            ));
        }

        let mut scheduled_job_registrations_result = HashMap::new();

        for scheduled_job_registration in registrations {
            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::Services(CoreMessagesServices::JobScheduler(
                    JobSchedulerMessages::AddJob(
                        self.metadata.plugin_uuid,
                        scheduled_job_registration.clone(),
                        sender,
                    ),
                )))
                .unwrap();

            let job_scheduler_result = receiver.await.unwrap().map_err(|err| err.to_string());

            scheduled_job_registrations_result
                .insert(scheduled_job_registration, job_scheduler_result);
        }

        Ok(scheduled_job_registrations_result)
    }

    async fn deregister(
        &mut self,
        deregistrations: Deregistrations,
    ) -> Result<DeregistrationsResult, HostError> {
        if TASKS.read().await.services.job_scheduler.is_none() {
            return Err(HostError::from("The job scheduler service is disabled"));
        }

        if !self
            .metadata
            .permissions
            .services
            .job_scheduler
            .contains(&PluginPermissionsJobScheduler::ScheduledJobs)
        {
            return Err(HostError::from(
                "Plugin does not have the permission to deregister scheduled jobs",
            ));
        }

        let mut scheduled_job_deregistrations_result = HashMap::new();

        for scheduled_job_deregistration in deregistrations {
            let (sender, receiver) = channel();

            self.core_tx
                .send(CoreMessages::Services(CoreMessagesServices::JobScheduler(
                    JobSchedulerMessages::RemoveJob(
                        self.metadata.plugin_uuid,
                        scheduled_job_deregistration.clone(),
                        sender,
                    ),
                )))
                .unwrap();

            let job_scheduler_result = receiver.await.unwrap().map_err(|err| err.to_string());

            scheduled_job_deregistrations_result
                .insert(scheduled_job_deregistration, job_scheduler_result);
        }

        Ok(scheduled_job_deregistrations_result)
    }
}
