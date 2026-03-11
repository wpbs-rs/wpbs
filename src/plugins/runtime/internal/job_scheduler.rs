/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use crate::plugins::{
    discord_bot::plugin::{
        job_scheduler_import_functions::Host as JobSchedulerImportFunctionsHost,
        job_scheduler_import_types::{
            Host as JobSchedulerImportTypesHost, JobSchedulerRegistrations,
            JobSchedulerRegistrationsResult, SupportedJobSchedulerRegistrations,
        },
    },
    runtime::internal::InternalRuntime,
};

impl JobSchedulerImportTypesHost for InternalRuntime {}

impl JobSchedulerImportFunctionsHost for InternalRuntime {
    async fn get_supported_registrations(&mut self) -> SupportedJobSchedulerRegistrations {
        todo!()
    }

    async fn register(
        &mut self,
        registrations: JobSchedulerRegistrations,
    ) -> JobSchedulerRegistrationsResult {
        todo!()
    }
}
