/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::job_scheduler_import_types::SupportedJobSchedulerRegistrations;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsJobScheduler {
    ScheduledJobs,
}

impl From<Vec<PluginPermissionsJobScheduler>> for SupportedJobSchedulerRegistrations {
    fn from(plugin_permissions_job_scheduler: Vec<PluginPermissionsJobScheduler>) -> Self {
        let mut supported_job_scheduler_registrations = Self::empty();

        for plugin_permission_job_scheduler in &plugin_permissions_job_scheduler {
            match plugin_permission_job_scheduler {
                PluginPermissionsJobScheduler::ScheduledJobs => {
                    supported_job_scheduler_registrations |= Self::SCHEDULED_JOBS;
                }
            }
        }

        supported_job_scheduler_registrations
    }
}
