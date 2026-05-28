/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::config::plugins::permissions::services::{
    discord::PluginPermissionsDiscord, job_scheduler::PluginPermissionsJobScheduler,
};

pub mod discord;
pub mod job_scheduler;

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissionsServices {
    #[serde(default)]
    pub job_scheduler: Vec<PluginPermissionsJobScheduler>,
    #[serde(default)]
    pub discord: PluginPermissionsDiscord,
}

impl PluginPermissionsServices {
    pub fn calculate(&mut self) {
        if self
            .job_scheduler
            .contains(&PluginPermissionsJobScheduler::All)
        {
            self.job_scheduler = vec![PluginPermissionsJobScheduler::ScheduledJobs];
        }

        self.discord.calculate();
    }
}
