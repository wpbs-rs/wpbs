/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::Deserialize;

use crate::config::services::{discord::ConfigDiscord, job_scheduler::ConfigJobScheduler};

pub mod discord;
pub mod job_scheduler;

#[derive(Default, Deserialize)]
pub struct ConfigServices {
    #[serde(default)]
    pub job_scheduler: ConfigJobScheduler,
    #[serde(default)]
    pub discord: ConfigDiscord,
}
