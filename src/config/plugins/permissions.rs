/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::config::plugins::permissions::{
    core::PluginPermissionsCore, services::PluginPermissionsServices,
};

pub mod core;
pub mod services;

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissions {
    #[serde(default)]
    pub core: Vec<PluginPermissionsCore>,
    #[serde(default)]
    pub services: PluginPermissionsServices,
}

impl PluginPermissions {
    pub fn calculate(&mut self) {
        if self.core.contains(&PluginPermissionsCore::All) {
            self.core = vec![
                PluginPermissionsCore::DependencyFunctions,
                PluginPermissionsCore::Shutdown,
            ];
        }

        self.services.calculate();
    }
}
