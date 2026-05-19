/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use serde::{Deserialize, Serialize};

use crate::runtime::plugins::wpbs::plugin::core_import_types::SupportedCoreRegistrations;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsCore {
    DependencyFunctions,
    Shutdown,
}

impl From<Vec<PluginPermissionsCore>> for SupportedCoreRegistrations {
    fn from(plugin_permissions_core: Vec<PluginPermissionsCore>) -> Self {
        let mut supported_core_registrations = Self::empty();

        for plugin_permission_core in &plugin_permissions_core {
            match plugin_permission_core {
                PluginPermissionsCore::DependencyFunctions => {
                    supported_core_registrations |= Self::DEPENDENCY_FUNCTIONS;
                }
                PluginPermissionsCore::Shutdown => {
                    supported_core_registrations |= Self::SHUTDOWN;
                }
            }
        }

        supported_core_registrations
    }
}
