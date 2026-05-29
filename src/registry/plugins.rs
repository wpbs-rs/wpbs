/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::HashMap;

use semver::Version;
use serde::Deserialize;
use serde_yaml_ng::Value;

use crate::config::plugins::permissions::PluginPermissions;

#[derive(Deserialize)]
pub struct RegistryPlugin {
    pub versions: Vec<RegistryPluginVersion>,
    #[allow(unused)]
    pub description: String,
}

#[derive(Deserialize)]
pub struct RegistryPluginVersion {
    pub version: String,
    #[allow(unused)]
    pub release_time: String,
    pub compatible_program_version: String,
    pub deprecated: Option<bool>,
    #[allow(unused)]
    pub deprecation_reason: Option<String>,
}

pub struct AvailablePlugin {
    pub registry_id: String,
    pub plugin_id: String,
    pub user_id: String,
    pub version: Version,
    pub permissions: PluginPermissions,
    pub environment: HashMap<String, String>,
    pub settings: Value,
}
