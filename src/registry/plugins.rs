/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::HashMap;

use semver::Version;
use serde::Deserialize;
use serde_yaml_ng::Value;

use crate::config::plugins::permissions::PluginPermissions;

#[derive(Deserialize)]
#[allow(unused)]
pub struct RegistryPlugin {
    pub versions: Vec<RegistryPluginVersion>,
    pub description: String,
}

#[derive(Deserialize)]
#[allow(unused)]
pub struct RegistryPluginVersion {
    pub version: String,
    pub release_time: String,
    pub compatible_program_version: String,
    pub deprecated: Option<bool>,
    pub deprecation_reason: Option<String>,
}

pub struct AvailablePlugin {
    pub registry_id: String,
    pub id: String,
    pub user_id: String,
    pub version: Version,
    pub permissions: PluginPermissions,
    pub environment: HashMap<String, String>,
    pub settings: Value,
}
