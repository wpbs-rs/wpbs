/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::HashMap;

use serde::Deserialize;
use serde_yaml_ng::Value;

use crate::config::plugins::permissions::PluginPermissions;

pub mod permissions;

#[derive(Deserialize)]
pub struct ConfigPlugin {
    pub plugin: String,
    pub cache: Option<bool>,
    #[serde(default)]
    pub permissions: PluginPermissions,
    #[serde(default)]
    pub environment: HashMap<String, String>,
    #[serde(default)]
    pub settings: Value,
}
