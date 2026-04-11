use std::collections::HashMap;

use serde::Deserialize;
use sonic_rs::Value;

use crate::config::plugins::permissions::PluginPermissions;

pub mod permissions;

#[derive(Deserialize)]
pub struct ConfigPlugin {
    pub plugin: String,
    pub cache: Option<bool>,
    pub permissions: PluginPermissions,
    #[serde(default)]
    pub environment: HashMap<String, String>,
    #[serde(default)]
    pub settings: Value,
}
