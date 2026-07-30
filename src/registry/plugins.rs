/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::collections::HashMap;

use semver::Version;
use wasm_pkg_client::ContentDigest;
use yaml_serde::Value;

use crate::config::plugins::permissions::PluginPermissions;

pub struct AvailablePlugin {
    pub namespace_id: String,
    pub plugin_id: String,
    pub version: Version,
    pub content_digest: Option<ContentDigest>,
    pub user_id: String,
    pub permissions: PluginPermissions,
    pub environment: HashMap<String, String>,
    pub settings: Value,
}
