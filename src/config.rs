/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, fs, path::Path};

use anyhow::Result;
use serde::Deserialize;
use tracing::info;

use crate::config::{plugins::ConfigPlugin, services::ConfigServices};

pub mod plugins;
pub mod services;

#[derive(Deserialize)]
pub struct Config {
    #[allow(unused)] // Will be used when multi instance support gets added
    pub name: String,
    #[serde(default)]
    pub services: ConfigServices,
    pub plugins: HashMap<String, ConfigPlugin>,
}

impl Config {
    pub fn new(file_path: &Path) -> Result<Self> {
        info!("Loading and parsing the config file");

        let file_bytes = fs::read(file_path)?;

        // TODO: Add environment variable interpolation
        let mut config = serde_yaml_ng::from_slice::<Config>(&file_bytes)?;

        config
            .plugins
            .values_mut()
            .for_each(|p| p.permissions.calculate());

        Ok(config)
    }
}
