/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{fs, path::Path};

use anyhow::{Result, bail};
use indexmap::IndexMap;
use serde::Deserialize;
use tracing::info;

use crate::config::plugins::ConfigPlugin;

pub mod plugins;

#[derive(Deserialize)]
pub struct Config {
    #[allow(unused)] // Will be used when multi discord bot client support gets added
    pub name: String,
    pub plugins: IndexMap<String, ConfigPlugin>,
}

impl Config {
    pub fn new(file_path: &Path) -> Result<Self> {
        info!("Loading and parsing the config file");

        let file_bytes = match fs::read(file_path) {
            Ok(file_bytes) => file_bytes,
            Err(err) => {
                bail!("An error occurred while trying to read the config file: {err}");
            }
        };

        match serde_yaml_ng::from_slice::<Config>(&file_bytes) {
            Ok(config) => Ok(config), // TODO: Env var interpolation
            Err(err) => {
                bail!(
                    "An error occurred while trying to deserialize the config file YAML to a struct: {err}"
                );
            }
        }
    }
}
