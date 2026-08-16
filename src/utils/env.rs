/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{env, path::Path};

use anyhow::{Result, bail};
use tracing::{debug, info};

use crate::config::services::ConfigServices;

pub struct Secrets {
    pub services: SecretsServices,
}

pub struct SecretsServices {
    pub discord: Option<SecretsDiscord>,
}

pub struct SecretsDiscord {
    pub bot_token: String,
}

pub fn load_env_file(env_file_path: &Path) -> Result<()> {
    info!("Loading and parsing the environment variables file");

    if let Err(err) = dotenvy::from_path(env_file_path) {
        if err.not_found() {
            debug!("No env file found at: {env_file_path:?}");
            return Ok(());
        }

        bail!("An error occurred wile trying to load the env file: {err}");
    }

    Ok(())
}

impl Secrets {
    pub fn new(config: &ConfigServices) -> Result<Self> {
        info!("Validating the environment variables");

        let discord = if config.discord.enabled {
            Some(SecretsDiscord {
                bot_token: env::var("DISCORD_BOT_TOKEN")?,
            })
        } else {
            None
        };

        Ok(Self {
            services: SecretsServices { discord },
        })
    }
}
