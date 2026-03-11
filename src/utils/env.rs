/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{env, path::Path};

use anyhow::{Context, Result, bail};
use dotenvy;
use tracing::{debug, info};

pub struct Secrets {
    pub discord_bot_client: String,
}

pub fn load_env_file(env_file_path: &Path) -> Result<()> {
    info!("Loading the env file");

    if let Err(err) = dotenvy::from_path(env_file_path) {
        if err.not_found() {
            debug!("No env file found at: {env_file_path:?}");
            return Ok(());
        }

        bail!("An error occurred wile trying to load the env file: {err}");
    }

    Ok(())
}

pub fn get_secrets() -> Result<Secrets> {
    info!("Validating the environment variables (DISCORD_BOT_CLIENT_TOKEN)");

    Ok(Secrets {
        discord_bot_client: env::var("DISCORD_BOT_CLIENT_TOKEN")
            .context("Failed to load the DISCORD_BOT_CLIENT_TOKEN environment variable")?,
    })
}
