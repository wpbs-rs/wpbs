/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

pub mod registry;

use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use tracing::info;

pub struct HttpClient {
    client: Client,
}

static USER_AGENT: &str = "wpbs-rs/wpbs";

impl HttpClient {
    pub fn new(http_client_timeout_seconds: u64) -> Result<Self> {
        info!("Creating the HTTP client");

        let client = Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(http_client_timeout_seconds))
            .build()?;

        Ok(HttpClient { client })
    }
}
