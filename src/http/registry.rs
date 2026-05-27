/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::str::FromStr;

use anyhow::{Result, bail};
use reqwest::StatusCode;
use tracing::debug;
use url::{ParseError, Url};

use crate::http::HttpClient;

impl HttpClient {
    pub async fn get_file_from_registry(&self, registry: &str, path: &str) -> Result<Vec<u8>> {
        let url = Self::parse_url(registry, path)?;

        debug!("Requested registry file: {url}");

        let response = self.client.get(url).send().await?;

        if response.status() != StatusCode::OK {
            bail!(
                "The response was undesired, status code: {}",
                response.status()
            );
        }

        Ok(response.bytes().await?.into())
    }

    fn parse_url(registry: &str, path: &str) -> Result<Url, ParseError> {
        Url::from_str(&format!("https://{registry}/"))?.join(path)
    }
}
