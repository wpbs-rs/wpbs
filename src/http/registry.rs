/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use tracing::debug;
use url::{ParseError, Url};

use crate::http::HttpClient;

impl HttpClient {
    pub async fn get_file_from_registry(&self, registry: &str, path: &str) -> Result<Vec<u8>> {
        let url = Self::parse_url(registry, path).context("An error occurred while trying to construct a valid URL from the provided registry and path")?;

        debug!("Requested registry file: {url}");

        let response = self.client.get(url).send().await?;

        if response.status() != StatusCode::OK {
            bail!(
                "The response was undesired, status code: {}",
                response.status()
            );
        }

        Ok(response
            .bytes()
            .await
            .context("Something went wrong while getting the raw bytes from the response")?
            .to_vec())
    }

    fn parse_url(registry: &str, path: &str) -> Result<Url, ParseError> {
        Url::from_str(&format!("https://{registry}/"))?.join(path)
    }
}
