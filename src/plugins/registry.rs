/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use anyhow::{Result, bail};
use semver::{Version, VersionReq};
use serde::Deserialize;
use tokio::fs;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    http::HttpClient,
    plugins::{AvailablePlugin, ConfigPlugin},
};

#[derive(Deserialize)]
#[allow(unused)]
pub struct Registry {
    pub name: String,
    pub description: String,
    pub maintainers: Vec<String>,
    pub plugins: BTreeMap<String, RegistryPlugin>,
}

#[derive(Deserialize)]
#[allow(unused)]
pub struct RegistryPlugin {
    pub versions: Vec<RegistryPluginVersion>,
    pub description: String,
}

#[derive(Deserialize)]
#[allow(unused)]
pub struct RegistryPluginVersion {
    pub version: String,
    pub release_time: String,
    pub compatible_program_version: String,
    pub deprecated: Option<bool>,
    pub deprecation_reason: Option<String>,
}

type RegistryTask = Vec<tokio::task::JoinHandle<Result<Vec<(Uuid, AvailablePlugin)>>>>;

static DEFAULT_REGISTRY_ID: &str =
    "raw.githubusercontent.com/celarye/discord-bot-plugins/refs/heads/master";

static PROGRAM_VERSION: LazyLock<Version> =
    LazyLock::new(|| Version::parse(env!("CARGO_PKG_VERSION")).unwrap());

pub async fn registry_get_plugins(
    http_client_timeout_seconds: u64,
    config: Config,
    plugin_directory: PathBuf,
    cache: bool,
) -> Result<Vec<(Uuid, AvailablePlugin)>, ()> {
    let http_client = Arc::new(HttpClient::new(http_client_timeout_seconds)?);

    get_plugins(http_client, config, plugin_directory, cache).await
}

pub async fn get_plugins(
    http_client: Arc<HttpClient>,
    config: Config,
    base_plugin_directory_path: PathBuf,
    cache: bool,
) -> Result<Vec<(Uuid, AvailablePlugin)>, ()> {
    info!("Fetching and storing the plugins");

    let mut available_plugins = vec![];

    let registries = get_cached_plugins(
        &base_plugin_directory_path,
        config,
        cache,
        &mut available_plugins,
    )
    .await;

    fetch_non_cached_plugins(
        http_client,
        &base_plugin_directory_path,
        registries,
        &mut available_plugins,
    )
    .await;

    if available_plugins.is_empty() {
        warn!("No plugins are available for the runtime");
        return Err(());
    }

    Ok(available_plugins)
}

async fn get_cached_plugins(
    base_plugin_directory_path: &Path,
    config: Config,
    cache: bool,
    available_plugins: &mut Vec<(Uuid, AvailablePlugin)>,
) -> HashMap<String, Vec<(String, ConfigPlugin)>> {
    let mut registries = HashMap::new();

    for (plugin_uid, plugin_options) in config.plugins {
        let (plugin_string, plugin_requested_version) =
            parse_plugin_string_requested_version(&plugin_options.plugin);
        let (registry_id, plugin_id) = parse_plugin_string_registry_id(plugin_string);

        if plugin_options.cache.unwrap_or(cache) {
            match check_plugin_cache(
                base_plugin_directory_path,
                registry_id,
                plugin_id,
                plugin_requested_version,
            )
            .await
            {
                Ok(cache_check) => {
                    if let Some(plugin_version) = cache_check {
                        available_plugins.push((
                            Uuid::new_v4(),
                            AvailablePlugin {
                                registry_id: registry_id.to_string(),
                                id: plugin_id.to_string(),
                                user_id: plugin_uid,
                                version: plugin_version,
                                permissions: plugin_options.permissions,
                                environment: plugin_options.environment,
                                settings: plugin_options.settings,
                            },
                        ));

                        continue;
                    }
                }
                Err(err) => {
                    error!("An error occurred while checking if the {plugin_uid} is cached: {err}");
                }
            }
        }

        registries
            .entry(registry_id.to_string())
            .or_insert(vec![])
            .push((plugin_uid, plugin_options));
    }

    registries
}

async fn fetch_non_cached_plugins(
    http_client: Arc<HttpClient>,
    base_plugin_directory_path: &Path,
    registries: HashMap<String, Vec<(String, ConfigPlugin)>>,
    available_plugins: &mut Vec<(Uuid, AvailablePlugin)>,
) {
    let mut registry_tasks: RegistryTask = vec![];

    for (registry_id, plugins) in registries {
        let http_client = http_client.clone();
        let registry_directory_path = base_plugin_directory_path.join(&registry_id);
        let registry_id = Arc::new(registry_id);

        registry_tasks.push(tokio::spawn(async move {
            let mut available_registry_plugins = vec![];

            let registry = Arc::new(fetch_registry(http_client.clone(), &registry_id, &registry_directory_path).await?);

            let mut plugin_tasks = vec![];

            for (plugin_uid, plugin_options) in plugins {
                let http_client = http_client.clone();
                let registry_directory_path = registry_directory_path.clone();
                let registry = registry.clone();

                plugin_tasks.push(tokio::spawn(async move {
                    let (plugin_string, plugin_requested_version) =
                        parse_plugin_string_requested_version(&plugin_options.plugin);
                    let (registry_id, plugin_id) = parse_plugin_string_registry_id(plugin_string);

                    let mut plugin_directory_path = registry_directory_path.join(plugin_id);

                    let Some(registry_plugin) = registry.plugins.get(plugin_id) else {
                        bail!("The {registry_id} registry has no {plugin_id} plugin entry",
                        );
                    };

                    let Some(plugin_version) = get_plugin_matching_version(
                        plugin_requested_version,
                        &registry_plugin.versions,
                    )?
                    else {
                        bail!(
                        "The {plugin_uid} plugin has no version which isn't marked as deprecated and is compatible with this version of the program");
                    };

                    plugin_directory_path.push(plugin_version.to_string());

                    let plugin_url_segment = format!("{plugin_id}/{plugin_version}/");

                    fetch_plugin(
                        http_client,
                        registry_id,
                        plugin_id,
                        &plugin_url_segment,
                        &plugin_directory_path,
                    )
                    .await?;

                    Ok((
                        Uuid::new_v4(),
                        AvailablePlugin {
                            registry_id: registry_id.to_string(),
                            id: plugin_id.to_string(),
                            user_id: plugin_uid,
                            version: plugin_version,
                            permissions: plugin_options.permissions,
                            environment: plugin_options.environment,
                            settings: plugin_options.settings,
                        },
                    ))
                }));
            }

            for plugin_task in plugin_tasks {
                match plugin_task.await.unwrap() {
                    Ok(available_plugin) => available_registry_plugins.push(available_plugin),
                    Err(err) => error!("An error occurred while fetching a plugin from the {registry_id} registry: {err}")
                }
            }

            Ok(available_registry_plugins)
        }));
    }

    for registry_task in registry_tasks {
        match registry_task.await.unwrap() {
            Ok(available_registry_plugins) => {
                for available_registry_plugin in available_registry_plugins {
                    available_plugins.push(available_registry_plugin);
                }
            }
            Err(err) => {
                error!("An error occurred while fetching a registry: {err}");
            }
        }
    }
}

fn parse_plugin_string_registry_id(value: &str) -> (&str, &str) {
    match value.rsplit_once('/') {
        Some((registry_id, plugin_string)) => (registry_id, plugin_string),
        None => (DEFAULT_REGISTRY_ID, value),
    }
}

fn parse_plugin_string_requested_version(value: &str) -> (&str, &str) {
    match value.rsplit_once(':') {
        Some((plugin_string, plugin_requested_version)) => {
            (plugin_string, plugin_requested_version)
        }
        None => (value, "latest"),
    }
}

async fn check_plugin_cache(
    base_plugin_directory: &Path,
    registry_id: &str,
    plugin_id: &str,
    plugin_requested_version: &str,
) -> Result<Option<Version>> {
    let mut plugin_path = base_plugin_directory.join(registry_id);
    plugin_path.push(plugin_id);

    let plugin_version = if plugin_requested_version == "latest" {
        let Some(plugin_version) = get_plugin_latest_cached_version(&plugin_path).await? else {
            return Ok(None);
        };

        plugin_path.push(plugin_version.to_string());
        plugin_path.push("plugin.wasm");

        plugin_version
    } else {
        let plugin_version = Version::parse(plugin_requested_version)?;

        plugin_path.push(plugin_requested_version);
        plugin_path.push("plugin.wasm");

        plugin_version
    };

    if fs::try_exists(plugin_path).await? {
        return Ok(Some(plugin_version));
    }

    Ok(None)
}

async fn get_plugin_latest_cached_version(plugin_path: &Path) -> Result<Option<Version>> {
    let mut plugin_latest_version = None;

    let mut plugin_cached_dir = match fs::read_dir(plugin_path).await {
        Ok(plugin_cached_dir) => plugin_cached_dir,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                return Ok(None);
            }

            bail!(err);
        }
    };

    while let Some(plugin_cached_version) = plugin_cached_dir.next_entry().await? {
        if plugin_cached_version.file_type().await?.is_dir()
            && let Some(plugin_cached_version_file_name) =
                plugin_cached_version.file_name().to_str()
        {
            let Ok(plugin_cached_version) = Version::parse(plugin_cached_version_file_name) else {
                continue;
            };

            if &plugin_cached_version
                > plugin_latest_version
                    .as_ref()
                    .unwrap_or(&Version::new(0, 0, 0))
            {
                plugin_latest_version = Some(plugin_cached_version);
            }
        }
    }

    Ok(plugin_latest_version)
}

fn get_plugin_matching_version(
    requested_version: &str,
    plugin_versions: &[RegistryPluginVersion],
) -> Result<Option<Version>> {
    if requested_version == "latest" {
        let mut plugin_latest_version = None;

        for plugin_version in plugin_versions {
            let plugin_version_version = Version::parse(&plugin_version.version)?;

            if check_plugin_version_usability(plugin_version)?
                && &plugin_version_version
                    > plugin_latest_version
                        .as_ref()
                        .unwrap_or(&Version::new(0, 0, 0))
            {
                plugin_latest_version = Some(plugin_version_version);
            }
        }

        return Ok(plugin_latest_version);
    } else if let Some(plugin_version) = plugin_versions
        .iter()
        .find(|v| v.version == requested_version)
        && check_plugin_version_usability(plugin_version)?
    {
        return Ok(Some(Version::parse(&plugin_version.version)?));
    }

    Ok(None)
}

fn check_plugin_version_usability(plugin_version: &RegistryPluginVersion) -> Result<bool> {
    if let Some(deprecated) = plugin_version.deprecated
        && deprecated
    {
        return Ok(false);
    }

    let plugin_compatible_program_version =
        VersionReq::parse(&plugin_version.compatible_program_version)?;

    if !plugin_compatible_program_version.matches(&PROGRAM_VERSION) {
        return Ok(false);
    }

    Ok(true)
}

async fn fetch_registry(
    http_client: Arc<HttpClient>,
    registry_id: &str,
    registry_directory_path: &Path,
) -> Result<Registry> {
    info!("Fetching the {registry_id} registry");

    let registry_metadata_bytes = http_client
        .get_file_from_registry(registry_id, "plugins.json")
        .await?;

    fs::create_dir_all(registry_directory_path).await?;

    fs::write(
        registry_directory_path.join("plugins.json"),
        &registry_metadata_bytes,
    )
    .await?;

    Ok(sonic_rs::from_slice::<Registry>(&registry_metadata_bytes)?)
}

async fn fetch_plugin(
    http_client: Arc<HttpClient>,
    registry_id: &str,
    plugin_id: &str,
    plugin_url_segment: &str,
    plugin_directory_path: &Path,
) -> Result<()> {
    info!("Fetching the {plugin_id} plugin from its registry");

    let plugin_metadata_bytes = http_client
        .get_file_from_registry(registry_id, &(format!("{plugin_url_segment}metadata.json")))
        .await?;

    fs::create_dir_all(plugin_directory_path).await?;

    fs::write(
        plugin_directory_path.join("metadata.json"),
        &plugin_metadata_bytes,
    )
    .await?;

    let plugin_bytes = http_client
        .get_file_from_registry(registry_id, &(format!("{plugin_url_segment}plugin.wasm")))
        .await?;

    fs::write(plugin_directory_path.join("plugin.wasm"), &plugin_bytes).await?;

    Ok(())
}
