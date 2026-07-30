/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

pub mod plugins;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, bail};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use futures_util::TryStreamExt;
use semver::Version;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    task::JoinHandle,
};
use tracing::{error, info};
use uuid::Uuid;
use wasm_pkg_client::{
    Client, Config as ClientConfig, PackageRef, Release,
    caching::{CachingClient, FileCache},
};

use crate::{
    config::plugins::ConfigPlugin,
    database::{Keyspaces, get_keyspace},
    registry::plugins::AvailablePlugin,
};

static DEFAULT_NAMESPACE_ID: &str = "wpbs-rs";

#[hotpath::measure]
pub async fn get_plugins(
    database: Database,
    config_name: Arc<String>,
    config_plugins: HashMap<String, ConfigPlugin>,
    plugin_directory_path: &Path,
) -> Result<Vec<(Uuid, AvailablePlugin)>> {
    info!("Getting all plugins from their respective registries");

    let caching_client =
        create_registry_client(&plugin_directory_path.join("binaries").join("remote")).await?;

    let mut available_plugins = Vec::new();

    let mut plugin_tasks: Vec<JoinHandle<Result<(Uuid, AvailablePlugin)>>> = Vec::new();

    let plugins_keyspace = database.keyspace(
        get_keyspace(&Keyspaces::Plugins),
        KeyspaceCreateOptions::default,
    )?;

    let plugin_directory_path = Arc::new(plugin_directory_path.join("binaries").join("local"));
    for (plugin_user_id, plugin_config) in config_plugins {
        let caching_client = caching_client.clone();
        let plugins_keyspace = plugins_keyspace.clone();
        let config_name = config_name.clone();
        let plugin_directory_path = plugin_directory_path.clone();

        plugin_tasks.push(tokio::spawn(async move {
            let (plugin_string, plugin_version) =
                parse_plugin_string_version(&plugin_config.plugin)?;
            let (namespace_id, plugin_id) = parse_plugin_string_namespace_id(&plugin_string);

            if namespace_id == "local" {
                get_local_plugin(plugin_directory_path, &plugin_id, &plugin_version).await?;

                let uuid = get_plugin_uuid(
                    &plugins_keyspace,
                    &namespace_id,
                    &plugin_id,
                    &plugin_version,
                    &config_name,
                    &plugin_user_id,
                )?;

                return Ok((
                    uuid,
                    AvailablePlugin {
                        namespace_id,
                        plugin_id,
                        version: plugin_version,
                        content_digest: None,
                        user_id: plugin_user_id,
                        environment: plugin_config.environment,
                        permissions: plugin_config.permissions,
                        settings: plugin_config.settings,
                    },
                ));
            }

            let release =
                fetch_plugin(caching_client, &namespace_id, &plugin_id, &plugin_version).await?;

            let uuid = get_plugin_uuid(
                &plugins_keyspace,
                &namespace_id,
                &plugin_id,
                &plugin_version,
                &config_name,
                &plugin_user_id,
            )?;

            Ok((
                uuid,
                AvailablePlugin {
                    namespace_id,
                    plugin_id,
                    version: release.version,
                    content_digest: Some(release.content_digest),
                    user_id: plugin_user_id,
                    permissions: plugin_config.permissions,
                    environment: plugin_config.environment,
                    settings: plugin_config.settings,
                },
            ))
        }));
    }

    for plugin_task in plugin_tasks {
        match plugin_task.await.unwrap() {
            Ok(available_plugin) => available_plugins.push(available_plugin),
            Err(err) => error!("An error occurred while fetching a plugin: {err}"),
        }
    }

    if available_plugins.is_empty() {
        bail!("No plugins are available for the runtime");
    }

    Ok(available_plugins)
}

async fn create_registry_client(plugin_directory_path: &Path) -> Result<CachingClient<FileCache>> {
    let config = ClientConfig::global_defaults().await?;

    let client = Client::new(config);

    let file_cache = FileCache::new(plugin_directory_path).await?;

    Ok(CachingClient::new(Some(client), file_cache))
}

fn parse_plugin_string_version(value: &str) -> Result<(String, Version)> {
    match value.rsplit_once(':') {
        Some((plugin_string, plugin_requested_version)) => Ok((
            plugin_string.to_string(),
            Version::parse(plugin_requested_version)?,
        )),
        None => bail!("A version is required"),
    }
}

fn parse_plugin_string_namespace_id(value: &str) -> (String, String) {
    match value.rsplit_once('/') {
        Some((namespace_id, plugin_string)) => {
            (namespace_id.to_string(), plugin_string.to_string())
        }
        None => (DEFAULT_NAMESPACE_ID.to_string(), value.to_string()),
    }
}

async fn get_local_plugin(
    plugin_directory_path: Arc<PathBuf>,
    plugin_id: &str,
    plugin_version: &Version,
) -> Result<()> {
    let path = plugin_directory_path
        .join(plugin_id)
        .join(plugin_version.to_string())
        .join("plugin.wasm");

    if fs::try_exists(&path).await? {
        Ok(())
    } else {
        bail!("Local {plugin_id} plugin not found");
    }
}

async fn fetch_plugin(
    client: CachingClient<FileCache>,
    namespace_id: &str,
    plugin_id: &str,
    plugin_version: &Version,
) -> Result<Release> {
    info!("Fetching the {plugin_id} plugin from its registry");

    let package_ref = PackageRef::new(namespace_id.parse()?, plugin_id.parse()?);

    let release = client.get_release(&package_ref, plugin_version).await?;

    let mut stream = client.get_content(&package_ref, &release).await?;

    let mut file = File::create("output.wasm").await?;

    while let Some(chunk) = stream.try_next().await? {
        file.write_all(&chunk).await?;
    }

    Ok(release)
}

fn get_plugin_uuid(
    plugins_keyspace: &Keyspace,
    namespace_id: &str,
    plugin_id: &str,
    plugin_version: &Version,
    config_name: &str,
    plugin_user_id: &str,
) -> Result<Uuid> {
    let key = format!("{namespace_id}:{plugin_id}@{plugin_version}:{config_name}:{plugin_user_id}");

    let uuid = if let Ok(puuid_bytes) = plugins_keyspace.get(&key)
        && let Some(uuid_bytes) = puuid_bytes
    {
        Uuid::from_slice(&uuid_bytes).unwrap()
    } else {
        Uuid::new_v4()
    };

    plugins_keyspace.insert(&key, uuid.as_bytes())?;

    Ok(uuid)
}
