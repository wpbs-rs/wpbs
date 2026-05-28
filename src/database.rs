/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    fs::{self},
    io::ErrorKind,
    path::Path,
};

use crate::utils::channels::DatabaseMessages;
use anyhow::{Result, bail};
use fjall::{Database, Guard, Iter, KeyspaceCreateOptions, PersistMode, Slice};
use tokio::task::spawn_blocking;

pub enum Keyspaces {
    PluginStore, // K: String (Uuid:String); V: Vec<u8>

    DependencyFunctions, // K: String (registry_id/plugin_id/function_id:version); V: Uuid

    DiscordEvents,              // K: String (DiscordEventKinds:Uuid); V: Uuid
    DiscordApplicationCommands, // 1) K: String (Uuid:Uuid); V: Vec<u8>; 2) K: String (u64); V: Uuid
    DiscordMessageComponents,   // K: Uuid; V: Uuid
    DiscordModals,              // K: Uuid; V: Uuid
}

pub fn new(database_directory_path: &Path) -> Result<Database> {
    if let Err(err) = fs::create_dir_all(database_directory_path)
        && err.kind() != ErrorKind::AlreadyExists
    {
        bail!(err);
    }

    Ok(Database::builder(database_directory_path).open()?)
}

pub async fn handle_action(database: &Database, message: DatabaseMessages) {
    match message {
        DatabaseMessages::Get(keyspace, key, sender) => {
            let _ = sender.send(get(database, &keyspace, key).await);
        }
        DatabaseMessages::Range(keyspace, range_start, range_end, inclusive, sender) => {
            let _ =
                sender.send(range(database, &keyspace, range_start, range_end, inclusive).await);
        }
        DatabaseMessages::Prefix(keyspace, prefix_value, sender) => {
            let _ = sender.send(prefix(database, &keyspace, prefix_value).await);
        }
        DatabaseMessages::GetAllEntries(keyspace, sender) => {
            let _ = sender.send(get_all_entries(database, &keyspace).await);
        }
        DatabaseMessages::GetAllKeys(keyspace, sender) => {
            let _ = sender.send(get_all_keys(database, &keyspace).await);
        }
        DatabaseMessages::GetAllValues(keyspace, sender) => {
            let _ = sender.send(get_all_values(database, &keyspace).await);
        }
        DatabaseMessages::Insert(keyspace, key, value, sender) => {
            let _ = sender.send(insert(database, &keyspace, key, value).await);
        }
        DatabaseMessages::Remove(keyspace, key, sender) => {
            let _ = sender.send(remove(database, &keyspace, key).await);
        }
        DatabaseMessages::ContainsKey(keyspace, key, sender) => {
            let _ = sender.send(contains_key(database, &keyspace, key).await);
        }
        DatabaseMessages::Clear(keyspace, sender) => {
            let _ = sender.send(clear(database, &keyspace).await);
        }
    }
}

pub async fn get(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<Option<Slice>> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.get(key)).await.unwrap()?)
}

pub async fn range(
    database: &Database,
    keyspace: &Keyspaces,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    inclusive: bool,
) -> Result<Iter> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    if inclusive {
        return Ok(
            spawn_blocking(move || keyspace.range(range_start..=range_end))
                .await
                .unwrap(),
        );
    }

    Ok(
        spawn_blocking(move || keyspace.range(range_start..range_end))
            .await
            .unwrap(),
    )
}

pub async fn prefix(database: &Database, keyspace: &Keyspaces, prefix: Vec<u8>) -> Result<Iter> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.prefix(prefix))
        .await
        .unwrap())
}

pub async fn get_all_entries(
    database: &Database,
    keyspace: &Keyspaces,
) -> Result<Vec<(Slice, Slice)>> {
    Ok(prefix(database, keyspace, Vec::new())
        .await?
        .map(Guard::into_inner)
        .collect::<Result<Vec<(Slice, Slice)>, fjall::Error>>()?)
}

pub async fn get_all_keys(database: &Database, keyspace: &Keyspaces) -> Result<Vec<Slice>> {
    Ok(prefix(database, keyspace, Vec::new())
        .await?
        .map(Guard::key)
        .collect::<std::result::Result<Vec<Slice>, fjall::Error>>()?)
}

pub async fn get_all_values(database: &Database, keyspace: &Keyspaces) -> Result<Vec<Slice>> {
    Ok(prefix(database, keyspace, Vec::new())
        .await?
        .map(Guard::value)
        .collect::<std::result::Result<Vec<Slice>, fjall::Error>>()?)
}

pub async fn insert(
    database: &Database,
    keyspace: &Keyspaces,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.insert(key, value))
        .await
        .unwrap()?)
}

pub async fn remove(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.remove(key))
        .await
        .unwrap()?)
}

pub async fn contains_key(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<bool> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.contains_key(key))
        .await
        .unwrap()?)
}

pub async fn clear(database: &Database, keyspace: &Keyspaces) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(spawn_blocking(move || keyspace.clear()).await.unwrap()?)
}

// TODO: Review if this should this be async
pub fn cleanup(database: &Database) -> Result<()> {
    for keyspace in [
        Keyspaces::DependencyFunctions,
        Keyspaces::DiscordEvents,
        Keyspaces::DiscordApplicationCommands,
    ] {
        let keyspace =
            database.keyspace(get_keyspace(&keyspace), KeyspaceCreateOptions::default)?;

        keyspace.clear()?;
    }

    Ok(())
}

pub fn persist(database: &Database, persist_mode: PersistMode) -> Result<()> {
    Ok(database.persist(persist_mode)?)
}

fn get_keyspace(keyspace: &Keyspaces) -> &'static str {
    match keyspace {
        Keyspaces::PluginStore => "plugin_store",
        Keyspaces::DependencyFunctions => "dependency_functions",

        Keyspaces::DiscordEvents => "discord_events",
        Keyspaces::DiscordApplicationCommands => "discord_application_commands",
        Keyspaces::DiscordMessageComponents => "discord_message_components",
        Keyspaces::DiscordModals => "discord_modals",
    }
}
