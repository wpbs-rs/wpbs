/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    fs::{self},
    io::ErrorKind,
    path::Path,
};

use anyhow::{Result, bail};
use fjall::{Database, Guard, Iter, KeyspaceCreateOptions, PersistMode, Slice};

use crate::utils::channels::DatabaseMessages;

pub enum Keyspaces {
    PluginStore, // K: String (Uuid:String); V: Vec<u8>

    DependencyFunctions, // K: String (registry_id/plugin_id/function_id); V: Uuid

    DiscordEvents,              // K: String; V: Vec<Uuid>
    DiscordApplicationCommands, // 1) K: String (Uuid-String); V: Command; 2) K: String (u64); V: Uuid
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

pub fn handle_action(database: &Database, message: DatabaseMessages) {
    match message {
        DatabaseMessages::Get(keyspace, key, response_sender) => {
            response_sender.send(get(database, &keyspace, key)).unwrap();
        }
        DatabaseMessages::Range(keyspace, range_start, range_end, inclusive, response_sender) => {
            let _ = response_sender.send(range(
                database,
                &keyspace,
                range_start,
                range_end,
                inclusive,
            ));
        }
        DatabaseMessages::Prefix(keyspace, prefix_value, response_sender) => {
            let _ = response_sender.send(prefix(database, &keyspace, prefix_value));
        }
        DatabaseMessages::GetAllEntries(keyspace, response_sender) => {
            response_sender
                .send(get_all_entries(database, &keyspace))
                .unwrap();
        }
        DatabaseMessages::GetAllKeys(keyspace, response_sender) => {
            response_sender
                .send(get_all_keys(database, &keyspace))
                .unwrap();
        }
        DatabaseMessages::GetAllValues(keyspace, response_sender) => {
            response_sender
                .send(get_all_values(database, &keyspace))
                .unwrap();
        }
        DatabaseMessages::Insert(keyspace, key, value, response_sender) => {
            response_sender
                .send(insert(database, &keyspace, key, value))
                .unwrap();
        }
        DatabaseMessages::Remove(keyspace, key, response_sender) => {
            response_sender
                .send(remove(database, &keyspace, key))
                .unwrap();
        }
        DatabaseMessages::ContainsKey(keyspace, key, response_sender) => {
            response_sender
                .send(contains_key(database, &keyspace, key))
                .unwrap();
        }
        DatabaseMessages::Clear(keyspace, response_sender) => {
            response_sender.send(clear(database, &keyspace)).unwrap();
        }
    }
}

pub fn get(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<Option<Slice>> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.get(key)?)
}

pub fn range(
    database: &Database,
    keyspace: &Keyspaces,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    inclusive: bool,
) -> Result<Iter> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    if inclusive {
        return Ok(keyspace.range(range_start..=range_end));
    }

    Ok(keyspace.range(range_start..range_end))
}

pub fn prefix(database: &Database, keyspace: &Keyspaces, prefix: Vec<u8>) -> Result<Iter> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.prefix(prefix))
}

pub fn get_all_entries(database: &Database, keyspace: &Keyspaces) -> Result<Vec<(Slice, Slice)>> {
    Ok(prefix(database, keyspace, Vec::new())?
        .map(Guard::into_inner)
        .collect::<Result<Vec<(Slice, Slice)>, fjall::Error>>()?)
}

pub fn get_all_keys(database: &Database, keyspace: &Keyspaces) -> Result<Vec<Slice>> {
    Ok(prefix(database, keyspace, Vec::new())?
        .map(Guard::key)
        .collect::<std::result::Result<Vec<Slice>, fjall::Error>>()?)
}

pub fn get_all_values(database: &Database, keyspace: &Keyspaces) -> Result<Vec<Slice>> {
    Ok(range(database, keyspace, Vec::new(), Vec::new(), true)?
        .map(Guard::value)
        .collect::<std::result::Result<Vec<Slice>, fjall::Error>>()?)
}

pub fn insert(
    database: &Database,
    keyspace: &Keyspaces,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.insert(key, value)?)
}

pub fn remove(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.remove(key)?)
}

pub fn contains_key(database: &Database, keyspace: &Keyspaces, key: Vec<u8>) -> Result<bool> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.contains_key(key)?)
}

pub fn clear(database: &Database, keyspace: &Keyspaces) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.clear()?)
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
        Keyspaces::DiscordMessageComponents => "discord_message_componets",
        Keyspaces::DiscordModals => "discord_modals",
    }
}
