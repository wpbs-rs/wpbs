/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    fs::{self},
    io::ErrorKind,
    path::Path,
};

use anyhow::{Result, bail};
use fjall::{Database, KeyspaceCreateOptions, PersistMode, Slice};

use crate::utils::channels::DatabaseMessages;

pub enum Keyspaces {
    Plugins,
    PluginStore,
    DependencyFunctions,
    ScheduledJobs,
    DiscordEvents,
    DiscordApplicationCommands,
    DiscordMessageComponents,
    DiscordModals,
}

pub fn new(database_directory_path: &Path) -> Result<Database> {
    if let Err(err) = fs::create_dir_all(database_directory_path)
        && err.kind() != ErrorKind::AlreadyExists
    {
        bail!(err);
    }

    Ok(Database::builder(database_directory_path).open()?)
}

pub fn handle_action(database: Database, message: DatabaseMessages) {
    match message {
        DatabaseMessages::GetState(keyspace, key, response_sender) => {
            response_sender.send(get(database, keyspace, key));
        }
        DatabaseMessages::InsertState(keyspace, key, value, response_sender) => {
            response_sender.send(insert(database, keyspace, key, value));
        }
        DatabaseMessages::DeleteState(keyspace, key, response_sender) => {
            response_sender.send(remove(database, keyspace, key));
        }
        DatabaseMessages::ContainsKey(keyspace, key, response_sender) => {
            response_sender.send(contains_key(database, keyspace, key));
        }
    }
}

pub fn get(database: Database, keyspace: Keyspaces, key: Vec<u8>) -> Result<Option<Slice>> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.get(key)?)
}

pub fn insert(database: Database, keyspace: Keyspaces, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.insert(key, value)?)
}

pub fn remove(database: Database, keyspace: Keyspaces, key: Vec<u8>) -> Result<()> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.remove(key)?)
}

pub fn contains_key(database: Database, keyspace: Keyspaces, key: Vec<u8>) -> Result<bool> {
    let keyspace = database.keyspace(get_keyspace(keyspace), KeyspaceCreateOptions::default)?;

    Ok(keyspace.contains_key(key)?)
}

pub fn persist(database: Database, persist_mode: PersistMode) -> Result<()> {
    Ok(database.persist(persist_mode)?)
}

fn get_keyspace(keyspace: Keyspaces) -> &'static str {
    match keyspace {
        Keyspaces::Plugins => "plugins",
        Keyspaces::PluginStore => "plugin_store",
        Keyspaces::DependencyFunctions => "dependency_functions",
        Keyspaces::ScheduledJobs => "scheduled_jobs",
        Keyspaces::DiscordEvents => "discord_events",
        Keyspaces::DiscordApplicationCommands => "discord_application_commands",
        Keyspaces::DiscordMessageComponents => "discord_message_componets",
        Keyspaces::DiscordModals => "discord_modals",
    }
}
