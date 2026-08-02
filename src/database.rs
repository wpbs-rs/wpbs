/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{fs, io::ErrorKind, path::Path};

use anyhow::{Result, bail};
use fjall::{Database, KeyspaceCreateOptions};
use tracing::info;

// Plugins: K: &str (stack:plugin); V: &Bytes (Uuid)
//
// PluginStore: K: &str (Uuid:String); V: &[u8]
//
// DependencyFunctions: K: &str (namespace_id:plugin_id:function_id@version); V: &Bytes (Uuid)
//
// DiscordEvents: K: &str (DiscordEventKinds:Uuid); V: &Bytes Uuid
// DiscordApplicationCommands: 1) K: &str (Uuid:Uuid); V: &Bytes (Uuid); 2) K: &[u8; 8]; V: &Bytes (Uuid)
// DiscordMessageComponents: K: &Bytes (Uuid); V: &Bytes (Uuid)
// DiscordModals: K: &Bytes (Uuid); V: &Bytes (Uuid)

pub fn new(database_directory_path: &Path) -> Result<Database> {
    info!("Opening or creating the database");

    if let Err(err) = fs::create_dir_all(database_directory_path)
        && err.kind() != ErrorKind::AlreadyExists
    {
        bail!(err);
    }

    let database = Database::builder(database_directory_path).open()?;

    database
        .keyspace("dependency_functions", KeyspaceCreateOptions::default)?
        .clear()?;

    Ok(database)
}
