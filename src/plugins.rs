/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

pub mod builder;
pub mod permissions;
pub mod registry;
pub mod runtime;

use std::collections::{HashMap, HashSet};

use semver::Version;
use serde::Deserialize;
use serde_yaml_ng::Value;
use twilight_model::id::{Id, marker::CommandMarker};

use crate::plugins::permissions::ConfigPluginPermissions;

wasmtime::component::bindgen!({ imports: { default: async }, exports: { default: async } });

#[derive(Deserialize)]
pub struct ConfigPlugin {
    pub plugin: String,
    pub cache: Option<bool>,
    pub permissions: ConfigPluginPermissions,
    pub environment: Option<HashMap<String, String>>,
    pub settings: Option<Value>,
}

pub struct AvailablePlugin {
    pub registry_id: String,
    pub id: String,
    pub user_id: String,
    pub version: Version,
    pub permissions: ConfigPluginPermissions,
    pub environment: Option<HashMap<String, String>>,
    pub settings: Option<Value>,
}

// TODO: Plugins which did not register anything should get dropped
pub struct PluginRegistrations {
    pub discord_events: PluginRegistrationsDiscordEvents,
    pub scheduled_jobs: HashMap<u128, (String, String)>, // UUID, plugin ID, internal ID
    pub dependency_functions: HashMap<String, HashSet<String>>,
}

pub struct PluginRegistrationsDiscordEvents {
    pub interaction_create: PluginRegistrationsInteractionCreate,
    pub message_create: Vec<String>,
    pub thread_create: Vec<String>,
    pub thread_delete: Vec<String>,
    pub thread_list_sync: Vec<String>,
    pub thread_member_update: Vec<String>,
    pub thread_members_update: Vec<String>,
    pub thread_update: Vec<String>,
}

pub struct PluginRegistrationsInteractionCreate {
    pub application_commands: HashMap<Id<CommandMarker>, String>, // Command ID, plugin ID
    pub message_components: HashMap<String, String>, // Message Component ID, plugin ID ISSUE: ID overlap is possible
    pub modals: HashMap<String, String>, // Modal ID, plugin ID ISSUE: ID overlap is possible
}

pub struct PluginRegistrationRequests {
    pub discord_event_interaction_create: PluginRegistrationRequestsInteractionCreate,
    pub scheduled_jobs: Vec<PluginRegistrationRequestsScheduledJob>,
}

pub struct PluginRegistrationRequestsInteractionCreate {
    pub application_commands: Vec<PluginRegistrationRequestsApplicationCommand>,
    #[allow(unused)] // Will be used when wasmtime provides component information on host calls
    pub message_component: Vec<PluginRegistrationRequestsMessageComponent>,
    #[allow(unused)] // Will be used when wasmtime provides component information on host calls
    pub modals: Vec<PluginRegistrationRequestsModal>,
}

pub struct PluginRegistrationRequestsApplicationCommand {
    pub plugin_id: String,
    pub data: Vec<u8>,
}

#[allow(unused)] // Will be used when wasmtime provides component information on host calls
pub struct PluginRegistrationRequestsMessageComponent {
    pub plugin_id: String,
    pub id: String,
}

#[allow(unused)] // Will be used when wasmtime provides component information on host calls
pub struct PluginRegistrationRequestsModal {
    pub plugin_id: String,
    pub id: String,
}

pub struct PluginRegistrationRequestsScheduledJob {
    pub plugin_id: String,
    pub id: String,
    pub crons: Vec<String>,
}

impl PluginRegistrations {
    pub fn new() -> Self {
        PluginRegistrations {
            discord_events: PluginRegistrationsDiscordEvents {
                interaction_create: PluginRegistrationsInteractionCreate {
                    application_commands: HashMap::new(),
                    message_components: HashMap::new(),
                    modals: HashMap::new(),
                },
                message_create: vec![],
                thread_create: vec![],
                thread_delete: vec![],
                thread_list_sync: vec![],
                thread_member_update: vec![],
                thread_members_update: vec![],
                thread_update: vec![],
            },
            scheduled_jobs: HashMap::new(),
            dependency_functions: HashMap::new(),
        }
    }
}
