use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct PluginPermissions {
    #[serde(default)]
    pub core: Vec<PluginPermissionsCore>,
    #[serde(default)]
    pub job_scheduler: Vec<PluginPermissionsJobScheduler>,
    #[serde(default)]
    pub discord: PluginPermissionsDiscord,
}

#[derive(Default, Deserialize, Serialize)]
pub struct PluginPermissionsDiscord {
    pub events: Vec<PluginPermissionsDiscordEvents>,
    pub interactions: Vec<PluginPermissionsDiscordInteractions>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsCore {
    DependencyFunctions,
    Shutdown,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsJobScheduler {
    ScheduledJobs,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsDiscordEvents {
    MessageCreate,
    InteractionCreate,
    ThreadCreate,
    ThreadDelete,
    ThreadListSync,
    ThreadMemberUpdate,
    ThreadMembersUpdate,
    ThreadUpdate,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PluginPermissionsDiscordInteractions {
    ApplicationCommands,
    MessageComponents,
    Modals,
}
