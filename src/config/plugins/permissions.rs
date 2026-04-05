use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ConfigPluginPermissions {
    #[serde(default)]
    pub core: Vec<ConfigSupportedCoreRegistrations>,
    #[serde(default)]
    pub job_scheduler: Vec<ConfigSupportedJobSchedulerRegistrations>,
    #[serde(default)]
    pub discord: Vec<ConfigSupportedDiscordRegistrations>,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ConfigSupportedCoreRegistrations {
    DependencyFunctions,
    Shutdown,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ConfigSupportedJobSchedulerRegistrations {
    ScheduledJobs,
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ConfigSupportedDiscordRegistrations {
    MessageCreate,
    InteractionCreate,
    ThreadCreate,
    ThreadDelete,
    ThreadListSync,
    ThreadMemberUpdate,
    ThreadMembersUpdate,
    ThreadUpdate,
}
