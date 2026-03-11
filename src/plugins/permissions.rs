use serde::{Deserialize, Serialize};

use crate::plugins::discord_bot::plugin::core_import_types::SupportedCoreRegistrations;

#[derive(Deserialize, Serialize)]
pub struct ConfigPluginPermissions {
    pub core: Vec<ConfigSupportedCoreRegistrations>,
    pub job_scheduler: Vec<ConfigSupportedJobSchedulerRegistrations>,
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

impl From<Vec<ConfigSupportedCoreRegistrations>> for SupportedCoreRegistrations {
    fn from(config_supported_core_registrations: Vec<ConfigSupportedCoreRegistrations>) -> Self {
        let mut supported_core_registrations = Self::empty();

        for registration in &config_supported_core_registrations {
            match registration {
                ConfigSupportedCoreRegistrations::DependencyFunctions => {
                    supported_core_registrations &= SupportedCoreRegistrations::DEPENDENCY_FUNCTIONS
                }
                ConfigSupportedCoreRegistrations::Shutdown => {
                    supported_core_registrations &= SupportedCoreRegistrations::SHUTDOWN
                }
            }
        }

        supported_core_registrations
    }
}
