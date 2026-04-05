use crate::{
    config::plugins::permissions::{
        ConfigSupportedCoreRegistrations, ConfigSupportedDiscordRegistrations,
        ConfigSupportedJobSchedulerRegistrations,
    },
    runtime::plugins::wbps::plugin::{
        core_import_types::SupportedCoreRegistrations,
        discord_import_types::SupportedDiscordRegistrations,
        job_scheduler_import_types::SupportedJobSchedulerRegistrations,
    },
};

wasmtime::component::bindgen!({ imports: { default: async }, exports: { default: async } });

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

impl From<Vec<ConfigSupportedJobSchedulerRegistrations>> for SupportedJobSchedulerRegistrations {
    fn from(
        config_supported_job_scheduler_registrations: Vec<ConfigSupportedJobSchedulerRegistrations>,
    ) -> Self {
        let mut supported_job_scheduler_registrations = Self::empty();

        for registration in &config_supported_job_scheduler_registrations {
            todo!();
        }

        supported_job_scheduler_registrations
    }
}

impl From<Vec<ConfigSupportedDiscordRegistrations>> for SupportedDiscordRegistrations {
    fn from(
        config_supported_discord_registrations: Vec<ConfigSupportedDiscordRegistrations>,
    ) -> Self {
        let mut supported_discord_registrations = Self::empty();

        for registration in &config_supported_discord_registrations {
            todo!();
        }

        supported_discord_registrations
    }
}
