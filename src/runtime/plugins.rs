use crate::{
    config::plugins::permissions::{
        PluginPermissionsCore, PluginPermissionsDiscordEvents,
        PluginPermissionsDiscordInteractions, PluginPermissionsJobScheduler,
    },
    runtime::plugins::wpbs::plugin::{
        core_import_types::SupportedCoreRegistrations,
        discord_import_types::{DiscordEvents, SupportedDiscordRegistrationsInteractions},
        job_scheduler_import_types::SupportedJobSchedulerRegistrations,
    },
};

wasmtime::component::bindgen!({ imports: { default: async }, exports: { default: async } });

impl From<Vec<PluginPermissionsCore>> for SupportedCoreRegistrations {
    fn from(plugin_permissions_core: Vec<PluginPermissionsCore>) -> Self {
        let mut supported_core_registrations = Self::empty();

        for plugin_permission_core in &plugin_permissions_core {
            match plugin_permission_core {
                PluginPermissionsCore::DependencyFunctions => {
                    supported_core_registrations &= Self::DEPENDENCY_FUNCTIONS;
                }
                PluginPermissionsCore::Shutdown => {
                    supported_core_registrations &= Self::SHUTDOWN;
                }
            }
        }

        supported_core_registrations
    }
}

impl From<Vec<PluginPermissionsJobScheduler>> for SupportedJobSchedulerRegistrations {
    fn from(plugin_permissions_job_scheduler: Vec<PluginPermissionsJobScheduler>) -> Self {
        let mut supported_job_scheduler_registrations = Self::empty();

        for plugin_permission_job_scheduler in &plugin_permissions_job_scheduler {
            match plugin_permission_job_scheduler {
                PluginPermissionsJobScheduler::ScheduledJobs => {
                    supported_job_scheduler_registrations &= Self::SCHEDULED_JOBS;
                }
            }
        }

        supported_job_scheduler_registrations
    }
}

// The `DiscordEvents` flags is retyped several times in the plugin API.
impl From<Vec<PluginPermissionsDiscordEvents>> for DiscordEvents {
    fn from(plugin_permissions_discord_events: Vec<PluginPermissionsDiscordEvents>) -> Self {
        let mut supported_discord_registrations_events = Self::empty();

        for plugin_permission_discord_events in &plugin_permissions_discord_events {
            match plugin_permission_discord_events {
                PluginPermissionsDiscordEvents::MessageCreate => {
                    supported_discord_registrations_events &= Self::MESSAGE_CREATE;
                }
                PluginPermissionsDiscordEvents::InteractionCreate => {
                    supported_discord_registrations_events &= Self::INTERACTION_CREATE;
                }
                PluginPermissionsDiscordEvents::ThreadCreate => {
                    supported_discord_registrations_events &= Self::THREAD_CREATE;
                }
                PluginPermissionsDiscordEvents::ThreadDelete => {
                    supported_discord_registrations_events &= Self::THREAD_DELETE;
                }
                PluginPermissionsDiscordEvents::ThreadListSync => {
                    supported_discord_registrations_events &= Self::THREAD_LIST_SYNC;
                }
                PluginPermissionsDiscordEvents::ThreadMemberUpdate => {
                    supported_discord_registrations_events &= Self::THREAD_MEMBER_UPDATE;
                }
                PluginPermissionsDiscordEvents::ThreadMembersUpdate => {
                    supported_discord_registrations_events &= Self::THREAD_MEMBERS_UPDATE;
                }
                PluginPermissionsDiscordEvents::ThreadUpdate => {
                    supported_discord_registrations_events &= Self::THREAD_UPDATE;
                }
            }
        }

        supported_discord_registrations_events
    }
}

impl From<DiscordEvents> for Vec<PluginPermissionsDiscordEvents> {
    fn from(requested_discord_registrations: DiscordEvents) -> Self {
        let mut plugin_permissions_discord_events = Vec::new();

        if requested_discord_registrations.contains(DiscordEvents::MESSAGE_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::MessageCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::INTERACTION_CREATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::InteractionCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_CREATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadCreate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_DELETE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadDelete);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_LIST_SYNC) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadListSync);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_MEMBER_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMemberUpdate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_MEMBERS_UPDATE) {
            plugin_permissions_discord_events
                .push(PluginPermissionsDiscordEvents::ThreadMembersUpdate);
        }

        if requested_discord_registrations.contains(DiscordEvents::THREAD_UPDATE) {
            plugin_permissions_discord_events.push(PluginPermissionsDiscordEvents::ThreadUpdate);
        }

        plugin_permissions_discord_events
    }
}

impl From<Vec<PluginPermissionsDiscordInteractions>> for SupportedDiscordRegistrationsInteractions {
    fn from(
        plugin_permissions_discord_interactions: Vec<PluginPermissionsDiscordInteractions>,
    ) -> Self {
        let mut supported_discord_registrations_interactions = Self::empty();

        for plugin_permission_discord_interactions in &plugin_permissions_discord_interactions {
            match plugin_permission_discord_interactions {
                PluginPermissionsDiscordInteractions::ApplicationCommands => {
                    supported_discord_registrations_interactions &= Self::APPLICATION_COMMANDS;
                }
                PluginPermissionsDiscordInteractions::MessageComponents => {
                    supported_discord_registrations_interactions &= Self::MESSAGE_COMPONENTS;
                }
                PluginPermissionsDiscordInteractions::Modals => {
                    supported_discord_registrations_interactions &= Self::MODALS;
                }
            }
        }

        supported_discord_registrations_interactions
    }
}
