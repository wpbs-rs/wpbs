/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

#[cfg(target_family = "unix")]
use std::os::unix::process::CommandExt;

use std::{
    collections::VecDeque,
    env,
    ffi::OsString,
    path::Path,
    process::{Command, ExitCode, exit},
    sync::LazyLock,
};

use anyhow::Result;
use clap::Parser;
use fjall::{Database, PersistMode};
use tokio::{signal, sync::RwLock, task::JoinHandle};
use tracing::{error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;

mod cli;
mod config;
mod database;
mod http;
mod registry;
mod runtime;
mod services;
mod utils;

use cli::{Cli, CliLogParameters};
use config::Config;
use services::{discord::DiscordBotClient, job_scheduler::JobScheduler};
use utils::{channels::Channels, env::Secrets};

use crate::{
    runtime::Runtime,
    utils::channels::{
        ChannelsCore, CoreMessages, DiscordBotClientMessages, JobSchedulerMessages,
        RuntimeMessages, RuntimeMessagesCore,
    },
};

#[derive(Clone, Copy, PartialEq)]
enum Shutdown {
    Normal,
    SigInt,
    Restart,
}

static TASKS: LazyLock<RwLock<Vec<JoinHandle<()>>>> = LazyLock::new(|| RwLock::new(vec![]));
static SHUTDOWN: LazyLock<RwLock<Option<Shutdown>>> = LazyLock::new(|| RwLock::new(None));

#[tokio::main]
async fn main() -> ExitCode {
    let result = run().await;

    info!("Exiting the program");

    if result.is_ok() {
        match SHUTDOWN.read().await.as_ref().unwrap() {
            Shutdown::Normal => return ExitCode::from(0),
            Shutdown::SigInt => return ExitCode::from(130),
            Shutdown::Restart => restart(),
        }
    }

    ExitCode::from(1)
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let (_guard, secrets, channels) = initialization(cli.log_parameters, &cli.env_file)?;

    let config = Config::new(&cli.config_file)?;

    let database = database::new(&cli.database_directory)?;

    let core = start(database, channels.core);

    let available_plugins = registry::registry_get_plugins(
        cli.http_client_timeout_seconds,
        config,
        cli.plugin_directory.clone(),
        cli.cache,
    )
    .await?;

    let discord_bot_client = DiscordBotClient::new(
        secrets.discord_bot_client,
        channels.discord_bot_client.core_tx,
        channels.discord_bot_client.rx,
    )
    .await?;

    let job_scheduler =
        JobScheduler::new(channels.job_scheduler.core_tx, channels.job_scheduler.rx).await?;

    let runtime = Runtime::new(channels.runtime.rx);

    TASKS.write().await.push(job_scheduler.start().await?);

    TASKS.write().await.push(discord_bot_client.start());

    runtime
        .initialize_plugins(
            available_plugins,
            channels.runtime.core_tx,
            &cli.plugin_directory,
        )
        .await?;

    TASKS.write().await.push(runtime.start());

    tokio::spawn(async move {
        if let Err(err) = signal::ctrl_c().await {
            error!(
                "Failed to listen for the terminal interrupt signal, error: {}",
                &err
            );
            return Err(());
        }

        info!("Terminal interrupt signal received, send another to force immediate shutdown");

        tokio::spawn(async {
            signal::ctrl_c()
                .await
                .expect("failed to listen for the terminal interrupt signal");

            warn!("Second terminal interrupt signal received, forcing immediate shutdown");
            exit(130);
        });

        channels
            .core_tx
            .send(CoreMessages::Shutdown(Shutdown::SigInt))
            .unwrap();

        Ok(())
    });

    core.await
}

fn initialization(
    cli_log_parameters: CliLogParameters,
    env_file: &Path,
) -> Result<(Option<WorkerGuard>, Secrets, Channels)> {
    let guard = utils::logger::new(cli_log_parameters)?;

    utils::env::load_env_file(env_file)?;

    let secrets = utils::env::get_secrets()?;

    let channels = utils::channels::new();

    Ok((guard, secrets, channels))
}

async fn start(database: Database, mut channels_core: ChannelsCore) -> anyhow::Result<()> {
    while let Some(core_message) = channels_core.rx.recv().await {
        match core_message {
            CoreMessages::DatabaseModule(database_message) => {
                let database = database.clone();

                tokio::spawn(async {
                    database::handle_action(database, database_message);
                });
            }
            CoreMessages::JobScheduler(job_scheduler_message) => {
                channels_core
                    .job_scheduler_tx
                    .send(job_scheduler_message)
                    .unwrap();
            }
            CoreMessages::DiscordBotClient(discord_bot_client_message) => {
                channels_core
                    .discord_bot_client_tx
                    .send(discord_bot_client_message)
                    .unwrap();
            }
            CoreMessages::Runtime(runtime_message) => {
                channels_core.runtime_tx.send(runtime_message).unwrap();
            }
            CoreMessages::Shutdown(shutdown_kind) => {
                {
                    let mut shutdown_guard = SHUTDOWN.write().await;

                    let shutdown_value = shutdown_guard.get_or_insert(shutdown_kind);

                    if shutdown_kind == Shutdown::SigInt && shutdown_value != &mut Shutdown::SigInt
                    {
                        let _ = shutdown_guard.insert(shutdown_kind);
                    }
                }

                shutdown(&channels_core).await;
            }
        }
    }

    database::persist(database, PersistMode::SyncAll)
}

async fn shutdown(channels_core: &ChannelsCore) {
    let mut tasks = TASKS.write().await;

    channels_core
        .runtime_tx
        .send(RuntimeMessages::Core(RuntimeMessagesCore::Shutdown))
        .unwrap();

    tasks.pop().unwrap().await.unwrap();

    channels_core
        .discord_bot_client_tx
        .send(DiscordBotClientMessages::Shutdown)
        .unwrap();

    tasks.pop().unwrap().await.unwrap();

    channels_core
        .job_scheduler_tx
        .send(JobSchedulerMessages::Shutdown)
        .unwrap();

    tasks.pop().unwrap().await.unwrap();
}

fn restart() {
    let executable_path = match env::current_exe() {
        Ok(executable_path) => executable_path,
        Err(err) => {
            error!("An error occured while trying to get the path of this program: {err}");
            return;
        }
    };

    let mut args: VecDeque<OsString> = env::args_os().collect();

    args.pop_front();

    info!("Restarting the bot");

    #[cfg(target_family = "unix")]
    {
        let err = Command::new(executable_path).args(args).exec();
        error!("An error occured while trying to start a new instance of the program: {err}");
    }

    // HACK: Windows does not support `exec`. Instead we spawn a child porcess and wait for it to finish.
    #[cfg(target_family = "windows")]
    if let Err(err) = Command::new(executable_path).args(args).status() {
        error!("An error occured while trying to start a new instance of the program: {err}");
    }
}
