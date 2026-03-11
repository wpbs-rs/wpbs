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

use clap::Parser;
use fjall::{Database, PersistMode};
use tokio::{signal, sync::RwLock, task::JoinHandle};
use tracing::{error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;

mod cli;
mod config;
mod database;
mod discord;
mod http;
mod job_scheduler;
mod plugins;
mod utils;

use cli::{Cli, CliLogParameters};
use config::Config;
use discord::DiscordBotClient;
use job_scheduler::JobScheduler;
use plugins::{registry, runtime::Runtime};
use utils::{channels::Channels, env::Secrets};

use crate::utils::channels::{ChannelsCore, CoreMessages};

#[derive(PartialEq)]
enum Shutdown {
    Normal,
    SigInt,
    Restart,
}

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

    ExitCode::from(0)
}

async fn run() -> Result<(), ()> {
    let cli = Cli::parse();

    let mut tasks: Vec<JoinHandle<()>> = vec![];

    let (_guard, secrets, channels) = initialization(cli.log_parameters, &cli.env_file)?;

    let config = Config::new(&cli.config_file)?;

    let database = database::new(&cli.database_directory).map_err(|_| ())?;

    tasks.push(start(database, channels.core));

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
        JobScheduler::new(channels.job_scheduler.core_tx, channels.job_scheduler.rx)
            .await
            .map_err(|_| ())?;

    let runtime = Runtime::new(channels.runtime.rx);

    tasks.push(job_scheduler.start().await.map_err(|_| ())?);

    tasks.push(discord_bot_client.start());

    runtime
        .initialize_plugins(
            available_plugins,
            channels.runtime.core_tx,
            &cli.plugin_directory,
        )
        .await?;

    tasks.push(runtime.start());

    shutdown(tasks).await
}

fn initialization(
    cli_log_parameters: CliLogParameters,
    env_file: &Path,
) -> Result<(Option<WorkerGuard>, Secrets, Channels), ()> {
    let guard = utils::logger::new(cli_log_parameters)?;

    utils::env::load_env_file(env_file).map_err(|_| ())?;

    let secrets = utils::env::get_secrets().map_err(|_| ())?;

    let channels = utils::channels::new();

    Ok((guard, secrets, channels))
}

fn start(database: Database, mut channels_core: ChannelsCore) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(core_message) = channels_core.rx.recv().await {
            match core_message {
                CoreMessages::DatabaseModule(database_message) => {
                    let database = database.clone();

                    tokio::spawn(async {
                        database::handle_action(database, database_message);
                    });
                }
                CoreMessages::JobSchedulerModule(job_scheduler_message) => {
                    channels_core.job_scheduler_tx.send(job_scheduler_message);
                }
                CoreMessages::DiscordBotClientModule(discord_bot_client_message) => {
                    channels_core
                        .discord_bot_client_tx
                        .send(discord_bot_client_message);
                }
                CoreMessages::RuntimeModule(runtime_message) => {
                    channels_core.runtime_tx.send(runtime_message);
                }
                CoreMessages::Shutdown(shutdown) => todo!(), // TODO: Figure shutdown out
            }
        }

        database::persist(database, PersistMode::SyncAll);
    })
}

async fn shutdown(mut tasks: Vec<JoinHandle<()>>) -> Result<(), ()> {
    tokio::spawn(async {
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

        Ok(())
    });

    for task in tasks.drain(..) {
        task.await;
    }

    Ok(())
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
