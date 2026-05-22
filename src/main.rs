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
use tokio::{
    signal,
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
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
use services::{discord::Discord, job_scheduler::JobScheduler};
use utils::channels::Channels;

use crate::{
    runtime::Runtime,
    utils::channels::{
        CoreMessages, DiscordMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesCore,
    },
};

struct Tasks {
    runtime: Option<JoinHandle<()>>,
    services: TasksServices,
}

struct TasksServices {
    job_scheduler: Option<JoinHandle<()>>,
    discord: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy, PartialEq)]
enum Shutdown {
    Normal,
    SigInt,
    Restart,
}

static TASKS: LazyLock<RwLock<Tasks>> = LazyLock::new(|| {
    RwLock::new(Tasks {
        runtime: None,
        services: TasksServices {
            job_scheduler: None,
            discord: None,
        },
    })
});
static SHUTDOWN: LazyLock<RwLock<Option<Shutdown>>> = LazyLock::new(|| RwLock::new(None));

#[tokio::main]
async fn main() -> ExitCode {
    let mut exit_code = 1;

    if let Err(err) = run().await {
        error!("{err}");
    } else {
        match SHUTDOWN.read().await.as_ref().unwrap() {
            Shutdown::Normal => exit_code = 0,
            Shutdown::SigInt => exit_code = 130,
            Shutdown::Restart => restart(),
        }
    }

    info!("Exiting the program");

    ExitCode::from(exit_code)
}

#[hotpath::main]
async fn run() -> Result<()> {
    let cli = Cli::parse();

    let (_guard, channels) = initialization(cli.log_parameters, &cli.env_file)?;

    shutdown_signal_listener(channels.core.shutdown);

    let config = Config::new(&cli.config_file)?;

    let secrets = utils::env::get_secrets(&config.services)?;

    let database = database::new(&cli.database_directory)?;

    let core = start(
        database,
        channels.core.runtime_tx,
        channels.core.job_scheduler_tx,
        channels.core.discord_tx,
        channels.core.rx,
    );

    {
        let mut tasks = TASKS.write().await;

        let available_plugins = registry::registry_get_plugins(
            cli.http_client_timeout_seconds,
            config.plugins,
            cli.plugin_directory.clone(),
            cli.cache,
        )
        .await?;

        if config.services.job_scheduler.enabled {
            let job_scheduler =
                JobScheduler::new(channels.job_scheduler.core_tx, channels.job_scheduler.rx)
                    .await?;

            tasks.services.job_scheduler = Some(job_scheduler.start().await?);
        } else {
            drop(channels.job_scheduler.rx);
        }

        if config.services.discord.enabled {
            let discord = Discord::new(
                config.services.discord,
                secrets.discord.unwrap(),
                channels.discord.core_tx,
                channels.discord.rx,
            )
            .await?;

            tasks.services.discord = Some(discord.start());
        } else {
            drop(channels.discord.rx);
        }

        let runtime = Runtime::new(channels.runtime.rx);

        runtime
            .initialize_plugins(
                available_plugins,
                channels.runtime.core_tx,
                cli.plugin_directory,
            )
            .await?;

        tasks.runtime = Some(runtime.start());

        channels
            .core
            .main
            .send(CoreMessages::Discord(
                DiscordMessages::RegisterApplicationCommands,
            ))
            .unwrap();
    }

    core.await.unwrap()
}

fn initialization(
    cli_log_parameters: CliLogParameters,
    env_file: &Path,
) -> Result<(Option<WorkerGuard>, Channels)> {
    let guard = utils::logger::new(cli_log_parameters)?;

    utils::env::load_env_file(env_file)?;

    let channels = utils::channels::new();

    Ok((guard, channels))
}

fn start(
    database: Database,
    runtime_tx: UnboundedSender<RuntimeMessages>,
    job_scheduler_tx: UnboundedSender<JobSchedulerMessages>,
    discord_tx: UnboundedSender<DiscordMessages>,
    mut rx: UnboundedReceiver<CoreMessages>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        while let Some(core_message) = rx.recv().await {
            match core_message {
                CoreMessages::DatabaseModule(database_message) => {
                    database::handle_action(&database, database_message).await;
                }
                CoreMessages::JobScheduler(job_scheduler_message) => {
                    job_scheduler_tx.send(job_scheduler_message).unwrap();
                }
                CoreMessages::Discord(discord_message) => {
                    discord_tx.send(discord_message).unwrap();
                }
                CoreMessages::Runtime(runtime_message) => {
                    // May error as services can send messages to it after its channel has been closed
                    let _ = runtime_tx.send(runtime_message);
                }
                CoreMessages::Shutdown(shutdown_kind) => {
                    {
                        let mut shutdown_guard = SHUTDOWN.write().await;

                        if let Some(shutdown_value) = *shutdown_guard
                            && (shutdown_kind != Shutdown::SigInt
                                || shutdown_value == Shutdown::SigInt)
                        {
                            continue;
                        }

                        let _ = shutdown_guard.insert(shutdown_kind);
                    }

                    shutdown(&job_scheduler_tx, &discord_tx, &runtime_tx).await;

                    // TODO: rewrite to not have to close the receiver
                    rx.close();
                }
            }
        }

        database::persist(&database, PersistMode::SyncAll)
    })
}

fn shutdown_signal_listener(core_tx: UnboundedSender<CoreMessages>) {
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

        core_tx
            .send(CoreMessages::Shutdown(Shutdown::SigInt))
            .unwrap();

        Ok(())
    });
}

async fn shutdown(
    job_scheduler_tx: &UnboundedSender<JobSchedulerMessages>,
    discord_tx: &UnboundedSender<DiscordMessages>,
    runtime_tx: &UnboundedSender<RuntimeMessages>,
) {
    let mut tasks = TASKS.write().await;

    if let Some(runtime) = tasks.runtime.take() {
        runtime_tx
            .send(RuntimeMessages::Core(RuntimeMessagesCore::Shutdown))
            .unwrap();

        runtime.await.unwrap();
    }

    if let Some(discord) = tasks.services.discord.take() {
        discord_tx.send(DiscordMessages::Shutdown).unwrap();

        discord.await.unwrap();
    }

    if let Some(job_scheduler) = tasks.services.job_scheduler.take() {
        job_scheduler_tx
            .send(JobSchedulerMessages::Shutdown)
            .unwrap();

        job_scheduler.await.unwrap();
    }
}

fn restart() {
    let executable_path = match env::current_exe() {
        Ok(executable_path) => executable_path,
        Err(err) => {
            error!("An error occurred while trying to get the path of this program: {err}");
            return;
        }
    };

    let mut args: VecDeque<OsString> = env::args_os().collect();

    args.pop_front();

    info!("Restarting the program");

    #[cfg(target_family = "unix")]
    {
        let err = Command::new(executable_path).args(args).exec();
        error!("An error occurred while trying to start a new instance of the program: {err}");
    }

    // HACK: Windows does not support `exec`. Instead we spawn a child porcess and wait for it to finish.
    #[cfg(target_family = "windows")]
    if let Err(err) = Command::new(executable_path).args(args).status() {
        error!("An error occurred while trying to start a new instance of the program: {err}");
    }
}
