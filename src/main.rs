/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    collections::VecDeque,
    env,
    ffi::OsString,
    path::PathBuf,
    process::{self, Command, ExitCode},
    sync::LazyLock,
};

use anyhow::Result;
use clap::Parser;
use fjall::{Database, PersistMode};
use tokio::{
    signal,
    sync::{
        Mutex, RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tracing::{debug, error, info, warn};

mod cli;
mod config;
mod database;
mod http;
mod registry;
mod runtime;
mod services;
mod utils;

use cli::Cli;
use config::Config;

use crate::{
    runtime::Runtime,
    utils::{
        channels::{
            ChannelsRuntime, ChannelsServices, CoreMessages, DiscordMessages, JobSchedulerMessages,
            RuntimeMessages,
        },
        env::Secrets,
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

static SETUP: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

static SHUTDOWN: LazyLock<RwLock<Option<Shutdown>>> = LazyLock::new(|| RwLock::new(None));

#[tokio::main]
#[hotpath::main]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();

    let _guard = utils::logger::new(cli.log_parameters)?;

    utils::env::load_env_file(&cli.env_file)?;

    let config = Config::new(&cli.config_file)?;

    let secrets = utils::env::get_secrets(&config.services)?;

    let channels = utils::channels::new(
        config.services.job_scheduler.enabled,
        config.services.discord.enabled,
    );

    let setup_guard = SETUP.lock().await;

    let shutdown_signal_listener = shutdown_signal_listener(channels.core.shutdown);

    let database = database::new(&cli.database_directory)?;

    database::cleanup(&database)?;

    let message_handler = message_handler(
        database,
        Some(channels.core.runtime_tx),
        channels.core.job_scheduler_tx,
        channels.core.discord_tx,
        channels.core.rx,
        Some(shutdown_signal_listener),
    );

    let setup_result = setup(
        cli.http_client_timeout_seconds,
        cli.plugin_directory,
        cli.cache,
        channels.services,
        channels.runtime,
        config,
        secrets,
    )
    .await;

    post_setup(channels.core.post_setup, setup_result).await;

    drop(setup_guard);

    message_handler.await.unwrap()?;

    exit().await
}

fn message_handler(
    database: Database,
    mut runtime_tx: Option<UnboundedSender<RuntimeMessages>>,
    mut job_scheduler_tx: Option<UnboundedSender<JobSchedulerMessages>>,
    mut discord_tx: Option<UnboundedSender<DiscordMessages>>,
    mut rx: UnboundedReceiver<CoreMessages>,
    mut shutdown_signal_listener: Option<JoinHandle<()>>,
) -> JoinHandle<Result<()>> {
    debug!("Starting the message handler");

    let mut shutdown_task = None;

    tokio::spawn(async move {
        while let Some(core_message) = rx.recv().await {
            match core_message {
                CoreMessages::DatabaseModule(database_message) => {
                    // TODO: Move behind a channel to keep the thread clear
                    database::handle_action(&database, database_message).await;
                }
                CoreMessages::JobScheduler(job_scheduler_message) => {
                    if let Some(job_scheduler_tx) = job_scheduler_tx.as_ref() {
                        job_scheduler_tx.send(job_scheduler_message).unwrap();
                    }
                }
                CoreMessages::Discord(discord_message) => {
                    if let Some(discord_tx) = discord_tx.as_ref() {
                        discord_tx.send(discord_message).unwrap();
                    }
                }
                CoreMessages::Runtime(runtime_message) => {
                    if let Some(runtime_tx) = runtime_tx.as_ref() {
                        runtime_tx.send(runtime_message).unwrap();
                    }
                }
                CoreMessages::Shutdown(shutdown_kind) => {
                    {
                        let mut shutdown_guard = SHUTDOWN.write().await;

                        if let Some(shutdown_value) = *shutdown_guard {
                            if (shutdown_value != Shutdown::SigInt
                                && shutdown_kind == Shutdown::SigInt)
                                || (shutdown_value == Shutdown::Restart
                                    && shutdown_kind == Shutdown::Normal)
                            {
                                let _ = shutdown_guard.insert(shutdown_kind);
                            }

                            continue;
                        }

                        let _ = shutdown_guard.insert(shutdown_kind);
                    }

                    shutdown_task = Some(tokio::spawn(shutdown(
                        job_scheduler_tx.take(),
                        discord_tx.take(),
                        runtime_tx.take(),
                        shutdown_signal_listener.take(),
                    )));
                }
            }
        }

        shutdown_task.unwrap().await.unwrap();

        database::persist(&database, PersistMode::SyncAll)
    })
}

async fn setup(
    http_client_timeout_seconds: u64,
    plugin_directory_path: PathBuf,
    cache: bool,
    service_channels: ChannelsServices,
    runtime_channels: ChannelsRuntime,
    config: Config,
    secrets: Secrets,
) -> Result<()> {
    let available_plugins = registry::registry_get_plugins(
        http_client_timeout_seconds,
        config.plugins,
        plugin_directory_path.clone(),
        cache,
    )
    .await?;

    services::setup(config.services, secrets.services, service_channels).await?;

    let runtime = Runtime::new(runtime_channels.rx);

    runtime
        .initialize_plugins(
            available_plugins,
            runtime_channels.core_tx,
            plugin_directory_path,
        )
        .await?;

    TASKS.write().await.runtime = Some(runtime.run());

    Ok(())
}

async fn post_setup(core_post_setup_tx: UnboundedSender<CoreMessages>, setup_result: Result<()>) {
    if let Err(err) = setup_result {
        error!("A setup error occurred: {err}");

        core_post_setup_tx
            .send(CoreMessages::Shutdown(Shutdown::Normal))
            .unwrap();

        return;
    }

    services::post_setup(&core_post_setup_tx).await;

    info!("Setup completed successfully");
}

fn shutdown_signal_listener(core_tx: UnboundedSender<CoreMessages>) -> JoinHandle<()> {
    debug!("Starting the shutdown signal listener");

    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to listen for the terminal interrupt signal");
        };

        #[cfg(target_family = "unix")]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install the terminate signal handler")
                .recv()
                .await;
        };

        #[cfg(target_family = "windows")]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {},
            () = terminate => {},
        }

        info!("Termination signal received, send another to force immediate shutdown");

        tokio::spawn(async {
            let ctrl_c = async {
                signal::ctrl_c()
                    .await
                    .expect("failed to listen for the terminal interrupt signal");
            };

            #[cfg(target_family = "unix")]
            let terminate = async {
                signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("failed to install the terminate signal handler")
                    .recv()
                    .await;
            };

            #[cfg(target_family = "windows")]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                () = ctrl_c => {},
                () = terminate => {},
            }

            warn!("Second termination signal received, forcing immediate shutdown");
            process::exit(130);
        });

        core_tx
            .send(CoreMessages::Shutdown(Shutdown::SigInt))
            .unwrap();
    })
}

async fn shutdown(
    job_scheduler_tx: Option<UnboundedSender<JobSchedulerMessages>>,
    discord_tx: Option<UnboundedSender<DiscordMessages>>,
    runtime_tx: Option<UnboundedSender<RuntimeMessages>>,
    shutdown_signal_listener: Option<JoinHandle<()>>,
) {
    let _setup_guard = SETUP.lock().await;
    let mut tasks = TASKS.write().await;

    drop(runtime_tx.unwrap());

    if let Some(runtime) = tasks.runtime.take() {
        runtime.await.unwrap();
    }

    drop((job_scheduler_tx, discord_tx));

    if let Some(job_scheduler) = tasks.services.job_scheduler.take() {
        job_scheduler.await.unwrap();
    }

    if let Some(discord) = tasks.services.discord.take() {
        discord.await.unwrap();
    }

    shutdown_signal_listener.as_ref().unwrap().abort();
    let _ = shutdown_signal_listener.unwrap().await;
}

fn restart() -> Result<u8> {
    let executable_path = env::current_exe()?;

    let mut args: VecDeque<OsString> = env::args_os().collect();

    args.pop_front();

    info!("Restarting the program");

    #[cfg(target_family = "unix")]
    {
        use std::os::unix::process::CommandExt;

        use anyhow::bail;

        bail!(Command::new(executable_path).args(args).exec());
    }

    // HACK: Windows does not support `exec`. Instead we spawn a child porcess and wait for it to finish.
    #[cfg(target_family = "windows")]
    {
        Command::new(executable_path).args(args).status()?;

        Ok(0)
    }
}

async fn exit() -> Result<ExitCode> {
    let exit_code = match SHUTDOWN.read().await.as_ref().unwrap() {
        Shutdown::Normal => 0,
        Shutdown::SigInt => 130,
        Shutdown::Restart => restart()?,
    };

    info!("Exiting the program");

    Ok(ExitCode::from(exit_code))
}
