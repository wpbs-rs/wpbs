/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{
    collections::VecDeque,
    env,
    ffi::OsString,
    path::PathBuf,
    process::{self, Command, ExitCode},
    sync::{Arc, LazyLock},
};

use anyhow::Result;
use clap::Parser;
use fjall::Database;
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

    let message_handler = message_handler(
        database.clone(),
        Arc::new(RwLock::new(Some(channels.core.runtime_tx))),
        Arc::new(RwLock::new(channels.core.job_scheduler_tx)),
        Arc::new(RwLock::new(channels.core.discord_tx)),
        Arc::new(shutdown_signal_listener),
        channels.core.rx,
    );

    let setup_result = setup(
        cli.plugin_directory,
        database,
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
    runtime_tx: Arc<RwLock<Option<UnboundedSender<RuntimeMessages>>>>,
    job_scheduler_tx: Arc<RwLock<Option<UnboundedSender<JobSchedulerMessages>>>>,
    discord_tx: Arc<RwLock<Option<UnboundedSender<DiscordMessages>>>>,
    shutdown_signal_listener: Arc<JoinHandle<()>>,
    mut rx: UnboundedReceiver<CoreMessages>,
) -> JoinHandle<Result<()>> {
    debug!("Starting the message handler");

    tokio::spawn(async move {
        while let Some(core_message) = rx.recv().await {
            match core_message {
                CoreMessages::JobScheduler(job_scheduler_message) => {
                    if let Some(job_scheduler_tx) = job_scheduler_tx.read().await.as_ref() {
                        job_scheduler_tx.send(job_scheduler_message).unwrap();
                    }
                }
                CoreMessages::Discord(discord_message) => {
                    if let Some(discord_tx) = discord_tx.read().await.as_ref() {
                        discord_tx.send(discord_message).unwrap();
                    }
                }
                CoreMessages::Runtime(runtime_message) => {
                    if let Some(runtime_tx) = runtime_tx.read().await.as_ref() {
                        runtime_tx.send(runtime_message).unwrap();
                    }
                }
                CoreMessages::Shutdown(shutdown_kind) => {
                    tokio::spawn(shutdown(
                        shutdown_kind,
                        runtime_tx.clone(),
                        job_scheduler_tx.clone(),
                        discord_tx.clone(),
                        shutdown_signal_listener.clone(),
                    ));
                }
            }
        }

        Ok(database.persist(fjall::PersistMode::SyncAll)?)
    })
}

async fn setup(
    plugin_directory_path: PathBuf,
    database: Database,
    service_channels: ChannelsServices,
    runtime_channels: ChannelsRuntime,
    config: Config,
    secrets: Secrets,
) -> Result<()> {
    let config_name = Arc::new(config.name);

    let available_plugins = registry::get_plugins(
        &plugin_directory_path,
        database.clone(),
        config_name.clone(),
        config.plugins,
    )
    .await?;

    services::setup(
        config.services,
        secrets.services,
        database.clone(),
        service_channels,
    )
    .await?;

    let runtime = Runtime::new(runtime_channels.rx);

    runtime
        .initialize_plugins(
            plugin_directory_path,
            config_name,
            available_plugins,
            database,
            runtime_channels.core_tx,
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
    shutdown_kind: Shutdown,
    runtime_tx: Arc<RwLock<Option<UnboundedSender<RuntimeMessages>>>>,
    job_scheduler_tx: Arc<RwLock<Option<UnboundedSender<JobSchedulerMessages>>>>,
    discord_tx: Arc<RwLock<Option<UnboundedSender<DiscordMessages>>>>,
    shutdown_signal_listener: Arc<JoinHandle<()>>,
) {
    let mut shutdown_guard = SHUTDOWN.write().await;

    if let Some(shutdown_value) = *shutdown_guard {
        if (shutdown_value != Shutdown::SigInt && shutdown_kind == Shutdown::SigInt)
            || (shutdown_value == Shutdown::Restart && shutdown_kind == Shutdown::Normal)
        {
            let _ = shutdown_guard.insert(shutdown_kind);
        }

        return;
    }

    let _ = shutdown_guard.insert(shutdown_kind);

    drop(shutdown_guard);

    let _setup_guard = SETUP.lock().await;
    let mut tasks = TASKS.write().await;

    drop(runtime_tx.write().await.take().unwrap());

    if let Some(runtime) = tasks.runtime.take() {
        runtime.await.unwrap();
    }

    drop((
        job_scheduler_tx.write().await.take(),
        discord_tx.write().await.take(),
    ));

    if let Some(job_scheduler) = tasks.services.job_scheduler.take() {
        job_scheduler.await.unwrap();
    }

    if let Some(discord) = tasks.services.discord.take() {
        discord.await.unwrap();
    }

    shutdown_signal_listener.abort();
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
