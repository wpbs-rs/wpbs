/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use anyhow::Result;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_cron_scheduler::{Job, JobScheduler as TokioCronScheduler};
use tracing::info;
use uuid::Uuid;

use crate::utils::channels::{
    CoreMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesJobScheduler,
};

pub struct JobScheduler {
    tokio_cron_scheduler: TokioCronScheduler,
    core_tx: UnboundedSender<CoreMessages>,
    rx: UnboundedReceiver<JobSchedulerMessages>,
}

impl JobScheduler {
    pub async fn new(
        core_tx: UnboundedSender<CoreMessages>,
        rx: UnboundedReceiver<JobSchedulerMessages>,
    ) -> Result<Self> {
        info!("Creating the job scheduler");

        Ok(JobScheduler {
            tokio_cron_scheduler: TokioCronScheduler::new().await?,
            core_tx,
            rx,
        })
    }

    pub async fn start(mut self) -> Result<JoinHandle<()>> {
        self.tokio_cron_scheduler.start().await?;

        Ok(tokio::spawn(async move {
            while let Some(message) = self.rx.recv().await {
                match message {
                    JobSchedulerMessages::AddJob(plugin_id, cron, result) => {
                        let tokio_cron_scheduler = self.tokio_cron_scheduler.clone();
                        let core_tx = self.core_tx.clone();

                        tokio::spawn(async move {
                            result
                                .send(
                                    Self::add_job(tokio_cron_scheduler, core_tx, plugin_id, cron)
                                        .await,
                                )
                                .unwrap();
                        });
                    }
                    JobSchedulerMessages::RemoveJob(uuid, result) => {
                        let tokio_cron_scheduler = self.tokio_cron_scheduler.clone();

                        tokio::spawn(async move {
                            result
                                .send(Self::remove_job(tokio_cron_scheduler, uuid).await)
                                .unwrap();
                        });
                    }
                    JobSchedulerMessages::Shutdown => {
                        self.rx.close();
                    }
                }
            }

            self.tokio_cron_scheduler.shutdown().await.unwrap();
        }))
    }

    async fn add_job(
        tokio_cron_scheduler: TokioCronScheduler,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_id: Uuid,
        cron: String,
    ) -> Result<Uuid> {
        info!(
            "Scheduled Job at {cron} cron from the {plugin_id} plugin requested to be registered"
        );

        let job = Job::new_async_tz(cron.clone(), chrono::Local, move |job_id, _lock| {
            let core_tx = core_tx.clone();

            Box::pin(async move {
                core_tx
                    .send(CoreMessages::Runtime(RuntimeMessages::JobScheduler(
                        RuntimeMessagesJobScheduler::CallScheduledJob(plugin_id, job_id),
                    )))
                    .unwrap();
            })
        })?;

        Ok(tokio_cron_scheduler.add(job).await?)
    }

    async fn remove_job(tokio_cron_scheduler: TokioCronScheduler, uuid: Uuid) -> Result<()> {
        info!("Removing scheduled Job {uuid}");

        Ok(tokio_cron_scheduler.remove(&uuid).await?)
    }
}
