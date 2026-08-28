/* SPDX-License-Identifier: GPL-3.0-or-later */
/* Copyright © 2026 Eduard Smet */

use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{Result, bail};
use chrono::Local;
use cron::Schedule;
use tokio::{
    sync::{
        RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
    time::Instant,
};
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use crate::utils::channels::{
    CoreMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesServices,
    RuntimeMessagesServicesJobScheduler,
};

pub struct JobScheduler {
    jobs: Arc<RwLock<HashMap<Uuid, JoinHandle<()>>>>,
    core_tx: UnboundedSender<CoreMessages>,
    rx: UnboundedReceiver<JobSchedulerMessages>,
}

impl JobScheduler {
    pub fn new(
        core_tx: UnboundedSender<CoreMessages>,
        rx: UnboundedReceiver<JobSchedulerMessages>,
    ) -> Self {
        info!("Creating the job scheduler service");

        JobScheduler {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            core_tx,
            rx,
        }
    }

    #[hotpath::measure]
    pub fn run(mut self) -> JoinHandle<()> {
        info!("Starting the job scheduler service");

        tokio::spawn(async move {
            let task_tracker = TaskTracker::new();

            while let Some(message) = self.rx.recv().await {
                match message {
                    JobSchedulerMessages::AddJob(plugin_uuid, cron, sender) => {
                        let jobs = self.jobs.clone();
                        let core_tx = self.core_tx.clone();

                        task_tracker.spawn(async move {
                            sender
                                .send(Self::add_job(jobs, core_tx, plugin_uuid, cron).await)
                                .unwrap();
                        });
                    }
                    JobSchedulerMessages::RemoveJob(job_uuid, sender) => {
                        let jobs = self.jobs.clone();

                        task_tracker.spawn(async move {
                            sender.send(Self::remove_job(jobs, job_uuid).await).unwrap();
                        });
                    }
                }
            }

            task_tracker.close();
            task_tracker.wait().await;

            self.shutdown().await;
        })
    }

    async fn add_job(
        jobs: Arc<RwLock<HashMap<Uuid, JoinHandle<()>>>>,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_uuid: Uuid,
        cron: String,
    ) -> Result<Uuid> {
        info!(
            "Scheduled Job at {cron} cron from the {plugin_uuid} plugin requested to be registered"
        );

        let job_uuid = Uuid::new_v4();

        let schedule = Schedule::from_str(&cron)?;

        let task = tokio::spawn(async move {
            for datetime in schedule.upcoming(Local) {
                if let Ok(duration) = datetime.signed_duration_since(Local::now()).to_std() {
                    tokio::time::sleep_until(Instant::now() + duration).await;
                }

                let _ = core_tx.send(CoreMessages::Runtime(RuntimeMessages::Services(
                    RuntimeMessagesServices::JobScheduler(
                        RuntimeMessagesServicesJobScheduler::CallScheduledJob(
                            plugin_uuid,
                            job_uuid,
                        ),
                    ),
                )));
            }
        });

        jobs.write().await.insert(job_uuid, task);

        Ok(job_uuid)
    }

    async fn remove_job(
        jobs: Arc<RwLock<HashMap<Uuid, JoinHandle<()>>>>,
        job_uuid: Uuid,
    ) -> Result<()> {
        info!("Removing scheduled job {job_uuid}");

        if let Some(job) = jobs.write().await.remove(&job_uuid) {
            job.abort();

            let _ = job.await;

            return Ok(());
        }

        bail!("No job with this uuid was found");
    }

    async fn shutdown(&self) {
        info!("Shutting the job scheduler service down");

        for job in self.jobs.write().await.drain() {
            job.1.abort();

            let _ = job.1.await;
        }
    }
}
