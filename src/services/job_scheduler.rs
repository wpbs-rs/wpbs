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

use crate::{
    runtime::plugins::{
        RuntimePluginMetadata,
        bindings::services::job_scheduler::wpbs_services::job_scheduler::job_scheduler_types::Cron,
    },
    utils::channels::{
        CoreMessages, JobSchedulerMessages, RuntimeMessages, RuntimeMessagesServices,
        RuntimeMessagesServicesJobScheduler,
    },
};

pub struct JobScheduler {
    jobs: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
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
                    JobSchedulerMessages::RegisterJobs(plugin_uuid, registrations, sender) => {
                        let jobs = self.jobs.clone();
                        let core_tx = self.core_tx.clone();

                        task_tracker.spawn(async move {
                            sender
                                .send(Self::register_job(jobs, core_tx, plugin_uuid, cron).await)
                                .unwrap();
                        });
                    }
                    JobSchedulerMessages::UpdateJobs(plugin_uuid, updates, sender) => {
                        let jobs = self.jobs.clone();

                        task_tracker.spawn(async move {
                            sender
                                .send(Self::deregister_job(jobs, plugin_uuid, cron).await)
                                .unwrap();
                        });
                    }
                    JobSchedulerMessages::DeregisterJobs(plugin_uuid, deregistrations, sender) => {
                        let jobs = self.jobs.clone();

                        task_tracker.spawn(async move {
                            sender
                                .send(Self::deregister_job(jobs, plugin_uuid, cron).await)
                                .unwrap();
                        });
                    }
                }
            }

            task_tracker.close();
            task_tracker.wait().await;

            self.shutdown().await;
        })
    }

    async fn register_jobs(
        jobs: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
        core_tx: UnboundedSender<CoreMessages>,
        plugin_metadata: Arc<RuntimePluginMetadata>,
        registrations: HashMap<String, Cron>,
    ) -> Result<HashMap<String, Result<Uuid>>> {
        let registrations_result = HashMap::new();

        for (job_id, cron) in registrations {
            info!(
                "Scheduled \"{job_id}\" job at {cron} cron from the {} plugin requested to be registered",
                plugin_metadata.user_id
            );

            let cron = Arc::new(cron);

            let key = format!("{}:{}", plugin_metadata.plugin_uuid, job_id);

            let mut jobs = jobs.write().await;

            if jobs.contains_key(&key) {}

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
                                cron.clone(),
                            ),
                        ),
                    )));
                }
            });

            jobs.insert(key, task);
        }

        Ok(())
    }

    async fn deregister_job(
        jobs: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
        plugin_uuid: Uuid,
        cron: String,
    ) -> Result<()> {
        info!("Removing scheduled job {cron} from the {plugin_uuid} plugin");

        let key = format!("{plugin_uuid}:{cron}");

        if let Some(job) = jobs.write().await.remove(&key) {
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
