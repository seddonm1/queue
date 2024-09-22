use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Config, Pool, Runtime, Transaction};
use tokio_postgres::NoTls;
use tracing::{trace_span, Instrument};
use uuid::Uuid;

use crate::{task::Task, task_queue::TaskQueue};

pub struct Worker {
    pub id: Uuid,
    pool: Pool,
    sleep_duration: Duration,
    work_stealing: bool,
    concurrency: i64,
}

impl Worker {
    pub fn new(
        cfg: &Config,
        sleep_duration: Option<Duration>,
        work_stealing: Option<bool>,
        concurrency: Option<u16>,
    ) -> Self {
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
        pool.resize(1);

        Self {
            id: Uuid::new_v4(),
            pool,
            sleep_duration: sleep_duration.unwrap_or(Duration::from_millis(250)),
            work_stealing: work_stealing.unwrap_or(true),
            concurrency: concurrency.unwrap_or(10) as i64,
        }
    }

    #[tracing::instrument(name="worker", skip_all, fields(id = ?self.id))]
    pub async fn run(&self) -> Result<()> {
        let mut client = self.pool.get().await?;

        loop {
            let txn = client.transaction().await?;

            if let Some((task_queue, tasks)) = self.retrieve_tasks(&txn).await? {
                tracing::info!(?task_queue, len = tasks.len());

                // Spawn each task
                let tasks_set = tasks
                    .into_iter()
                    .map(|task| {
                        let task_move = task.clone();
                        (
                            task.clone(),
                            tokio::spawn(async move { task_move.work().await })
                                .instrument(trace_span!("task", id = ?task.id)),
                        )
                    })
                    .collect::<Vec<_>>();

                // Wait for all tasks to complete
                for (task, join_handle) in tasks_set {
                    match join_handle.await {
                        Ok(task_result) => match task_result {
                            Ok(_) => task.delete(&txn).await?,
                            Err(error) => task.fail(&txn, error).await?,
                        },
                        Err(error) => task.fail(&txn, error.into()).await?,
                    };
                }

                // Update the queue
                task_queue.update_scheduled_for(&txn).await?;

                txn.commit().await?;

                tokio::time::sleep(self.sleep_duration.div_f32(2.0)).await;
            } else {
                tokio::time::sleep(self.sleep_duration).await;
            }
        }
    }

    #[allow(unused)]
    pub async fn create_task(
        txn: &Transaction<'_>,
        scheduled_for: Option<DateTime<Utc>>,
        max_retries: Option<u16>,
        args: Option<serde_json::Value>,
        queue: &str,
    ) -> Result<()> {
        let id = Uuid::new_v4();
        let scheduled_for = scheduled_for.unwrap_or_else(Utc::now);
        let max_retries = max_retries.unwrap_or(5) as i16;
        let args = args.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));

        Task::insert(txn, queue, id, scheduled_for, max_retries, args).await?;

        Ok(())
    }

    // Retrieve Tasks using a fair scheduler. Each batch of tasks can only belong to at most one queues but
    // depending on `work_stealing` may either be an exclusive ownership of the queues or return work
    // from a queues that is already locked by another worker.
    //
    // `work_stealing` should mean that if tasks are very imbalanced between queues then the latency will go down
    // for tasks in that queue however it comes with increased latency on the other queues.
    #[tracing::instrument(level = "trace", skip_all, ret)]
    pub async fn retrieve_tasks(
        &self,
        txn: &Transaction<'_>,
    ) -> Result<Option<(TaskQueue, Vec<Task>)>> {
        let now = Utc::now();

        // Try to retrieve an unlocked queue with available work. If there is no unlocked queue with available work
        // and work stealing is enabled then try to 'steal' work from the queue with the earliest scheduled_for.
        let task_queue = match TaskQueue::retrieve_next_skip_locked(txn, &now).await? {
            Some(task_queue) => Some(task_queue),
            None => {
                if self.work_stealing {
                    TaskQueue::retrieve_next(txn, &now).await?
                } else {
                    None
                }
            }
        };

        // If a queue with workload was found then retrieve available tasks. This should always
        // return tasks given the scheduled_for check on the task_queue.
        if let Some(task_queue) = task_queue {
            let tasks =
                Task::retrieve_tasks(txn, &task_queue.queue, &now, self.concurrency).await?;

            Ok(Some((task_queue, tasks)))
        } else {
            Ok(None)
        }
    }
}
