use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Config, Pool, Runtime, Transaction};
use futures::{future::BoxFuture, stream::FuturesUnordered};
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;
use tracing::{trace_span, Instrument};
use uuid::Uuid;

use crate::{task::Task, task_queue::TaskQueue};

/// A worker in the task queue system that processes tasks.
pub struct Worker {
    /// Unique identifier for the worker.
    id: Uuid,
    /// Connection pool to interact with the database.
    pool: Pool,
    /// Duration the worker sleeps between checks for new work.
    sleep_duration: Duration,
    /// Whether the worker uses work stealing.
    work_stealing: bool,
    /// Maximum number of tasks the worker can process concurrently.
    concurrency: i64,
    f: fn(Task) -> BoxFuture<'static, Result<()>>,
}

impl Worker {
    /// Creates a new `Worker` with optional configurations.
    ///
    /// # Arguments
    /// * `cfg` - Database configuration to create a connection pool.
    /// * `sleep_duration` - Optional duration for the worker to sleep between checks. Defaults to 250ms if not provided.
    /// * `work_stealing` - Optional flag indicating whether work stealing is enabled. Defaults to true if not provided.
    /// * `concurrency` - Optional number of tasks the worker can process concurrently. Defaults to 10 if not provided.
    ///
    /// # Returns
    /// A new `Worker` instance with default values for any optional parameters that are not provided.
    pub fn new(
        cfg: &Config,
        sleep_duration: Option<Duration>,
        work_stealing: Option<bool>,
        concurrency: Option<u16>,

        f: fn(Task) -> BoxFuture<'static, Result<()>>,
    ) -> Self {
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
        pool.resize(1);

        Self {
            id: Uuid::new_v4(),
            pool,
            sleep_duration: sleep_duration.unwrap_or(Duration::from_millis(250)),
            work_stealing: work_stealing.unwrap_or(true),
            concurrency: concurrency.unwrap_or(10) as i64,
            f,
        }
    }

    /// Retrieve Tasks using a fair scheduler. Each batch of tasks can only belong to at most one queues but
    /// depending on `work_stealing` may either be an exclusive ownership of the queues or return work
    /// from a queues that is already locked by another worker.
    ///
    /// `work_stealing` should mean that if tasks are very imbalanced between queues then the latency will go down
    /// for tasks in that queue however it comes with increased latency on the other queues.
    #[tracing::instrument(name="worker", skip_all, fields(id = ?self.id))]
    pub async fn run(&self) -> Result<()> {
        let mut client = self.pool.get().await?;

        loop {
            // Create a transaction that is held for the entire duration of the execution loop.
            let txn = client.transaction().await?;

            // A constant time to execute the queries relative to.
            let txn_time = Utc::now();

            // Try to retrieve an unlocked queue with available work. If there is no unlocked queue with available work
            // and work stealing is enabled then try to 'steal' work from the queue with the earliest scheduled_for.
            let task_queue = match TaskQueue::retrieve_next_skip_locked(&txn, &txn_time).await? {
                Some(task_queue) => Some(task_queue),
                None => {
                    if self.work_stealing {
                        TaskQueue::retrieve_next_ignore_locks(&txn, &txn_time).await?
                    } else {
                        None
                    }
                }
            };

            // If a queue with workload was found then retrieve available tasks.
            if let Some(task_queue) = task_queue {
                // If work stealing is enabled it is possible for not tasks to be returned.
                let tasks =
                    Task::retrieve_tasks(&txn, &task_queue.queue, &txn_time, self.concurrency)
                        .await?;

                tracing::info!(?task_queue, len = tasks.len());

                if !tasks.is_empty() {
                    // Spawn each task and do the actual work.
                    let tasks_set = tasks
                        .into_iter()
                        .map(|task| {
                            let task_id = task.id;
                            let task_clone = task.clone();
                            (
                                task,
                                tokio::spawn(
                                    { (self.f)(task_clone) }
                                        .instrument(trace_span!("task", id = ?task_id)),
                                ),
                            )
                        })
                        .collect::<FuturesUnordered<_>>();

                    // Wait for all tasks to complete. This uses a FuturesUnordered so that the tasks are updated
                    // in the order they are completed allowing for task execution duration variability between
                    // tasks.
                    for (task, join_handle) in tasks_set {
                        match join_handle.await {
                            Ok(task_result) => match task_result {
                                Ok(_) => task.delete(&txn).await?,
                                Err(error) => task.fail(&txn, error).await?,
                            },
                            Err(error) => task.fail(&txn, error.into()).await?,
                        };
                    }

                    // Update the task_queue to set the scheduled_for based on the new state of tasks.
                    task_queue.update_scheduled_for(&txn).await?;

                    // Commit the transaction which releases the locks.
                    txn.commit().await?;
                }
            }

            sleep(self.sleep_duration).await;
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
}
