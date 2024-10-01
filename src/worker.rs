use anyhow::Result;
use chrono::Utc;
use deadpool_postgres::Pool;
use futures::stream::FuturesUnordered;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{trace_span, Instrument};
use uuid::Uuid;

use crate::{task::Task, task_queue::TaskQueue};

pub trait Worker {
    /// Unique identifier for the worker.
    fn id(&self) -> Uuid;

    /// Connection pool to interact with the database.
    fn pool(&self) -> &Pool;

    /// Whether the worker uses work stealing.
    fn work_stealing(&self) -> bool {
        true
    }

    /// Maximum number of tasks the worker can process concurrently.
    fn concurrency(&self) -> i64 {
        10
    }

    /// Duration the worker sleeps between checks for new work.
    fn sleep_duration(&self) -> Duration {
        Duration::from_millis(100)
    }

    /// A function to do the work.
    fn work(
        task: Task,
        pool: Pool,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send + 'static;

    /// Retrieve Tasks using a fair scheduler. Each batch of tasks can only belong to at most one queues but
    /// depending on `work_stealing` may either be an exclusive ownership of the queues or return work
    /// from a queues that is already locked by another worker.
    ///
    /// `work_stealing` should mean that if tasks are very imbalanced between queues then the latency will go down
    /// for tasks in that queue however it comes with increased latency on the other queues.
    #[tracing::instrument(name="worker", skip_all, fields(id = ?self.id()))]
    async fn run(&self) -> Result<()> {
        let mut client = self.pool().get().await?;

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
                    if self.work_stealing() {
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
                    Task::retrieve_tasks(&txn, &task_queue.queue, &txn_time, self.concurrency())
                        .await?;

                tracing::info!(?task_queue, len = tasks.len());

                if !tasks.is_empty() {
                    // Spawn each task and do the actual work.
                    let tasks_set = tasks
                        .into_iter()
                        .map(|task| {
                            let task_id = task.id;
                            let task_clone = task.clone();
                            let pool = self.pool().clone();
                            (
                                task,
                                tokio::spawn(
                                    async move { Self::work(task_clone, pool).await }
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
                                Ok(_) => task.success(&txn).await?,
                                Err(error) => task.failure(&txn, error).await?,
                            },
                            Err(error) => task.failure(&txn, error.into()).await?,
                        };
                    }
                }

                // Update the task_queue to set the scheduled_for based on the new state of tasks.
                task_queue.update_scheduled_for(&txn).await?;

                // Commit the transaction which releases the locks.
                txn.commit().await?;
            }

            sleep(self.sleep_duration()).await;
        }
    }
}
