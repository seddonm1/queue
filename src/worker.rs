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

            if let Some((task_queue, tasks)) = self.pull_tasks(&txn).await? {
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
                self.finalize_tasks(&txn, task_queue).await?;

                txn.commit().await?;

                tokio::time::sleep(self.sleep_duration.div_f32(2.0)).await;
            } else {
                tokio::time::sleep(self.sleep_duration).await;
            }
        }
    }

    pub async fn create_task(
        txn: &Transaction<'_>,
        scheduled_for: Option<DateTime<Utc>>,
        max_retries: Option<u16>,
        args: Option<serde_json::Value>,
        queue: &str,
    ) -> Result<()> {
        let scheduled_for = scheduled_for.unwrap_or_else(|| Utc::now());
        let max_retries = max_retries.unwrap_or(5) as i16;

        // Insert the job.
        let stmt = txn
            .prepare_cached(
                "
                    WITH insert_tasks AS (
                        INSERT INTO tasks (
                            queue, 
                            id, 
                            scheduled_for, 
                            max_retries, 
                            args
                        ) VALUES (
                            $1, 
                            $2, 
                            $3, 
                            $4, 
                            $5
                        )
                        RETURNING scheduled_for
                    )
                    UPDATE task_queues
                    SET scheduled_for = LEAST(scheduled_for, (SELECT scheduled_for FROM insert_tasks))
                    WHERE queue = $1
                ",
            )
            .await?;

        txn.execute(
            &stmt,
            &[
                &queue,
                &Uuid::new_v4(),
                &scheduled_for,
                &max_retries,
                &args.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
            ],
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, ret)]
    pub async fn pull_tasks(
        &self,
        txn: &Transaction<'_>,
    ) -> Result<Option<(TaskQueue, Vec<Task>)>> {
        let now = Utc::now();

        // First try to get exclusive access to a single queue.
        // This is the fairness component.
        let stmt = txn
            .prepare_cached(
                "
                WITH locked_task_queue AS (
                    SELECT queue 
                    FROM task_queues 
                    WHERE scheduled_for <= $1
                    ORDER BY scheduled_for 
                    LIMIT 1 
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE task_queues
                SET updated_at = $1
                FROM locked_task_queue
                WHERE task_queues.queue = locked_task_queue.queue 
                RETURNING task_queues.*;
                ",
            )
            .await?;

        let task_queue = match txn.query_opt(&stmt, &[&now]).await? {
            Some(row) => Some(TaskQueue::try_from(row)?),
            None => {
                if self.work_stealing {
                    // Otherwise just get the next work available from the queue.
                    let stmt = txn
                        .prepare_cached(
                            "
                            SELECT task_queues.*
                            FROM task_queues
                            WHERE scheduled_for <= $1
                            ORDER BY scheduled_for
                            LIMIT 1;
                            ",
                        )
                        .await?;

                    txn.query_opt(&stmt, &[&now])
                        .await?
                        .map(TaskQueue::try_from)
                        .transpose()?
                } else {
                    None
                }
            }
        };

        if let Some(task_queue) = task_queue {
            let stmt = txn
                .prepare_cached(
                    "
                    WITH locked_tasks AS (
                        SELECT queue, id
                        FROM tasks 
                        WHERE queue = $1
                        AND status = 'QUEUED'::task_status 
                        AND scheduled_for <= $2
                        ORDER BY scheduled_for ASC
                        LIMIT $3
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE tasks
                    SET status = 'RUNNING'::task_status
                    FROM locked_tasks
                    WHERE 
                        tasks.queue = locked_tasks.queue 
                        AND tasks.id = locked_tasks.id 
                    RETURNING tasks.*;    
                    ",
                )
                .await?;

            let tasks = txn
                .query(&stmt, &[&task_queue.queue, &now, &self.concurrency])
                .instrument(trace_span!("locked_tasks"))
                .await?
                .into_iter()
                .map(|row| Ok(Task::try_from(row)?))
                .collect::<Result<Vec<_>>>()?;

            if !tasks.is_empty() {
                return Ok(Some((task_queue, tasks)));
            }
        }

        Ok(None)
    }

    /// Update the task_queue table to record the next available task `scheduled_for`
    #[tracing::instrument(level = "trace", skip_all, ret)]
    pub async fn finalize_tasks(
        &self,
        txn: &Transaction<'_>,
        task_queue: TaskQueue,
    ) -> Result<TaskQueue> {
        let stmt = txn
            .prepare_cached(
                "
                UPDATE task_queues 
                SET 
                    scheduled_for = (
                        SELECT MIN(scheduled_for)
                        FROM tasks
                        WHERE queue = $1
                        AND status = 'QUEUED'::task_status
                    ),
                    updated_at = $2
                WHERE queue = $1
                RETURNING task_queues.*;
                ",
            )
            .await?;

        let task_queue = txn
            .query_one(&stmt, &[&task_queue.queue, &Utc::now()])
            .await?
            .try_into()?;

        Ok(task_queue)
    }
}
