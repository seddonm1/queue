use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use tokio_postgres::Row;

#[derive(Debug)]
pub struct TaskQueue {
    pub queue: String,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl TaskQueue {
    pub async fn upsert(&self, txn: &Transaction<'_>) -> Result<()> {
        let stmt = txn
            .prepare_cached(
                "
                INSERT INTO task_queues (
                    queue,
                    scheduled_for,
                    updated_at
                ) VALUES (
                    $1,
                    $2,
                    $3
                )
                ON CONFLICT (
                    queue
                )
                DO UPDATE SET
                    scheduled_for = EXCLUDED.scheduled_for,
                    updated_at = EXCLUDED.updated_at;
                ",
            )
            .await?;

        txn.execute(&stmt, &[&self.queue, &self.scheduled_for, &self.updated_at])
            .await?;

        Ok(())
    }

    /// Retrieve the next task_queue that has available work respecting locked rows.
    pub async fn retrieve_next_skip_locked(
        txn: &Transaction<'_>,
        ts: &DateTime<Utc>,
    ) -> Result<Option<Self>> {
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

        Ok(txn
            .query_opt(&stmt, &[ts])
            .await?
            .map(TaskQueue::try_from)
            .transpose()?)
    }

    /// Retrieve the next task_queue that has available_work ignoring any locking.
    pub async fn retrieve_next_ignore_locks(
        txn: &Transaction<'_>,
        ts: &DateTime<Utc>,
    ) -> Result<Option<Self>> {
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

        Ok(txn
            .query_opt(&stmt, &[ts])
            .await?
            .map(TaskQueue::try_from)
            .transpose()?)
    }

    /// Update the task_queue table to record the next available task `scheduled_for`.
    pub async fn update_scheduled_for(&self, txn: &Transaction<'_>) -> Result<Self> {
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
            .query_one(&stmt, &[&self.queue, &Utc::now()])
            .await?
            .try_into()?;

        Ok(task_queue)
    }
}

impl TryFrom<Row> for TaskQueue {
    type Error = tokio_postgres::Error;

    /// Converts a database row into a `TaskQueue` instance.
    fn try_from(value: Row) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            queue: value.try_get(0)?,
            scheduled_for: value.try_get(1)?,
            updated_at: value.try_get(2)?,
        })
    }
}
