use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;
use uuid::Uuid;

/// Enum representing the status of a task.
#[derive(Clone, Debug, FromSql)]
#[postgres(name = "task_status", rename_all = "UPPERCASE")]
pub enum TaskStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Struct representing a task in the system.
#[derive(Clone, Debug)]
#[allow(unused)]
pub struct Task {
    /// The queue this task belongs to.
    pub queue: String,
    /// Unique identifier for the task.
    pub id: Uuid,
    /// Timestamp when the task was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp when the task was last updated.
    pub updated_at: DateTime<Utc>,
    /// Timestamp when the task is scheduled to run.
    pub scheduled_for: DateTime<Utc>,
    /// Current status of the task.
    pub status: TaskStatus,
    /// Arguments associated with the task.
    pub args: serde_json::Value,
    /// Maximum number of retries allowed for the task.
    pub max_retries: i16,
    /// Errors encountered during execution of the task.
    pub errors: Vec<TaskError>,
}

/// Struct representing an error encountered during task execution.
#[derive(Clone, Debug, Serialize, Deserialize, ToSql, FromSql)]
pub struct TaskError {
    ts: DateTime<Utc>,
    error: String,
}

impl Task {
    /// Inserts a new task into the database.
    pub async fn insert(
        txn: &Transaction<'_>,
        queue: &str,
        id: Uuid,
        scheduled_for: DateTime<Utc>,
        max_retries: i16,
        args: serde_json::Value,
    ) -> Result<()> {
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

        txn.execute(&stmt, &[&queue, &id, &scheduled_for, &max_retries, &args])
            .await?;

        Ok(())
    }

    /// Retreive a set of Task from the queue and take ownership of them by locking them.
    pub async fn retrieve_tasks(
        txn: &Transaction<'_>,
        queue: &str,
        ts: &DateTime<Utc>,
        concurrency: i64,
    ) -> Result<Vec<Self>> {
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

        txn.query(&stmt, &[&queue, ts, &concurrency])
            .await?
            .into_iter()
            .map(|row| Ok(Task::try_from(row)?))
            .collect::<Result<Vec<_>>>()
    }

    /// Deletes the task from the database.
    #[tracing::instrument(level = "trace", skip_all, fields(id = ?self.id), ret)]
    pub async fn success(&self, txn: &Transaction<'_>) -> Result<Self> {
        let stmt = txn
            .prepare_cached(
                "
                UPDATE tasks
                SET
                    status = 'SUCCEEDED'::task_status,
                    updated_at = $3
                WHERE queue = $1 AND id = $2
                RETURNING tasks.*;
                ",
            )
            .await?;

        Ok(Task::try_from(
            txn.query_one(&stmt, &[&self.queue, &self.id, &Utc::now()])
                .await?,
        )?)
    }

    /// Updates the task with and Error. Using the max_retries field the status can be calculated as
    /// 'Queued' or 'Failed' and a new scheduled_for time can be calculated using [expontential-backoff](https://worker.graphile.org/docs/exponential-backoff).
    #[tracing::instrument(level = "trace", skip_all, fields(id = ?self.id), ret)]
    pub async fn failure(&self, txn: &Transaction<'_>, error: anyhow::Error) -> Result<Self> {
        let stmt = txn
            .prepare_cached(
                "
                UPDATE tasks
                SET
                    updated_at = $3,
                    errors = ARRAY_APPEND(errors, $4),
                    status = CASE
                        WHEN CARDINALITY(errors) = max_retries - 1 THEN 'FAILED'::task_status
                        ELSE 'QUEUED'::task_status
                    END,
                    scheduled_for = CASE
                        WHEN CARDINALITY(errors) = max_retries - 1 THEN scheduled_for
                        ELSE scheduled_for + EXP(LEAST(10, CARDINALITY(errors))) * INTERVAL '1 second'
                    END
                WHERE queue = $1 AND id = $2
                RETURNING tasks.*;
                ",
            )
            .await?;

        let now = Utc::now();

        Ok(Task::try_from(
            txn.query_one(
                &stmt,
                &[
                    &self.queue,
                    &self.id,
                    &now,
                    &serde_json::to_value(TaskError {
                        ts: now,
                        error: error.to_string(),
                    })
                    .unwrap(),
                ],
            )
            .await?,
        )?)
    }
}

impl TryFrom<Row> for Task {
    type Error = tokio_postgres::Error;

    /// Converts a database row into a `Task` instance.
    fn try_from(value: Row) -> std::result::Result<Self, Self::Error> {
        let errors = value
            .try_get::<_, Vec<serde_json::Value>>(8)?
            .into_iter()
            .map(|value| Ok(serde_json::from_value(value)?))
            .collect::<Result<Vec<TaskError>>>()
            .unwrap();

        Ok(Self {
            queue: value.try_get(0)?,
            id: value.try_get(1)?,
            created_at: value.try_get(2)?,
            updated_at: value.try_get(3)?,
            scheduled_for: value.try_get(4)?,
            status: value.try_get(5)?,
            args: value.try_get(6)?,
            max_retries: value.try_get(7)?,
            errors,
        })
    }
}
