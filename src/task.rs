use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone, Debug, FromSql)]
#[postgres(name = "task_status", rename_all = "UPPERCASE")]
pub enum TaskStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct Task {
    pub queue: String,
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub scheduled_for: DateTime<Utc>,
    pub status: TaskStatus,
    pub args: serde_json::Value,
    pub max_retries: i16,
    pub errors: Vec<TaskError>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSql, FromSql)]
pub struct TaskError {
    ts: DateTime<Utc>,
    error: String,
}

impl TryFrom<Row> for Task {
    type Error = tokio_postgres::Error;

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

impl Task {
    pub async fn work(&self) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }

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

    #[tracing::instrument(level = "trace", skip_all, fields(id = ?self.id), ret)]
    pub async fn delete(&self, txn: &Transaction<'_>) -> Result<Self> {
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

    /// https://worker.graphile.org/docs/exponential-backoff
    #[tracing::instrument(level = "trace", skip_all, fields(id = ?self.id), ret)]
    pub async fn fail(&self, txn: &Transaction<'_>, error: anyhow::Error) -> Result<Self> {
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
