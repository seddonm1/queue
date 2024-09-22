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

    #[allow(unused)]
    pub async fn retrieve(txn: &Transaction<'_>, queue: &str) -> Result<Option<Self>> {
        let stmt = txn
            .prepare_cached(
                "
                SELECT * 
                FROM task_queues
                WHERE queue = $1;
                ",
            )
            .await?;

        Ok(txn
            .query_opt(&stmt, &[&queue])
            .await?
            .map(TaskQueue::try_from)
            .transpose()?)
    }
}

impl TryFrom<Row> for TaskQueue {
    type Error = tokio_postgres::Error;

    fn try_from(value: Row) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            queue: value.try_get(0)?,
            scheduled_for: value.try_get(1)?,
            updated_at: value.try_get(2)?,
        })
    }
}
