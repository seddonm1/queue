mod task;
mod task_queue;
mod worker;

use anyhow::Result;
use chrono::Utc;
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use rand::Rng;
use rand_distr::Normal;
use std::{ops::DerefMut, time::Duration};
use task::Task;
use task_queue::TaskQueue;
use tracing::{info_span, Instrument};
use uuid::Uuid;
// use testcontainers::{
//     core::{ContainerPort, WaitFor},
//     runners::AsyncRunner,
//     GenericImage, ImageExt,
// };
use tokio::{task::JoinSet, time};
use tokio_postgres::NoTls;
use worker::Worker;

mod embedded {
    refinery::embed_migrations!("./migrations");
}

pub async fn run() -> Result<()> {
    // // startup the module
    // let node = GenericImage::new("postgres", "16.4")
    //     .with_exposed_port(ContainerPort::Tcp(5432))
    //     .with_wait_for(WaitFor::message_on_stderr(
    //         "database system is ready to accept connections",
    //     ))
    //     .with_env_var("POSTGRES_PASSWORD", "postgres")
    //     .start()
    //     .await?;
    // cfg.port = Some(node.get_host_port_ipv4(5432).await?);

    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);
    cfg.user = Some("postgres".to_string());
    cfg.password = Some("postgres".to_string());
    cfg.dbname = Some("postgres".to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .clone()
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .unwrap();

    let mut conn = pool.get().await?;
    let client = conn.deref_mut().deref_mut();
    embedded::migrations::runner().run_async(client).await?;

    // fixtures
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let normal = Normal::new(5.0, 1.0).unwrap();

    for i in 0..10 {
        TaskQueue {
            queue: format!("queue_{i}"),
            scheduled_for: None,
            updated_at: Utc::now(),
        }
        .upsert(&txn)
        .await?;
    }
    for _ in 0i32..10_000 {
        Task::insert(
            &txn,
            &format!("queue_{}", rand::thread_rng().sample(normal) as u32),
            Uuid::new_v4(),
            Utc::now()
                + Duration::from_millis(30000)
                + Duration::from_millis(rand::thread_rng().gen_range(0..10000)),
            5,
            serde_json::Value::Object(serde_json::Map::new()),
        )
        .await?;
    }
    txn.commit().await?;

    let mut set = JoinSet::new();
    for _ in 0..200 {
        let cfg = cfg.clone();
        set.spawn(async move { ExampleWorker::new(&cfg).run().await });
    }

    while let Some(res) = set.join_next().await {
        println!("{:?}", res);
    }

    Ok(())
}

/// A worker in the task queue system that processes tasks.
pub struct ExampleWorker {
    /// Unique identifier for the worker.
    id: Uuid,
    /// Connection pool to interact with the database.
    pool: Pool,
}

impl ExampleWorker {
    /// Creates a new `ExampleWorker`.
    pub fn new(cfg: &Config) -> Self {
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        Self {
            id: Uuid::new_v4(),
            pool,
        }
    }
}

impl Worker for ExampleWorker {
    /// Unique identifier for the worker.
    fn id(&self) -> Uuid {
        self.id
    }

    /// Connection pool to interact with the database.
    fn pool(&self) -> &Pool {
        &self.pool
    }

    /// A function to do the work.
    #[allow(unused)]
    fn work(
        task: Task,
        pool: Pool,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send + 'static {
        let task_id = task.id;
        async move {
            time::sleep(Duration::from_millis(10)).await;
            Ok(())
        }
        .instrument(info_span!("task", id = ?task_id))
    }
}
