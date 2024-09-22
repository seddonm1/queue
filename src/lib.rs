mod task;
mod task_queue;
mod worker;

use anyhow::Result;
use chrono::Utc;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use rand::Rng;
use rand_distr::Normal;
use std::{ops::DerefMut, time::Duration};
use task_queue::TaskQueue;
// use testcontainers::{
//     core::{ContainerPort, WaitFor},
//     runners::AsyncRunner,
//     GenericImage, ImageExt,
// };
use tokio::task::JoinSet;
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
        Worker::create_task(
            &txn,
            Some(
                Utc::now()
                    + Duration::from_millis(10000)
                    + Duration::from_millis(rand::thread_rng().gen_range(0..10000)),
            ),
            None,
            None,
            &format!("queue_{}", rand::thread_rng().sample(normal) as u32),
        )
        .await?;
    }
    txn.commit().await?;

    let mut set = JoinSet::new();
    for _ in 0..100 {
        let cfg = cfg.clone();
        set.spawn(async move {
            Worker::new(
                &cfg,
                Some(Duration::from_millis(100)),
                Some(true),
                Some(100),
            )
            .run()
            .await
        });
    }

    while let Some(res) = set.join_next().await {
        println!("{:?}", res);
    }

    Ok(())
}
