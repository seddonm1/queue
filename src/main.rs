use anyhow::Result;
use queue::run;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "error,queue=info");
    }
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_ids(false)
        // .with_span_events(FmtSpan::CLOSE)
        // display source code file paths
        // .with_file(true)
        // display source code line numbers
        // .with_line_number(true)
        // disable targets
        .with_target(false)
        // sets this to be the default, global collector for this application.
        .init();

    run().await
}
