mod aggregator;
mod message;
mod metrics;
mod server;
mod worker;

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Config {
    #[arg(short, long, default_value = "8080")]
    port: u16,
    #[arg(short, long, default_value = "1000")]
    capacity: usize,
    #[arg(long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(if config.verbose { "debug" } else { "info" }))
        .init();

    let aggregator = Arc::new(aggregator::Aggregator::new());
    let metrics = Arc::new(metrics::Metrics::new());
    let (tx, rx) = mpsc::channel(config.capacity);

    worker::start_worker(rx, Arc::clone(&aggregator), Arc::clone(&metrics));
    metrics::start_reporter(Arc::clone(&metrics));

    info!(
        port = config.port,
        capacity = config.capacity,
        verbose = config.verbose,
        "starting distributed-log-processor"
    );

    server::run(config.port, tx).await
}
