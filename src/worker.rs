// Baseline single-worker implementation.
// spawn_blocking tasks are not awaited — under sustained load this can
// accumulate unbounded blocking tasks. Replaced in Week 5 with a
// dedicated sharded thread pool.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::Receiver;
use tracing::debug;

use crate::aggregator::Aggregator;
use crate::message::LogEntry;
use crate::metrics::Metrics;

pub fn start_worker(
    mut rx: Receiver<LogEntry>,
    aggregator: Arc<Aggregator>,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        while let Some(entry) = rx.recv().await {
            let aggregator = Arc::clone(&aggregator);
            let metrics = Arc::clone(&metrics);

            tokio::task::spawn_blocking(move || {
                let start = Instant::now();
                aggregator.process(&entry);
                metrics.record_processed(start.elapsed());
            });
        }

        debug!("worker shutting down: channel closed");
    });
}
