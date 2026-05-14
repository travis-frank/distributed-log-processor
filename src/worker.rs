// Baseline single-worker implementation.
// spawn_blocking tasks are not awaited — under sustained load this can
// accumulate unbounded blocking tasks. Replaced in Week 5 with a
// dedicated sharded thread pool.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::Sender as CrossbeamSender;
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

pub struct ShardedPool {
    senders: Vec<CrossbeamSender<LogEntry>>,
}

impl ShardedPool {
    pub fn route(&self, entry: LogEntry) {
        let mut hasher = DefaultHasher::new();
        entry.account_id.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.senders.len();
        let _ = self.senders[index].send(entry);
    }
}

pub fn start_sharded_pool(
    worker_count: usize,
    aggregator: Arc<Aggregator>,
    metrics: Arc<Metrics>,
    capacity: usize,
) -> Arc<ShardedPool> {
    assert!(worker_count > 0, "worker_count must be greater than zero");

    let mut senders = Vec::with_capacity(worker_count);

    for i in 0..worker_count {
        let (tx, rx) = crossbeam_channel::bounded::<LogEntry>(capacity);
        senders.push(tx);

        let aggregator = Arc::clone(&aggregator);
        let metrics = Arc::clone(&metrics);

        thread::Builder::new()
            .name(format!("shard-worker-{i}"))
            .spawn(move || {
                for entry in &rx {
                    let start = Instant::now();
                    aggregator.process(&entry);
                    metrics.record_processed(start.elapsed());
                }
            })
            .expect("failed to spawn shard worker thread");
    }

    Arc::new(ShardedPool { senders })
}
