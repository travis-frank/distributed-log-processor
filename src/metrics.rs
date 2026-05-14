use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::info;

pub struct Metrics {
    pub messages_processed: AtomicU64,
    pub total_latency_us: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
        }
    }

    pub fn record_processed(&self, duration: Duration) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, f64) {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        let total_latency_us = self.total_latency_us.load(Ordering::Relaxed);

        let avg_latency_us = if processed == 0 {
            0.0
        } else {
            total_latency_us as f64 / processed as f64
        };

        (processed, avg_latency_us)
    }
}

pub fn start_reporter(metrics: Arc<Metrics>) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        let mut previous_count = 0_u64;

        loop {
            interval.tick().await;

            let (total_processed, avg_latency_us) = metrics.snapshot();
            let processed_in_window = total_processed.saturating_sub(previous_count);
            let throughput_per_second = processed_in_window as f64 / 5.0;
            previous_count = total_processed;

            info!(
                total_processed = total_processed,
                throughput_per_second = throughput_per_second,
                average_latency_us = avg_latency_us,
                "processor metrics"
            );
        }
    });
}
