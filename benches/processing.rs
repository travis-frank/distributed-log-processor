use std::sync::Arc;
use std::thread;

use chrono::{TimeZone, Utc};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use distributed_log_processor::aggregator::Aggregator;
use distributed_log_processor::aggregator_mutex::MutexAggregator;
use distributed_log_processor::message::{EntryType, LogEntry};

fn fixed_entry() -> LogEntry {
    LogEntry {
        timestamp: Utc.timestamp_opt(1_705_311_000, 0).single().unwrap(),
        account_id: "acc_0001".to_string(),
        amount: 1500.0,
        entry_type: EntryType::Deposit,
        currency: "USD".to_string(),
    }
}

fn bench_parse_message(c: &mut Criterion) {
    let json = r#"{"timestamp":"2024-01-15T09:30:00Z","account_id":"acc_0001","amount":1500.0,"type":"deposit","currency":"USD"}"#;

    c.bench_function("parse_message", |b| {
        b.iter(|| {
            let parsed: LogEntry = serde_json::from_str(black_box(json)).unwrap();
            black_box(parsed);
        })
    });
}

fn bench_process_dashmap(c: &mut Criterion) {
    let aggregator = Aggregator::new();
    let entry = fixed_entry();

    c.bench_function("process_dashmap", |b| {
        b.iter(|| aggregator.process(black_box(&entry)));
    });
}

fn bench_process_mutex(c: &mut Criterion) {
    let aggregator = MutexAggregator::new();
    let entry = fixed_entry();

    c.bench_function("process_mutex", |b| {
        b.iter(|| aggregator.process(black_box(&entry)));
    });
}

fn bench_concurrent_8_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_insert_8_threads");

    group.bench_function(BenchmarkId::new("dashmap", "8x1000"), |b| {
        b.iter(|| {
            let aggregator = Arc::new(Aggregator::new());
            let mut handles = Vec::with_capacity(8);

            for worker_id in 0..8 {
                let aggregator = Arc::clone(&aggregator);
                handles.push(thread::spawn(move || {
                    for i in 0..1000 {
                        let account_id = format!("acc_{:04}", (worker_id * 1000 + i) % 100);
                        let entry = LogEntry {
                            timestamp: Utc.timestamp_opt(1_705_311_000, 0).single().unwrap(),
                            account_id,
                            amount: 100.0,
                            entry_type: EntryType::Deposit,
                            currency: "USD".to_string(),
                        };
                        aggregator.process(&entry);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            black_box(aggregator.account_count());
        });
    });

    group.bench_function(BenchmarkId::new("mutex", "8x1000"), |b| {
        b.iter(|| {
            let aggregator = Arc::new(MutexAggregator::new());
            let mut handles = Vec::with_capacity(8);

            for worker_id in 0..8 {
                let aggregator = Arc::clone(&aggregator);
                handles.push(thread::spawn(move || {
                    for i in 0..1000 {
                        let account_id = format!("acc_{:04}", (worker_id * 1000 + i) % 100);
                        let entry = LogEntry {
                            timestamp: Utc.timestamp_opt(1_705_311_000, 0).single().unwrap(),
                            account_id,
                            amount: 100.0,
                            entry_type: EntryType::Deposit,
                            currency: "USD".to_string(),
                        };
                        aggregator.process(&entry);
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            black_box(aggregator.account_count());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_message,
    bench_process_dashmap,
    bench_process_mutex,
    bench_concurrent_8_threads
);
criterion_main!(benches);
