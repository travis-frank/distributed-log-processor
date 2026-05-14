use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time::Instant;

const TIMESTAMP: &str = "2024-01-15T09:30:00Z";
const CURRENCY: &str = "USD";
const ENTRY_TYPES: [&str; 4] = ["deposit", "withdrawal", "transfer", "fee"];

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value_t = 100)]
    clients: usize,
    #[arg(long, default_value_t = 1000)]
    messages: usize,
    #[arg(long)]
    duration: Option<u64>,
}

fn make_entry_json(rng: &mut StdRng) -> String {
    let account_id = format!("acc_{:04}", rng.gen_range(0..1000));
    let amount = rng.gen_range(10.0..=50000.0);
    let entry_type = ENTRY_TYPES[rng.gen_range(0..ENTRY_TYPES.len())];

    format!(
        r#"{{"timestamp":"{TIMESTAMP}","account_id":"{account_id}","amount":{amount:.2},"type":"{entry_type}","currency":"{CURRENCY}"}}"#
    )
}

async fn run_client(
    host: String,
    port: u16,
    messages: usize,
    duration_secs: Option<u64>,
    sent: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
    let target = format!("{host}:{port}");
    let stream = match TcpStream::connect(&target).await {
        Ok(stream) => stream,
        Err(_) => {
            let missed = if duration_secs.is_some() {
                1_u64
            } else {
                messages as u64
            };
            errors.fetch_add(missed, Ordering::Relaxed);
            return;
        }
    };
    let mut stream = BufWriter::new(stream);

    let mut rng = StdRng::from_entropy();
    let start = Instant::now();
    let mut local_sent = 0_u64;
    let mut local_errors = 0_u64;

    match duration_secs {
        Some(seconds) => {
            let max_duration = Duration::from_secs(seconds);
            while start.elapsed() < max_duration {
                let mut line = make_entry_json(&mut rng);
                line.push('\n');

                if stream.write_all(line.as_bytes()).await.is_ok() {
                    local_sent += 1;
                } else {
                    local_errors += 1;
                    break;
                }
            }
        }
        None => {
            for _ in 0..messages {
                let mut line = make_entry_json(&mut rng);
                line.push('\n');

                if stream.write_all(line.as_bytes()).await.is_ok() {
                    local_sent += 1;
                } else {
                    local_errors += 1;
                    break;
                }
            }
        }
    }

    if stream.flush().await.is_err() {
        local_errors += 1;
    }
    let _ = stream.shutdown().await;
    sent.fetch_add(local_sent, Ordering::Relaxed);
    errors.fetch_add(local_errors, Ordering::Relaxed);
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(config.clients);
    for _ in 0..config.clients {
        handles.push(tokio::spawn(run_client(
            config.host.clone(),
            config.port,
            config.messages,
            config.duration,
            Arc::clone(&sent),
            Arc::clone(&errors),
        )));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_sent = sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let attempted = total_sent + total_errors;
    let throughput = if elapsed > 0.0 {
        total_sent as f64 / elapsed
    } else {
        0.0
    };
    let error_rate = if attempted > 0 {
        (total_errors as f64 / attempted as f64) * 100.0
    } else {
        0.0
    };

    println!("Total messages sent: {total_sent}");
    println!("Total errors: {total_errors}");
    println!("Elapsed time (s): {elapsed:.3}");
    println!("Throughput (messages/sec): {throughput:.2}");
    println!("Error rate (%): {error_rate:.2}");
}
