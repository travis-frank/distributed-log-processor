# distributed-log-processor

A high-throughput TCP log ingestion engine built in Rust. Accepts thousands of
concurrent connections, processes structured JSON messages through a sharded
worker pool, and aggregates results in memory with sub-microsecond latency.

Built to explore concurrency tradeoffs in systems programming: async I/O,
bounded channel backpressure, lock-free vs mutex-based data structures,
and thread scaling behavior.

---

## Architecture

```
TCP Clients (thousands)
        │
        ▼
  TCP Listener (Tokio)          — one lightweight task per connection
        │
        ▼
  Bounded MPSC Channel          — backpressure point: blocks when full
        │
        ▼
  Sharded Worker Pool           — N OS threads, routed by account_id hash
        │
        ▼
  In-Memory Aggregator          — DashMap (sharded) or Mutex<HashMap> (comparison)
        │
        ▼
  Metrics Reporter              — throughput + latency logged every 5s
```

Each connection handler is a Tokio async task — lightweight enough to run
thousands concurrently without OS thread overhead. Workers are dedicated OS
threads pulling from per-shard crossbeam channels. Account IDs are hashed to
route each message to the same worker every time, eliminating cross-worker
contention entirely.

---

## Results (Apple M1, 8 cores, 16GB RAM)

**Peak throughput:** 1,000,000 msg/sec sustained (8 workers, 5,000 concurrent clients)

**Latency:** 0.05–0.45 µs average processing latency under load

**Memory:** ~43MB at 10,000 concurrent connections (Tokio tasks, not OS threads)

**DashMap vs Mutex end-to-end (8 workers):** 0.45 µs vs 3.90 µs — **8.7× lower latency**

**DashMap vs Mutex isolated (Criterion, 8 threads):** 2.51ms vs 31.18ms — **12.4× faster**

Full results: [BENCHMARKS.md](BENCHMARKS.md)

---

## Quick Start

```bash
# Build
cargo build --release

# macOS: raise file descriptor limit for high connection counts
ulimit -n 65536

# Run with 8 sharded workers
cargo run --release -- --workers 8 --capacity 5000

# Send a test message
echo '{"timestamp":"2024-01-15T09:30:00Z","account_id":"acc_0001","amount":100.0,"type":"deposit","currency":"USD"}' | nc localhost 8080

# Rust load generator (accurate throughput measurement)
cargo run --release --bin load_gen -- --clients 1000 --messages 1000

# Python stress tester (no build required)
python3 tools/stress_test.py --clients 100 --messages 100

# Criterion micro-benchmarks (no server needed)
cargo bench
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 8080 | TCP port to listen on |
| `--workers` | 4 | Worker thread count (1 = single async baseline) |
| `--capacity` | 1000 | Bounded channel depth (backpressure threshold) |
| `--aggregator` | dashmap | `dashmap` or `mutex` (for comparison benchmarks) |
| `--verbose` | false | Enable debug-level logging |

---

## Key Engineering Decisions

**Why Tokio async tasks per connection, not OS threads?**
OS threads consume ~8MB each. At 10,000 connections that's 80GB.
Tokio tasks are ~KB each, multiplexed across a small thread pool. This is how
production servers handle connection scale. Measured result: ~43MB at 10,000
concurrent connections.

**Why bounded channels?**
An unbounded channel lets producers run ahead of consumers indefinitely so memory
grows without limit until OOM. A bounded channel blocks the producer when full,
propagating backpressure to the client. The server stays stable under any load.
Measured result: 0% error rate across all tested capacities and client counts.

**Why account_id hash routing to worker shards?**
If all workers share one aggregator, writes to the same account from different
workers contend. By routing `acc_0001` always to worker 0 and `acc_0042` always
to worker 2, each worker exclusively owns its accounts: zero cross-worker
contention regardless of write volume.

**Why DashMap over Mutex\<HashMap\>?**
Single-threaded they're identical (133 ns vs 139 ns). Under 8 concurrent threads,
Mutex serializes all writes through one lock — 8.7× higher latency end-to-end,
12.4× slower in isolated Criterion benchmarks. The difference grows with thread
count. DashMap's internal sharding lets threads write to different accounts
simultaneously without blocking each other.

**Why crossbeam channels for worker queues, not tokio mpsc?**
Worker threads are synchronous menaing they loop, process, repeat. There's no need for async. `crossbeam::bounded` is a high-performance sync channel purpose-built for this pattern. Using `tokio::sync::mpsc` in a sync thread requires `.blocking_recv()` which adds overhead and muddies the async/sync boundary.

**Why a Rust load generator instead of Python?**
Python's async client caps at ~13k msg/sec on localhost due to interpreter overhead. The Rust load generator (`src/bin/load_gen.rs`) removes this bottleneck entirely, revealing true server throughput. The Python tester (`tools/stress_test.py`) remains useful for validating the server works with any TCP client.

---

## Project Structure

```
src/
  main.rs              — CLI config, runtime init, module wiring
  server.rs            — Tokio TCP listener, one task per connection
  worker.rs            — Single-task baseline + sharded thread pool
  aggregator.rs        — DashMap concurrent aggregator
  aggregator_mutex.rs  — Mutex<HashMap> aggregator (benchmarking only)
  message.rs           — LogEntry, EntryType, AccountStats structs
  metrics.rs           — AtomicU64 counters, 5-second throughput reporter
  lib.rs               — Re-exports for Criterion benchmarks
  bin/
    load_gen.rs        — Rust async load generator (BufWriter batching)

benches/
  processing.rs        — Criterion: parse, process, concurrent insert

tools/
  stress_test.py       — Python async TCP client, N concurrent connections
```

---

## Message Format

Newline-delimited JSON over TCP:

```json
{"timestamp":"2024-01-15T09:30:00Z","account_id":"acc_0001","amount":1500.00,"type":"deposit","currency":"USD"}
```

Valid types: `deposit`, `withdrawal`, `transfer`, `fee`
