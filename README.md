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

## Results (Intel i5-1030NG7, 4 cores, 8GB RAM)

**Throughput:** 13,757 msg/sec sustained across 10,000 concurrent connections, 0% error rate

**Latency:** 0.7–1.4 µs average processing latency under load

**Memory:** ~16MB at 10,000 concurrent connections (Tokio tasks, not OS threads)

**DashMap vs Mutex under 8 concurrent threads:** 2.5ms vs 31.2ms — **12.4× faster**

Full results in [BENCHMARKS.md](BENCHMARKS.md).

---

## Quick Start

```bash
# Build
cargo build --release

# Run with 4 sharded workers
cargo run --release -- --workers 4 --capacity 5000

# Send a test message
echo '{"timestamp":"2024-01-15T09:30:00Z","account_id":"acc_0001","amount":100.0,"type":"deposit","currency":"USD"}' | nc localhost 8080

# Run stress test (requires server running in another terminal)
python3 tools/stress_test.py --clients 1000 --messages 100

# Run Criterion benchmarks (no server needed)
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
OS threads consume ~8MB each. At 10,000 connections that's 80GB — impossible.
Tokio tasks are ~KB each, multiplexed across a small thread pool. This is how
production servers (nginx, Node.js) handle connection scale.

**Why bounded channels?**  
An unbounded channel lets producers run ahead of consumers indefinitely —
memory grows without limit under load until the process is OOM-killed.
A bounded channel blocks the producer when full, propagating backpressure to
the client. The server stays stable under any load; clients slow down instead
of the server crashing.

**Why account_id hash routing to shards?**  
If all workers share one DashMap, writes to the same account from different
workers still contend on that entry's shard. By routing `acc_0001` always to
worker 0 and `acc_0042` always to worker 2, each worker exclusively owns its
accounts. Zero cross-worker contention, regardless of write volume.

**Why DashMap over Mutex\<HashMap\>?**  
Under single-threaded access they're identical (133 ns vs 139 ns). Under 8
concurrent threads, Mutex serializes all writes through one lock: 12.4× slower
than DashMap's internal sharding. The Criterion benchmarks demonstrate this
directly: `concurrent_insert/dashmap` = 2.51ms, `concurrent_insert/mutex` = 31.18ms.

**Why crossbeam channels for worker queues, not tokio mpsc?**  
Worker threads are synchronous — they loop, process, repeat. There's no need
for async. `crossbeam::bounded` is a high-performance sync channel purpose-built
for this pattern. Using `tokio::sync::mpsc` in a sync thread would require
`.blocking_recv()` which adds overhead and muddies the async/sync boundary.

---

## Project Structure

```
src/
  main.rs           — CLI config, runtime init, module wiring
  server.rs         — Tokio TCP listener, one task per connection
  worker.rs         — Single-task baseline + sharded thread pool
  aggregator.rs     — DashMap concurrent aggregator
  aggregator_mutex.rs — Mutex<HashMap> aggregator (for benchmarking)
  message.rs        — LogEntry, EntryType, AccountStats structs
  metrics.rs        — AtomicU64 counters, 5-second throughput reporter
  lib.rs            — Re-exports for Criterion benchmarks

benches/
  processing.rs     — Criterion: parse, process, concurrent insert

tools/
  stress_test.py    — Async Python TCP client, N concurrent connections
```

---

## Message Format

Newline-delimited JSON over TCP:

```json
{"timestamp":"2024-01-15T09:30:00Z","account_id":"acc_0001","amount":1500.00,"type":"deposit","currency":"USD"}
```

Valid types: `deposit`, `withdrawal`, `transfer`, `fee`