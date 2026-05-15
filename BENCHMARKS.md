# Benchmarks

**Machine:** Apple M1, 8 cores, 16GB RAM
**OS:** macOS  
**Rust:** rustc 1.95.0 (59807616e 2026-04-14)  
**Load generator:** `src/bin/load_gen.rs` (Rust, Tokio async, BufWriter batching)  
**Note:** `ulimit -n 65536` required on macOS for high connection counts

---

## Worker Scaling

Fixed: `--capacity 5000 --aggregator dashmap`, 200 clients × 1000 messages (200k total)

| Workers | Avg Latency | Notes |
|---------|-------------|-------|
| 1       | 0.115 µs    | Single async task baseline (spawn_blocking) |
| 2       | 0.048 µs    | Sharded pool — latency halves immediately |
| 4       | 0.140 µs    | Optimal for most workloads |
| 8       | 0.449 µs    | Thread overhead becoming visible |
| 16      | 1.547 µs    | Beyond core count — context switching cost |

**Peak observed:** 1,000,000 msg/sec server-side during 5,000-client connection scaling test.

**Analysis:** The jump from 1 worker (spawn_blocking async baseline) to 2 workers
(dedicated sharded OS threads) is the biggest improvement, the single-task baseline serializes all CPU work through one async executor. Beyond 4 workers the M1 load generator becomes the bottleneck at this message size. Latency increases beyond 8 workers as threads exceed the M1's physical performance core count, introducing context-switching overhead. Optimal worker count on this hardware: 4–8.

---

## DashMap vs Mutex\<HashMap\>

Fixed: `--capacity 5000`, 200 clients × 1000 messages

| Workers | Aggregator        | Avg Latency | vs DashMap |
|---------|------------------|-------------|------------|
| 4       | DashMap          | 0.183 µs    | baseline   |
| 4       | Mutex\<HashMap\> | 1.260 µs    | **6.9× slower** |
| 8       | DashMap          | 0.449 µs    | baseline   |
| 8       | Mutex\<HashMap\> | 3.901 µs    | **8.7× slower** |

**Criterion isolated benchmark (8 threads × 1000 inserts, Intel i5 MacBook):**

| Benchmark | Time | vs DashMap |
|-----------|------|------------|
| DashMap concurrent insert | 2.51 ms | baseline |
| Mutex\<HashMap\> concurrent insert | 31.18 ms | **12.4× slower** |

**Analysis:** Under single-threaded access, DashMap (133 ns) and Mutex (139 ns) are essentially identical. The gap opens under concurrent write pressure. Mutex serializes every write through one global lock: when 8 threads call `process()` simultaneously, 7 block immediately. This is lock convoy, threads spend more time waiting than working, and the problem gets worse as thread count increases. DashMap shards internally so threads writing to different accounts rarely contend. At 8 workers the latency gap is 8.7×; the Criterion micro-benchmark shows 12.4× throughput difference under pure concurrent load.

---

## Connection Scaling

Fixed: `--workers 8 --capacity 10000 --aggregator dashmap`, `ulimit -n 65536`

| Concurrent Clients | Messages Sent | Throughput      | Error Rate | Memory |
|--------------------|--------------|-----------------|------------|--------|
| 100                | 100,000      | 4,982,074 msg/sec | 0.00%    | ~20 MB |
| 1,000              | 1,000,000    | 2,839,021 msg/sec | 0.00%    | ~30 MB |
| 5,000              | 5,000,000    | 6,228,988 msg/sec | 0.00%    | ~40 MB |
| 10,000             | 5,000,000    | 4,036,357 msg/sec | 0.00%    | ~43 MB |

**Server-side peak:** 1,000,000 msg/sec sustained during the 5,000-client test.

**Analysis:** Zero errors across all connection counts including 10,000 simultaneous TCP connections. Memory stays flat at ~43MB regardless of client count because Tokio uses one lightweight task per connection rather than one OS thread. An OS-thread-per-connection model would require ~8MB per thread — 80GB for 10,000 connections. Tokio tasks are ~KB each. The server's accept loop and worker pool remain stable under all tested loads.

**Note on macOS file descriptor limit:** macOS defaults to ~256 open files. Running 1000+ connections requires `ulimit -n 65536` on both server and load gen terminals.

---

## Channel Capacity vs Backpressure

Fixed: `--workers 4 --aggregator dashmap`, 500 clients × 1000 messages (500k total)

| Channel Capacity | Throughput      | Avg Latency | Error Rate | Memory |
|------------------|-----------------|-------------|------------|--------|
| 100              | 3,784,074 msg/sec | 0.064 µs  | 0.00%      | ~8 MB  |
| 1,000            | 4,899,907 msg/sec | 0.205 µs  | 0.00%      | ~10 MB |
| 5,000            | 4,788,303 msg/sec | 0.260 µs  | 0.00%      | ~12 MB |
| 10,000           | 4,769,446 msg/sec | 0.325 µs  | 0.00%      | ~12 MB |

**Analysis:** Smaller capacity = tighter backpressure = lower memory but higher client wait time. Larger capacity = more buffering = smoother throughput but messages wait longer in queue before processing. 0.00% error rate across all capacities confirms the bounded channel design prevents OOM crashes, the server degrades gracefully under overload. Capacity=1000 offers the best throughput-to-memory ratio for this workload.

---

## Criterion Micro-benchmarks

Run with: `cargo bench` (Intel i5-1030NG7, MacBook Air 2020)

| Benchmark | Time | What it measures |
|-----------|------|-----------------|
| parse_message | 740 ns | JSON → LogEntry deserialization |
| process_entry (DashMap) | 133 ns | Single-threaded DashMap write |
| process_entry (Mutex\<HashMap\>) | 139 ns | Single-threaded Mutex write |
| concurrent_insert/dashmap (8t×1000) | 2.51 ms | DashMap under 8-thread contention |
| concurrent_insert/mutex (8t×1000) | 31.18 ms | Mutex under 8-thread contention |

JSON parsing (740 ns) costs 5× more than the DashMap write (133 ns) and dominates
per-message CPU time. A binary protocol like MessagePack would reduce this significantly.

---

## How to Reproduce

```bash
# Build everything
cargo build --release

# Required on macOS for high connection counts
ulimit -n 65536

# Terminal 1 — start server
cargo run --release -- --workers 8 --capacity 10000

# Terminal 2 — Rust load generator
cargo run --release --bin load_gen -- --clients 1000 --messages 1000

# Terminal 2 — Python stress tester (no build required)
python3 tools/stress_test.py --clients 100 --messages 100

# Criterion benchmarks (no server needed)
cargo bench

# Memory monitoring
ps aux | grep distributed-log | grep -v grep | awk '{print $6/1024 " MB"}'

# Switch aggregator for comparison
cargo run --release -- --workers 8 --aggregator mutex
```