# Benchmarks

**Machine:** Intel Core i5-1030NG7 @ 1.10GHz, 4 cores, 8GB RAM  
**OS:** macOS (Apple MacBook Air 2020)  
**Rust:** rustc 1.95.0 (59807616e 2026-04-14)  
**Test load:** `python3 tools/stress_test.py --clients 1000 --messages 100` (100k messages) unless noted

---

## Worker Scaling

Fixed: `--capacity 5000 --aggregator dashmap`, 1000 clients × 100 messages

| Workers | Throughput (msg/sec) | Avg Latency | Memory |
|---------|---------------------|-------------|--------|
| 1       | 6,935               | 1.99 µs     | 13.7 MB |
| 2       | 12,628              | 0.91 µs     | 13.2 MB |
| 4       | 11,751              | 1.09 µs     | 14.0 MB |
| 8       | 11,731              | 1.02 µs     | 15.5 MB |
| 16      | 12,143              | 1.01 µs     | ~16 MB  |

**Analysis:** Throughput roughly doubles from 1→2 workers (6.9k → 12.6k msg/sec), then
plateaus. This is expected on a 4-core Intel i5 — the CPU is saturated beyond 2 threads
for this workload. The Python stress tester is also a bottleneck at ~12-13k msg/sec;
the Rust server processes faster than Python can feed it. Avg latency drops and stabilizes
below 1.1µs across 2+ workers, confirming the worker threads are not the limiting factor.

---

## DashMap vs Mutex\<HashMap\>

End-to-end stress test bottlenecks on Python (~12k msg/sec) at this scale,
making the two aggregators appear similar in throughput. The Criterion benchmarks
below show the real difference under isolated concurrent load.

### Criterion: concurrent_insert_8_threads (8 threads × 1000 inserts, 100 unique accounts)

| Aggregator       | Time     | vs DashMap |
|------------------|----------|------------|
| DashMap          | 2.51 ms  | baseline   |
| Mutex\<HashMap\> | 31.18 ms | **12.4× slower** |

**Analysis:** Under single-threaded access, DashMap (133 ns) and Mutex (139 ns) are
nearly identical — both are fast with no contention. The difference appears under
concurrent load. When 8 threads simultaneously write to `Mutex<HashMap>`, every write
acquires a global lock — 7 threads block while 1 proceeds. This is lock convoy: threads
spend more time waiting than working. DashMap shards internally, so threads writing to
different accounts rarely contend. At 8 concurrent threads, DashMap completes the same
work 12× faster. This gap widens as thread count increases.

---

## Connection Scaling

Fixed: `--workers 8 --capacity 5000 --aggregator dashmap`, 100 messages per client

| Concurrent Clients | Total Messages | Throughput (msg/sec) | Error Rate | Memory  |
|--------------------|----------------|---------------------|------------|---------|
| 100                | 10,000         | 13,051              | 0.00%      | ~15 MB  |
| 1,000              | 100,000        | 11,533              | 0.00%      | ~15 MB  |
| 5,000              | 500,000        | 13,170              | 0.00%      | ~16 MB  |
| 10,000             | 1,000,000      | 13,757              | 0.00%      | ~16 MB  |

**Analysis:** The server handles 10,000 concurrent TCP connections with zero errors
and stable throughput. Tokio spawns one lightweight task per connection rather than one
OS thread — this is why memory stays flat at ~16MB regardless of client count. An
OS-thread-per-connection model would consume ~8MB per thread and collapse at this scale.
Throughput is consistent across connection counts because the bottleneck is Python sending
speed, not the Rust accept loop or worker pool.

---

## Channel Capacity vs Backpressure

Fixed: `--workers 4 --aggregator dashmap`, 1000 clients × 100 messages

| Channel Capacity | Throughput (msg/sec) | Avg Latency | Notes |
|------------------|---------------------|-------------|-------|
| 100              | 10,937              | 1.09 µs     | Tight backpressure — clients block frequently |
| 1,000            | 11,335              | 1.11 µs     | Balanced |
| 5,000            | 11,628              | 1.00 µs     | Smooth throughput |
| 10,000           | 10,515              | 1.26 µs     | Large buffer — higher latency spike on burst |

**Analysis:** At capacity=100, the bounded channel fills quickly and forces connection
handlers to block on `tx.send().await` — this IS backpressure working as designed.
Clients slow down rather than the server running out of memory. Counterintuitively,
capacity=10000 shows slightly lower throughput and higher latency: a large buffer allows
many messages to pile up before workers drain them, increasing average time-in-queue.
Capacity=1000–5000 is the sweet spot for this hardware: enough buffer to absorb bursts
without unbounded memory growth. In production, capacity should be tuned to
`target_throughput × acceptable_queue_time_seconds`.

---

## Criterion Micro-benchmarks

Run with: `cargo bench`

These isolate the cost of individual operations, independent of network overhead.

| Benchmark                              | Time     | What it measures |
|----------------------------------------|----------|-----------------|
| parse_message                          | 740 ns   | JSON → LogEntry deserialization |
| process_entry (DashMap)                | 133 ns   | Single-threaded DashMap write |
| process_entry (Mutex\<HashMap\>)       | 139 ns   | Single-threaded Mutex write |
| concurrent_insert/dashmap (8t × 1000) | 2.51 ms  | DashMap under 8-thread contention |
| concurrent_insert/mutex (8t × 1000)   | 31.18 ms | Mutex under 8-thread contention |

JSON parsing (740 ns) dominates per-message cost — it costs 5× more than the DashMap
write itself. This is a good target for future optimization: a binary protocol like
MessagePack or FlatBuffers would reduce this significantly.

---

## How to Reproduce

```bash
# Build
cargo build --release

# Terminal 1 — start server
cargo run --release -- --workers 8 --capacity 5000

# Terminal 2 — run stress test
python3 tools/stress_test.py --clients 1000 --messages 100

# Criterion benchmarks (no server needed)
cargo bench

# Memory monitoring during stress test
ps aux | grep distributed-log | grep -v grep | awk '{print $6/1024 " MB"}'

# Switch aggregator at runtime
cargo run --release -- --workers 8 --aggregator mutex --capacity 1000
```