# Proxy Throughput Performance Test

## What it proves

The async Akka-based proxy multiplexes far more concurrent clients than the number of OS threads it uses.

Each test scenario spins up:
- A **mock HTTP backend** that sleeps `PERF_DELAYS` ms per request (simulates real I/O work)
- A **proxy pipeline**: `HeaderProcessor → PoolProcessor(round-robin) → RetryProcessor → HttpProcessor`
- An **ActorSystem dispatcher capped at exactly `PERF_THREADS` threads**

All `PERF_CLIENTS` requests are fired simultaneously. The test measures wall-clock time and compares it against the **blocking-thread lower bound**:

```
blocking_estimate = clients × delay_ms / threads
```

A thread-per-request server cannot finish faster than that bound. The async proxy finishes in roughly `delay_ms + overhead`, regardless of client count.

Two assertions are checked where meaningful:

| Assertion | Condition |
|-----------|-----------|
| `elapsed < blocking_estimate` | `delay > 0` and `blocking_estimate > 500 ms` |
| `peak_concurrent_backend_calls > threads` | `delay > 0` and `clients > threads` |

## Running

### Full matrix (27 scenarios, ~45 s)

```bash
sbt "testOnly io.syspulse.ika.server.ProxyThroughputPerfSpec"
```

### Single scenario

```bash
PERF_THREADS=4 PERF_CLIENTS=100 PERF_DELAYS=100 \
  sbt "testOnly io.syspulse.ika.server.ProxyThroughputPerfSpec"
```

### Custom matrix

Set any of the three variables to a comma-separated list; omit a variable to keep its default.

```bash
# 4 and 8 threads, 100 and 1000 clients, delays 100 ms and 1000 ms only
PERF_THREADS=4,8 PERF_CLIENTS=100,1000 PERF_DELAYS=100,1000 \
  sbt "testOnly io.syspulse.ika.server.ProxyThroughputPerfSpec"

# Single worst-case scenario: 1 thread, 1000 clients, 1 s backend work
PERF_THREADS=1 PERF_CLIENTS=1000 PERF_DELAYS=1000 \
  sbt "testOnly io.syspulse.ika.server.ProxyThroughputPerfSpec"

# Zero-delay correctness sweep (no timing assertions, verifies request count only)
PERF_DELAYS=0 \
  sbt "testOnly io.syspulse.ika.server.ProxyThroughputPerfSpec"
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PERF_THREADS` | `1,4,8` | Dispatcher thread counts to test |
| `PERF_CLIENTS` | `10,100,1000` | Number of concurrent clients per scenario |
| `PERF_DELAYS` | `0,100,1000` | Backend work delay in milliseconds |

## Results

Measured on: Linux 6.14, OpenJDK 21, 2026-05-02

| Threads | Clients | Delay (ms) | Elapsed (ms) | Blocking est. (ms) | Speedup | Peak concurrent |
|--------:|--------:|-----------:|-------------:|-------------------:|--------:|----------------:|
| 1 | 10 | 0 | 312 | — | n/a | 1 |
| 1 | 10 | 100 | 202 | 1 000 | **5.0×** | 10 |
| 1 | 10 | 1 000 | 1 101 | 10 000 | **9.1×** | 10 |
| 1 | 100 | 0 | 405 | — | n/a | 1 |
| 1 | 100 | 100 | 301 | 10 000 | **33.2×** | 94 |
| 1 | 100 | 1 000 | 1 202 | 100 000 | **83.2×** | 100 |
| 1 | 1 000 | 0 | 1 205 | — | n/a | 1 |
| 1 | 1 000 | 100 | 1 305 | 100 000 | **76.6×** | 505 |
| 1 | 1 000 | 1 000 | 2 105 | 1 000 000 | **475×** | 990 |
| 4 | 10 | 0 | 100 | — | n/a | 2 |
| 4 | 10 | 100 | 200 | 250 | 1.3× | 10 |
| 4 | 10 | 1 000 | 1 101 | 2 500 | 2.3× | 10 |
| 4 | 100 | 0 | 100 | — | n/a | 2 |
| 4 | 100 | 100 | 201 | 2 500 | **12.4×** | 100 |
| 4 | 100 | 1 000 | 1 102 | 25 000 | **22.7×** | 100 |
| 4 | 1 000 | 0 | 1 425 | — | n/a | 3 |
| 4 | 1 000 | 100 | 3 206 | 25 000 | **7.8×** | 434 |
| 4 | 1 000 | 1 000 | 3 804 | 250 000 | **65.7×** | 716 |
| 8 | 10 | 0 | 100 | — | n/a | 1 |
| 8 | 10 | 100 | 200 | 125 | 0.6× | 10 |
| 8 | 10 | 1 000 | 1 101 | 1 250 | 1.1× | 10 |
| 8 | 100 | 0 | 100 | — | n/a | 1 |
| 8 | 100 | 100 | 201 | 1 250 | **6.2×** | 100 |
| 8 | 100 | 1 000 | 1 102 | 12 500 | **11.3×** | 100 |
| 8 | 1 000 | 0 | 2 103 | — | n/a | 2 |
| 8 | 1 000 | 100 | 3 506 | 12 500 | **3.6×** | 279 |
| 8 | 1 000 | 1 000 | 3 305 | 125 000 | **37.8×** | 524 |

### Key observations

**1 thread, 1000 clients, 1000 ms work** — the extreme case:
- Async proxy finishes in **2.1 s**
- A blocking server would need **1000 s** (16+ minutes)
- **475× speedup**; 990 requests in-flight simultaneously on a single thread

**Delay = 0 ms** rows show correctness-only results — all requests succeed, but the gap between async and blocking is unmeasurable at this timescale so no timing assertion is made.

**Small client counts (10) with 8 threads** — speedup < 1× because the blocking estimate itself is small (125–1250 ms) and the async overhead is comparable; the timing assertion is intentionally skipped for these cases.

The `peak` column is the most direct proof: with **1 thread** and **1000 clients with 1000 ms work**, up to **990 requests were simultaneously in-flight** at the backend — orders of magnitude more than the 1 available thread.
