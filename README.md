# go-storm

**The Load Tester That Tells Truth.**

A high-performance HTTP load testing engine written in Go. Unlike other tools, go-storm detects when **YOUR generator is the bottleneck** — not just the target.

```

  ╔════════════════════════════════════╗
  ║ ⚡ go-storm                        ║
  ║ The Load Tester That Tells Truth   ║
  ╚════════════════════════════════════╝

```

[![CI](https://github.com/gostorm-dev/go-storm/actions/workflows/ci.yml/badge.svg)](https://github.com/gostorm-dev/go-storm/actions)
[![Go Report](https://goreportcard.com/badge/github.com/gostorm-dev/go-storm)](https://goreportcard.com/report/github.com/gostorm-dev/go-storm)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Why go-storm?

Most load testers show you **target metrics**. go-storm shows you **generator health** too.

When your load test shows slow responses, is it the **server** or **your machine**?

| Question | k6 | vegeta | go-storm |
|----------|:--:|:------:|:--------:|
| What's the target latency? | ✅ | ✅ | ✅ |
| What's the RPS? | ✅ | ✅ | ✅ |
| **Is my generator the bottleneck?** | ❌ | ❌ | ✅ |
| **CPU usage of load generator?** | ❌ | ❌ | ✅ |
| **Memory leak in generator?** | ❌ | ❌ | ✅ |
| **Worker utilization?** | ❌ | ❌ | ✅ |
| **Connection pool efficiency?** | ❌ | ❌ | ✅ |
| Generator saturation detection | ❌ | ❌ | ✅ |
| Actionable recommendations | ❌ | ❌ | ✅ |

**go-storm is the ONLY load tester that tells you if YOUR machine is the problem.**

---

## Benchmarks

Reproducible lab: 2 × c6i.large (same AZ, private-IP traffic), warmup + 3 runs,
median reported, `sar` monitoring on both machines. Full methodology and raw
numbers in [**BENCHMARKS.md**](BENCHMARKS.md) · suite in [`bench/`](bench/).

### Latest round (v0.5.7 — Round 3)

| Scenario | go-storm | k6 | Delta |
|----------|----------|-----|--------|
| Sustained soak (60s, c=100) | **46,018 RPS**, p95 4.40ms | 25,964 RPS, p95 8.68ms | **+77% throughput** |
| Generator ceiling (c=1000, 30s) | **39,687 RPS**, spread ±0.8% | 18,744 RPS | **2.1× throughput** |
| Rate accuracy (`-r 5000 -d 30s`) | **5000.00 RPS — exact dispatch**, p99 0.41ms at 18% generator CPU | 5000 RPS, p99 2.75ms @ 37% CPU | **6.7× lower tail latency** |
| Burst (100k req, c=100) | **45,583 RPS**, spread ±0.7% | hey: 45,489 ±2.8% | storm wins |

12,604,631 requests fired by go-storm this round — **zero failures**.
`%steal` <1% in every sar window; all tools otherwise within noise of prior
rounds, confirming the Round 3 gains come from the v0.5.7 pool fix.

Honest scoreboard: **wrk still wins raw single-endpoint throughput**
(64,774 RPS soak) — but ships no percentiles by default, no rate mode, no
generator health reporting. We publish our losses too — that is the point of
["The Load Tester That Tells Truth"](BENCHMARKS.md).

<details>
<summary>Round 1 (pre-v0.5.3 build): go-storm vs k6</summary>

| Scenario | go-storm | k6 | Delta |
|----------|----------|-----|--------|
| Sustained soak (60s, c=100) | **35,870 RPS**, p99 9.7ms | 24,332 RPS, p99 16.0ms | **+47% throughput, −39% p99** |
| Generator ceiling (c=1000, 30s) | **31,198 RPS** | 18,154 RPS | **+72% throughput** |

</details>

<details>
<summary>Round 2 (v0.5.3): rate-overshoot fix verified, soak regression documented</summary>

- S3 rate accuracy verified perfect: 5000.00 RPS median, exactly 150,000
  requests ×3 runs, p99 0.55ms @ 19% CPU (k6: p99 2.99ms @ 37%).
- Sustained soak collapsed to ~15K RPS with 66.5% kernel-space CPU.
  Investigation: no code regression (four-binary A/B), %steal ruled out,
  root cause = connection-pool churn — fixed in v0.5.7. Full trail in
  [BENCHMARKS.md](BENCHMARKS.md).

</details>

<details>
<summary>Early internal tests (single t3.medium, superseded by the lab above)</summary>

| Test | go-storm | k6 |
|------|----------|-----|
| 10K reqs, c=100 | **11,960 RPS** | 7,118 RPS |
| 200K reqs, c=2000 | **finished** | crashed (EOF errors) |
| 50K reqs @ 5000 RPS rate limit | **5,551 RPS** | 4,972 RPS |

</details>

### Key Numbers

```
go-storm (Round 3, dedicated c6i pair):
  ✅ 12.6M requests fired — ZERO failures
  ✅ +77% sustained throughput vs k6 (soak), 2.1× at ceiling
  ✅ Perfect rate delivery: 5000.00/5000, p99 0.41ms @ 18% CPU
  ✅ Run-to-run spread ±0.1–0.8% across all scenarios
  ✅ Generator health self-reported on every run

k6:
  ⚠️ 30–44% slower across soak and ceiling
  ⚠️ 6.7× worse tail latency in rate mode
  ❌ No generator health monitoring
```

---

## Features

### Core
- Concurrent workers with configurable pool size
- **Duration mode** (`-d 5m`) — sustained load for a fixed window, graceful drain, no fake errors at the deadline
- **Constant-arrival scheduling** (`-r` / `--rate`) — exactly `rate × duration` requests dispatched on fixed time slots; zero startup burst, zero overshoot
- GET / POST / PUT / DELETE / PATCH / HEAD
- Custom headers (`-H "Authorization: Bearer $TOKEN"`, repeatable)
- Per-request timeout
- Rich statistics: min/avg/max, p50/p90/p95/p99/p99.9 (sub-millisecond precision), RPS, success rate
- **Bounded-error percentiles** — every percentile carries a *guaranteed* ≤0.78% relative error, by construction (no distributional assumptions)
- Status code distribution
- Live progress bar with real-time RPS
- CI gate flags: `--fail-above-errors`, `--fail-above-p95` (exit 2 on violation)

### Output Formats
- `text` — human-readable with progress bar (default)
- `json` — machine-readable for CI/CD automation
- `table` — formatted table with borders
- `quiet` — numbers only, comma-separated
- `csv` — key-value CSV format

### Generator Health (UNIQUE)
- **Real-time monitoring**: CPU, memory, GC, goroutines, file descriptors
- **Saturation detection**: knows when YOUR machine is the bottleneck
- **Arrival accuracy**: did the generator hold its own schedule? On-time slot % + lag p50/p99/max
- **Health report**: post-test analysis with actionable recommendations
- **Capacity estimation** (`--estimate`): pre-test benchmark shows max RPS

### Connection Pooling
- 25x more connections per host (50 vs Go default 2)
- HTTP/2 support with multiplexing
- Buffer pooling (sync.Pool) — zero allocations on hot path
- Connection reuse tracking via httptrace

### Distributed Testing
- Redis-based job queue
- Multi-machine load generation
- Per-agent heartbeat and breakdown
- Automatic work distribution

### Observability
- Prometheus metrics endpoint
- Grafana dashboard (provisioned)
- Per-request latency histograms

---

## Installation

### Quick Install (Go 1.26+)

```bash
go install github.com/gostorm-dev/go-storm/cmd/storm@latest
```

### Pre-built Binaries

Download from [Releases](https://github.com/gostorm-dev/go-storm/releases):

```bash
# Linux amd64
curl -L https://github.com/gostorm-dev/go-storm/releases/latest/download/storm_linux_amd64.tar.gz | tar xz
chmod +x storm
sudo mv storm /usr/local/bin/
```

### Build from Source

```bash
git clone https://github.com/gostorm-dev/go-storm.git
cd go-storm
make build
```

---

## Quick Start

```bash
# Basic load test — 1000 requests, 50 workers
storm run -u https://example.com -n 1000 -c 50

# Sustained load — run for 5 minutes instead of a request count
storm run -u https://example.com -d 5m -c 200

# Constant arrival rate — hold exactly 1000 RPS for 30 minutes
storm run -u https://example.com -d 30m -c 100 -r 1000

# Rate limited — exactly 1000 RPS
storm run -u https://example.com -n 5000 -r 1000

# POST with JSON body
storm run -u https://api.example.com/users -m POST -b '{"name":"test"}'

# Authenticated request with custom headers
storm run -u https://api.example.com/me \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Trace: loadtest"

# CI gate — exit code 2 if >20 failures or p95 slower than 500ms
storm run -u https://staging.api.com/users -n 2000 -c 100 \
  --fail-above-errors 20 --fail-above-p95 500

# Save results as JSON
storm run -u https://example.com -n 1000 --format json --output result.json

# Capacity estimation — how fast can your server go?
storm run -u https://example.com --estimate

# Table output
storm run -u https://example.com -n 1000 --format table
```

---

## CLI Reference

### Commands

| Command | Description |
|---------|-------------|
| `storm run` | Run a local load test |
| `storm run-dist` | Run a distributed load test (Redis) |
| `storm agent` | Worker that pulls jobs from Redis |
| `storm report <file>` | Pretty-print a saved JSON report |
| `storm version` | Print version info |

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--url` | `-u` | *(required)* | Target URL |
| `--requests` | `-n` | `0` | Total requests (mutually exclusive with `--duration`) |
| `--duration` | `-d` | `0` | Test duration: `30s`, `5m`, `1h` (mutually exclusive with `--requests`) |
| `--concurrency` | `-c` | `10` | Parallel workers |
| `--method` | `-m` | `GET` | HTTP method |
| `--timeout` | `-t` | `10` | Request timeout (seconds) |
| `--rate` | `-r` | `0` | Rate limit RPS (0 = unlimited) |
| `--body` | `-b` | | Request body |
| `--header` | `-H` | | Custom header, repeatable: `-H "Key: Value"` |
| `--format` | | `text` | Output: `text`, `json`, `table`, `quiet`, `csv` |
| `--output` | | | Save report to file |
| `--fail-above-errors` | | `-1` *(off)* | Exit 2 if failed requests exceed N |
| `--fail-above-p95` | | `-1` *(off)* | Exit 2 if p95 latency exceeds MS ms |
| `--saturation` | | `true` | Generator health monitoring |
| `--estimate` | | `false` | Pre-test capacity estimation |
| `--saturation-kill` | | `false` | Terminate test on sustained critical generator saturation (CPU/GC/FDs/goroutines/memory) |
| `--metrics-port` | | `0` | Prometheus port (0 = disabled) |
| `--max-idle-conns` | | `0` *(auto)* | Max idle connections — auto-sizes to at least `2×concurrency` (min 256) |
| `--max-idle-per-host` | | `0` *(auto)* | Max idle connections per host (auto, same rule) |
| `--idle-timeout` | | `90` | Idle connection timeout (s) |
| `--keep-alive` | | `30` | TCP keep-alive (s) |
| `--force-http2` | | `true` | Force HTTP/2 |
| `--insecure` | | `false` | Skip TLS verify |

---

## CI/CD Integration

go-storm turns load tests into quality gates. By default it always exits `0` after a completed run — you opt in to failure conditions:

| Exit Code | Meaning |
|-----------|---------|
| `0` | Test ran, all thresholds passed (or none set) |
| `1` | Configuration error (bad URL, invalid header format) |
| `2` | **Threshold violation** — the service failed the gate |

```yaml
# .github/workflows/loadtest.yml
- name: Load test gate
  run: |
    storm run -u https://staging.api.com/users -n 2000 -c 100 \
      --fail-above-errors 20 --fail-above-p95 500
```

The pipeline goes red if more than 20 requests fail **or** p95 latency exceeds 500ms. The full report is always printed first, so failures come with complete debugging data.

---

## Sample Output

### Table Format

```
  ───────────────────┼────────────────────
  │ Metric            │              Value │
  ───────────────────┼────────────────────
  │ URL               │ https://example... │
  │ Method            │                GET │
  │ Workers           │                 50 │
  ───────────────────┼────────────────────
  │ Total             │              1,000 │
  │ Successful        │              1,000 │
  │ Failed            │                  0 │
  │ Success Rate      │            100.00% │
  ───────────────────┼────────────────────
  │ Avg Latency       │            7.00 ms │
  │ p50 Latency       │               5 ms │
  │ p90 Latency       │              10 ms │
  │ p95 Latency       │              10 ms │
  │ p99 Latency       │              50 ms │
  │ p99.9 Latency     │              80 ms │
  │ TTFB p50          │               4 ms │
  │ TTFB p95          │               6 ms │
  │ Min               │               0 ms │
  │ Max               │              47 ms │
  ───────────────────┼────────────────────
  │ RPS               │        11,960.10   │
  │ Duration          │             0.84 s │
  ───────────────────┴────────────────────

  Status Codes:
    200: 1,000
```

### Generator Health Report

```
═══════════════════════════════════════════════
        GENERATOR HEALTH REPORT
═══════════════════════════════════════════════

Load
  Target RPS:       Unlimited
  Achieved RPS:     11,960

System Resources
  CPU Usage:        46.7% ✅
  Memory:          16.5 MB (Heap: 4.9 MB)
  Goroutines:       57
  GC Cycles:        1
  GC Total Pause: 0.1 ms
  File Descriptors: 34

Connection Pool
  Connections Created:  27
  Connections Reused:   473
  Pool Hits:            473
  Pool Misses:          27
  Reuse Ratio:       94.6%

Arrival Schedule
  Dispatched:       50,000 / 50,000 slots
  Slot interval:    2.00 ms
  On-time slots:    99.4% (312 late) ✅
  Lag p50/p99/max:  0.08 / 1.10 / 6.42 ms

Checks
  ✅ CPU:                 46.7%
  ✅ GC Pause:            0.1 ms
  ✅ Goroutines:          57
  ✅ File Descriptors:    34
  ✅ Worker Utilization:  88.1%

───────────────────────────────────────────────
  ✅ GENERATOR HEALTHY
  Results are trustworthy.
───────────────────────────────────────────────
```

### JSON Output

```json
{
  "tool_version": "v0.5.6",
  "git_commit": "9f2c1ab...",
  "built_at": "2026-08-23T04:00:00Z",
  "url": "https://example.com",
  "method": "GET",
  "concurrency": 100,
  "total_requests": 10000,
  "successful": 10000,
  "failed": 0,
  "success_rate": 100,
  "avg_response_time_ms": 7.42,
  "p50_ms": 6.85,
  "p90_ms": 21.4,
  "p95_ms": 48.21,
  "p99_ms": 50,
  "p999_ms": 50.9,
  "ttfb_avg_ms": 5.1,
  "ttfb_p50_ms": 4.2,
  "ttfb_p95_ms": 6.9,
  "ttfb_p99_ms": 8.3,
  "requests_per_sec": 11960.10,
  "total_duration_ms": 836,
  "status_codes": { "200": 10000 }
}
```

Latency values are fractional milliseconds — sub-millisecond precision is preserved, so fast targets never report a misleading `0ms`.

Percentiles come from a log-linear histogram with a **guaranteed relative error ≤0.78%** — verified by property tests against exact sorted-slice computation (`pkg/storm/loghistogram_test.go`), not by assumption.

### What latency means

**Latency** = full response time: request sent → headers **and body** fully received. This matches vegeta, k6 and ab, so numbers are cross-tool comparable. (Older releases stopped the clock at first byte; see CHANGELOG.)

**TTFB** = request sent → response headers received. It isolates server processing + queueing from transfer time and is reported alongside latency in `text`, `table`, `csv` and JSON (`ttfb_*` fields) — most load testers don't expose it at all.

---

## Distributed Load Testing

Distribute load across multiple machines using Redis. The distributed engine is covered by an in-process integration suite (miniredis) — queue flow, agent lifecycle, TTL-based dead-agent detection, coordinator aggregation, and stale-run isolation are all tested; no live Redis needed to develop against it.

### Architecture

```
   storm run-dist               Redis                     storm agent × N
   +---------------+        +--------------+         +----------------+
   | PushJobs      |  LPUSH |  storm:jobs  | BLPOP  | pop → Execute   |
   | (job queue)   | -----> |  (shared)    | <----- | → PushResult    |
   | poll results  |        +--------------+         |                |
   | → Aggregate   |  LRANGE| storm:results| LPUSH  | (any machines)  |
   | + breakdown   |  <----  +--------------+  <-----+----------------+
   +---------------+        | storm:agent  |        | register + hb   |
                            +--------------+        +----------------+
```

### Quick Start

```bash
# 1. Start Redis
docker run -d -p 6379:6379 redis

# 2. Start agents (on each machine)
./storm agent -c 10 --name agent-1
./storm agent -c 10 --name agent-2

# 3. Run distributed test
./storm run-dist -u https://example.com -n 100000 --agents 2
```

---

## Live Metrics (Prometheus + Grafana)

```bash
# Start monitoring stack
docker run -d --name prometheus --network host \
  -v "$PWD/prometheus.yml:/etc/prometheus/prometheus.yml:ro" prom/prometheus

docker run -d --name grafana --network host \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -v "$PWD/grafana/provisioning:/etc/grafana/provisioning" \
  -v "$PWD/grafana/dashboards:/var/lib/grafana/dashboards" \
  grafana/grafana

# Start agents with metrics
./storm agent --name a --metrics-port 9091 --stay-alive &
./storm agent --name b --metrics-port 9092 --stay-alive &

# Run test
./storm run-dist -u https://example.com -n 10000 --agents 2
```

Open `http://localhost:3000` → Dashboards → Storm Load Test

---

## Project Structure

```
go-storm/
├── cmd/storm/                  # CLI (Cobra)
│   ├── main.go                 # Root command
│   ├── run.go                  # Local load test
│   ├── run_dist.go             # Distributed coordinator
│   ├── agent.go                # Distributed worker
│   ├── report.go               # Report viewer
│   ├── version.go              # Version info
│   └── banner.go               # ASCII banner
├── pkg/storm/                  # Core engine (public API)
│   ├── storm.go                # LoadTester + Run()
│   ├── config.go               # Config, Job, Result, Stats
│   ├── execute.go              # HTTP execution
│   ├── scheduler.go            # Job producer + rate limiter
│   ├── worker.go               # Worker goroutine
│   ├── collector.go            # Streaming aggregation (O(1))
│   ├── report.go               # Report formatting
│   ├── monitor.go              # System stats (CPU, mem, GC)
│   ├── saturation.go           # Health detection + recommendations
│   └── capacity.go             # Pre-test estimation
├── internal/
│   ├── transport/              # Connection pooling
│   ├── config/                 # CLI flag builder
│   ├── dist/                   # Redis distributed engine
│   └── metrics/                # Prometheus collectors
├── grafana/                    # Dashboard provisioning
├── prometheus.yml              # Prometheus config
├── .github/workflows/          # CI + Release automation
├── CHANGELOG.md
├── CONTRIBUTING.md
├── Makefile
└── README.md
```

---

## Development

```bash
make fmt     # format code
make test    # run tests
make race    # run tests with race detector
make bench   # run benchmarks
make build   # build binary
```

---

## Testing

```bash
# Unit + integration tests
go test ./...

# With race detector
go test -race ./...

# Benchmarks
go test -bench=. -benchmem ./...
```

---

## Roadmap

- [x] Core load testing engine
- [x] Rate limiting
- [x] JSON/table/quiet/csv output
- [x] Generator saturation detection
- [x] Connection pooling (HTTP/2, buffer pool)
- [x] Distributed mode (Redis)
- [x] Prometheus/Grafana metrics
- [x] Capacity estimation
- [x] Battle tested against k6
- [ ] YAML configuration
- [ ] Load patterns (ramp up/down)
- [ ] WebSocket testing
- [ ] gRPC support

---

## Blog

- [From Curiosity to Go-Storm: Why I Built My Own HTTP Load Tester](https://hariomop12.github.io/blog/from-curiosity-to-go-storm-why-i-built-my-own-http-load-tester)
- [Why Your Load Test Results Might Be Wrong](https://hariomop12.github.io/blog/why-your-load-test-results-might-be-wrong)

---

## License

Released under the [Apache License 2.0](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
