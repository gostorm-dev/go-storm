# go-storm

**The Load Tester That Tells Truth.**

A high-performance HTTP load testing engine written in Go. Unlike other tools, go-storm detects when **YOUR generator is the bottleneck** — not just the target.

```
  ╔═══════════════════════════════════════╗
  ║          ⚡ go-storm                  ║
  ║   The Load Tester That Tells Truth   ║
  ╚═══════════════════════════════════════╝
```

[![CI](https://github.com/gostorm-dev/go-storm/actions/workflows/ci.yml/badge.svg)](https://github.com/gostorm-dev/go-storm/actions)
[![Go Report](https://goreportcard.com/badge/github.com/gostorm-dev/go-storm)](https://goreportcard.com/report/github.com/gostorm-dev/go-storm)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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

Real numbers from battle testing on AWS EC2 (t3.medium, 2 vCPU, 4GB RAM).

### go-storm vs k6

| Test | go-storm | k6 | Winner |
|------|----------|-----|--------|
| 10K reqs, 100 concurrency | **11,960 RPS** | 7,118 RPS | go-storm **1.68x faster** |
| 200K reqs, 2000 concurrency | **7,220 RPS** | CRASHED (EOF errors) | go-storm **finished, k6 didn't** |
| 50K reqs, 5000 RPS rate limit | **5,551 RPS** (100% accurate) | 4,972 RPS (277 dropped) | go-storm **more accurate** |
| 10K POST, 200 concurrency | **10,322 RPS** | 5,471 RPS | go-storm **1.89x faster** |
| 5K slow endpoint (100ms) | **976 RPS** | 956 RPS | go-storm |
| 100K POST, 500 concurrency | **9,816 RPS** | 6,250 RPS | go-storm **1.57x faster** |

**Final Score: go-storm 6 — k6 0**

### Key Numbers

```
go-storm:
  ✅ 865,000+ requests tested — ZERO failures
  ✅ 200K extreme load — completed in 27.7 seconds
  ✅ 500K endurance test — 105.6 MB memory, no leaks
  ✅ 94%+ connection reuse ratio
  ✅ Generator health: HEALTHY across all tests

k6:
  ❌ 200K test — crashed with EOF errors
  ⚠️ 30-89% slower than go-storm across all tests
  ❌ No generator health monitoring
```

---

## Features

### Core
- Concurrent workers with configurable pool size
- Rate limiting via token bucket (`-r` / `--rate`)
- GET / POST / PUT / DELETE / PATCH / HEAD
- Per-request timeout
- Rich statistics: min/avg/max, p50/p95/p99, RPS, success rate
- Status code distribution
- Live progress bar with real-time RPS

### Output Formats
- `text` — human-readable with progress bar (default)
- `json` — machine-readable for CI/CD automation
- `table` — formatted table with borders
- `quiet` — numbers only, comma-separated
- `csv` — key-value CSV format

### Generator Health (UNIQUE)
- **Real-time monitoring**: CPU, memory, GC, goroutines, file descriptors
- **Saturation detection**: knows when YOUR machine is the bottleneck
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

### Quick Install (Go 1.22+)

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

# Rate limited — exactly 1000 RPS
storm run -u https://example.com -n 5000 -r 1000

# POST with JSON body
storm run -u https://api.example.com/users -m POST -b '{"name":"test"}'

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
| `--requests` | `-n` | `100` | Total requests |
| `--concurrency` | `-c` | `10` | Parallel workers |
| `--method` | `-m` | `GET` | HTTP method |
| `--timeout` | `-t` | `10` | Request timeout (seconds) |
| `--rate` | `-r` | `0` | Rate limit RPS (0 = unlimited) |
| `--body` | `-b` | | Request body |
| `--format` | | `text` | Output: `text`, `json`, `table`, `quiet`, `csv` |
| `--output` | | | Save report to file |
| `--saturation` | | `true` | Generator health monitoring |
| `--estimate` | | `false` | Pre-test capacity estimation |
| `--saturation-kill` | | `false` | Kill on critical saturation |
| `--metrics-port` | | `0` | Prometheus port (0 = disabled) |
| `--max-idle-conns` | | `200` | Max idle connections |
| `--max-idle-per-host` | | `50` | Max idle per host |
| `--idle-timeout` | | `90` | Idle connection timeout (s) |
| `--keep-alive` | | `30` | TCP keep-alive (s) |
| `--force-http2` | | `true` | Force HTTP/2 |
| `--insecure` | | `false` | Skip TLS verify |

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
  │ p95 Latency       │              10 ms │
  │ p99 Latency       │              50 ms │
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
  "url": "https://example.com",
  "method": "GET",
  "concurrency": 100,
  "total_requests": 10000,
  "successful": 10000,
  "failed": 0,
  "success_rate": 100,
  "avg_response_time_ms": 7,
  "p50_ms": 10,
  "p95_ms": 50,
  "p99_ms": 50,
  "requests_per_sec": 11960.10,
  "total_duration_ms": 836,
  "status_codes": { "200": 10000 }
}
```

---

## Distributed Load Testing

Distribute load across multiple machines using Redis.

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

Released under the [MIT License](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
