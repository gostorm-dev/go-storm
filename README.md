# Go-Storm

A lightweight HTTP load testing tool written in Go. Send thousands of requests to any endpoint with configurable concurrency and rate limiting, and get instant stats — min/max/avg response times, requests/sec, and status code distribution — in text or JSON.

Built with classic Go concurrency patterns: **producer → rate-limited pipeline → worker pool → consumer**, powered by goroutines, channels, and a `sync.WaitGroup`.

## Features

- Concurrent workers (configurable pool size)
- Rate limiting via token bucket (`-r/--rate` requests per second)
- Supports GET / POST / PUT / DELETE
- Per-request timeout
- Rich statistics: min/avg/max response time, p50/p95/p99 percentiles, requests/sec, success rate
- Status code distribution breakdown
- Live progress bar with real-time requests/sec
- JSON report output (`--format json`) with optional file export (`--output`)
- `report` subcommand to pretty-print a saved JSON report
- Graceful shutdown on `Ctrl+C` (SIGINT / SIGTERM)
- Panic-safe and race-free result aggregation
- Config validation before every run
- **Distributed load testing** via Redis (`storm run-dist` + `storm agent`): agents on any machine share one job queue and push results back for centralized aggregation

## Quick Start

Install once (needs Go 1.22+):

```bash
go install github.com/hariomop12/go-storm/cmd/storm@latest
```

Then run anywhere:

```bash
storm run -u https://hariomtanu.com -n 100 -c 10
```

Runs 100 requests against `https://hariomtanu.com` with 10 concurrent workers.

No Go installed? Grab a prebuilt binary from the [Releases](https://github.com/hariomop12/go-storm/releases) page.

## Commands

```bash
go run ./cmd/storm run [flags]      # run a load test
go run ./cmd/storm run-dist [flags] # run a distributed load test (needs Redis)
go run ./cmd/storm agent [flags]    # worker that pulls jobs from Redis
go run ./cmd/storm report <file>    # pretty-print a saved JSON report
go run ./cmd/storm version          # print the version
```

## Usage

```bash
go run ./cmd/storm run [flags]
```

| Flag         | Shorthand | Default                  | Description                          |
| ------------ | --------- | ------------------------ | ------------------------------------ |
| `--url`      | `-u`      | `https://hariomtanu.com` | Target URL                          |
| `--requests` | `-n`      | `100`                    | Total number of requests             |
| `--concurrency` | `-c`   | `10`                     | Concurrency level (parallel workers) |
| `--method`   | `-m`      | `GET`                    | HTTP method: GET, POST, PUT, DELETE  |
| `--timeout`  | `-t`      | `10`                     | Request timeout in seconds           |
| `--rate`     | `-r`      | `0`                      | Max requests per second (0 = unlimited) |
| `--body`     | `-b`      | ``                       | Request body, e.g. `-b '{"name":"hariom"}'` |
| `--format`   |           | `text`                   | Output format: `text` or `json`      |
| `--output`   |           | ``                       | Write JSON report to a file          |

### Examples

Load test an API with 50 concurrent workers:

```bash
go run ./cmd/storm run -u https://api.example.com -n 1000 -c 50
```

Send POST requests with a JSON payload:

```bash
go run ./cmd/storm run -u https://api.example.com/users -n 500 -c 20 -m POST -t 5
```

Send POST with a custom body:

```bash
go run ./cmd/storm run -u https://api.example.com/users -n 500 -c 20 -m POST -b '{"name":"hariom","age":25}'
```

Throttle throughput to 500 requests per second:

```bash
go run ./cmd/storm run -u https://api.example.com -n 10000 -c 100 -r 500
```

Export results as JSON for automation/CI:

```bash
go run ./cmd/storm run -u https://api.example.com -n 1000 -c 50 --format json --output report.json
```

Pretty-print a saved report later:

```bash
go run ./cmd/storm report report.json
```

Build a single binary and use it directly:

```bash
make build
./storm run -u https://api.example.com -n 1000 -c 50
```

## Distributed Load Testing (Redis)

`storm run-dist` + `storm agent` distribute load across any number of machines. Agents pull jobs from a shared Redis queue, execute them, and push results back; the coordinator aggregates everything into the same report format as local runs.

Prerequisite: a running Redis server (`docker run -d -p 6379:6379 redis`, or install `redis-server`).

**1. Start agents** — on every machine you want to generate load from:

```bash
./storm agent -c 5                          # 5 worker goroutines pulling from the queue
./storm agent -c 3 --redis 10.0.0.5:6379    # pointing at a remote Redis
./storm agent --name loadbox-1 -c 5         # name your agent for the per-agent breakdown
```

**2. Run the distributed test** — once, from anywhere:

```bash
./storm run-dist -u https://api.example.com -n 10000
./storm run-dist --agents 2 -n 10000        # wait until 2 agents are registered before starting
```

The coordinator pushes 10,000 jobs to the queue, waits for all results, and prints the aggregated report plus a per-agent breakdown (which agent handled how many requests). In this example the two agents (5 + 3 workers) would split the work automatically.

Redis keys: `storm:jobs` (job queue), `storm:agent:{id}` (per-agent heartbeat key with a TTL), `storm:results:{runID}` (per-run result list — a fresh key per `run-dist` so runs never mix). The queue is flushed at the start of every `run-dist`, and agents idle-exit after 5s with no jobs.

## Sample Output

### Text format (default)

During the run you get a live progress bar with real-time requests/sec:

```
Starting Load Test
Target: https://api.example.com
Total: 1000 requests
Concurrency: 50 workers
Rate: 0 req/sec
Running 100% |████████████████████████████████████████| (1000/1000) [1s:0s]
512 req/s

============================================================
LOAD TEST RESULTS
============================================================
URL: https://api.example.com
Method: GET
Concurrency: 50
Total Requests: 1000
------------------------------------------------------------
Successful: 998
Failed: 2
Success Rate: 99.80%
------------------------------------------------------------
Min Response: 12ms
Max Response: 3.2s
Avg Response: 145ms
p50 Response: 90ms
p95 Response: 1.1s
p99 Response: 2.4s
Requests/sec: 512.34
Total Duration: 1.95s
------------------------------------------------------------
Status Code Distribution:
   200: 998 requests
   429: 2 requests
```

### Distributed breakdown (`run-dist`)

`run-dist` additionally prints how each agent contributed to the run:

```
AGENT BREAKDOWN
------------------------------------------------------------
Agent            Requests        Avg        p95    Success
agent-a               100 4.147726ms   6.4086ms     100.0%
agent-b               100 4.174609ms 6.947868ms     100.0%
```

Name agents with `storm agent --name <id>` so you can tell machines apart at a glance (default: `hostname-timestamp`).

### JSON format

```json
{
  "url": "https://api.example.com",
  "method": "GET",
  "concurrency": 100,
  "rate": 500,
  "total_requests": 10000,
  "successful": 10000,
  "failed": 0,
  "success_rate": 100,
  "min_response_time_ms": 1,
  "max_response_time_ms": 207,
  "avg_response_time_ms": 7,
  "p50_ms": 4,
  "p95_ms": 18,
  "p99_ms": 95,
  "requests_per_sec": 526.13,
  "total_duration_ms": 19007,
  "status_codes": {
    "200": 10000
  }
}
```

Durations are in milliseconds (`_ms`) — easy to read for humans and machines alike.

## Architecture

### Local mode — producer → worker pool → consumer

```
          +------------------+     +----------------------+     +----------------+
          |   Producer       |     |   Worker Pool (N)    |     |    Consumer    |
  Config  |  produceJobs     | --> |  goroutines (jobs)   | --> | collectResults |
          |   + rate limiter |     |  execute HTTP reqs   |     |  → Aggregate   |
          +------------------+     +----------------------+     +----------------+
```

The pipeline runs inside a single `LoadTester`, which owns everything it needs:

| Component | Responsibility |
| --------- | -------------- |
| `ctx` / `cancel` | a cancellable `context` — `Ctrl+C` shuts workers down gracefully |
| `client` | one shared `http.Client` with the configured per-request timeout |
| `jobs chan Job` | buffered channel of size `TotalReqs` — producer and workers never block on send |
| `results chan Result` | buffered channel of size `TotalReqs` — workers push results here |
| `wg sync.WaitGroup` | counts running workers so results can be closed exactly once |
| `limiter *rate.Limiter` | shared token bucket (`golang.org/x/time/rate`), created only if `rate > 0` |
| `completed atomic.Int64` | live request counter read by the progress bar |

**Step by step:**

1. **Producer** (`produceJobs`) loops `TotalReqs` times, building one `Job` per request. Before each job it calls `limiter.Wait(ctx)` when a rate is set — this is what throttles throughput. Jobs land on the buffered `jobs` channel, then the producer closes the channel.
2. **Workers** (`worker`) — `Concurrency` goroutines — `select` on `ctx.Done()` and the `jobs` channel. Each takes a `Job`, executes the HTTP request via `Execute`, bumps the `completed` counter, and sends the `Result` to `results`. Every worker also watches the context, so cancellation exits cleanly instead of deadlocking.
3. **Completion** — the consumer waits for the `WaitGroup`, and only then closes `results`. This guarantees the channel closes after (and only after) every worker finishes.
4. **Consumer** (`collectResults`) reads all `Result`s and hands them to `Aggregate`, which computes counts, status-code distribution, min/max/avg latency, and p50/p95/p99 (nearest-rank method).
5. **Final metrics** — `Run` times the whole test, then sets `TotalDuration` and `RequestsPerSec` on the stats.
6. **Live progress** — while running, a board-watcher goroutine reads `completed` every 500ms, computes real-time req/s from the delta, and updates the progress bar (text output only, so JSON stays clean).

**Why channels + WaitGroup?** Channels give each component an explicit, race-free hand-off point; the WaitGroup makes "all workers done" a first-class event. This is the idiomatic Go pipeline — the same shape used by real systems (e.g. log processors, image pipelines).

### Distributed mode — coordinator + agents + Redis

```
   storm run-dist               Redis                     storm agent × N
   +---------------+        +--------------+         +----------------+
   | PushJobs      |  LPUSH |  storm:jobs  | BLPOP  | pop → Execute   |
   | (job queue)   | -----> |  (shared)    | <----- | → PushResult    |
   | poll results  |        +--------------+         |                |
   | → Aggregate   |  LRANGE| storm:results:{runID}| LPUSH| (any machines) |
   | + breakdown   |  <----  +--------------+  <----- +----------------+
   +---------------+        | storm:agent:{id} |     | register + hb  |
```

Two separate roles communicate **only through Redis** — they never talk to each other directly:

**Coordinator (`storm run-dist`)**
1. **Flush** — deletes `storm:jobs` and every `storm:results:*` key so leftovers from previous runs can't pollute the report.
2. **PushJobs** — marshals every `Job` (wrapped with the run's `runID`) to JSON and `LPUSH`es them onto `storm:jobs` in chunks of 100 (so a million jobs don't hog memory in one command).
3. **Wait / Poll** — optionally blocks until `--agents` N are registered (30s timeout), then every 300ms checks `LLEN storm:results:{runID}`. When the count reaches `TotalReqs`, all jobs are done.
4. **LRANGE** — pulls the run's result list, decodes each entry into a `Result`, and runs the exact same `storm.Aggregate` as local mode — then groups results per agent for the breakdown.

**Agent (`storm agent`)**
1. **Register + heartbeat** — writes `storm:agent:{id}` with a 5s TTL, then a goroutine renews it every second. A crashed agent's key just expires; `--agents` coordination sees only live agents.
2. Starts `-c` worker goroutines, each with its own `http.Client`.
3. Each worker calls `BLPop` on `storm:jobs` — a **blocking** pop that returns instantly when a job appears and wastes no CPU when the queue is empty (no polling).
4. Executes the job with the shared `storm.Execute`, pushes the result (tagged with the agent ID) to `LPUSH storm:results:{runID}`.
5. Exits after ~5s with no jobs, or immediately on `Ctrl+C` (unregistering itself).

**Why Redis?** The queue is the **single source of truth** shared by every machine. A coordinator pushes jobs once; *any* number of agents pull from the same queue, so the work is split automatically and workers on one machine can't see another machine's state. Results all land in one list, so aggregation is trivial. Redis is fast (in-memory), simple (atomic `BLPop`), and its lists are durable enough that jobs survive an agent crash — the job just waits for the next pull.

**The `distResult` wire format** — results are serialized as a small struct where the error is a `string`. A Go `error` interface can't be JSON-marshaled, so the error message is converted to text at the agent and back to an `error` at the coordinator.

**One code path for reports** — local and distributed runs share `Execute`, `Aggregate`, `PrintStatsReport`, and `ReportJSON`. That's why a distributed report looks byte-for-byte like a local one, and bug fixes in aggregation apply everywhere automatically.

## Concurrency & Rate Limiting Concepts

- **Concurrency (`-c`)** — how many requests run in parallel (worker pool size).
- **Rate (`-r`)** — total throughput limit across all workers (requests/second).
- **Burst** — the token bucket capacity. With `burst = rate`, the first `rate` requests fire instantly, then the rate stays steady at `rate`/sec.

## Testing

The suite covers the pipeline with unit and integration tests, run with the race detector:

```bash
go test ./... -race -v
```

- `collectResults` — aggregation logic (table-driven scenarios)
- `executeRequest` — HTTP behavior via `httptest` servers
- `Run` — full producer → worker → consumer pipeline
- `RunCancellation` — graceful shutdown / deadlock check
- `RateLimiting` — token bucket actually throttles
- `ConfigValidate` — invalid config rejected
- `JSONReport` — JSON round-trip correctness
- `Percentile` / `CollectResultsPercentiles` — p50/p95/p99 calculation
- `CompletedCounter` — atomic live-progress counter tracks requests during and after a run

## Project Structure

```
go-storm/
├── cmd/storm/              # CLI entry point (Cobra)
│   ├── main.go             # root command + --redis flag
│   ├── run.go              # storm run (local load test + progress bar)
│   ├── run_dist.go         # storm run-dist (distributed coordinator)
│   ├── agent.go            # storm agent (distributed worker)
│   ├── report.go           # storm report (pretty-print JSON)
│   └── version.go          # storm version
├── internal/
│   ├── config/             # CLI flags → Config (Build)
│   ├── dist/               # distributed engine (Redis queue, agent, coordinator)
│   └── (tester, stats, ratelimit — planned splits)
├── pkg/storm/              # Core engine (public library API)
│   ├── storm.go
│   └── storm_test.go
├── .github/workflows/ci.yml # CI: gofmt, build, vet, test -race
├── Makefile
├── go.mod
└── README.md
```

## Development

```bash
make fmt     # format all Go files
make test    # tests with race detector + coverage
make lint    # go vet
make build   # build the binary
```

## CI

Every push / pull request runs on GitHub Actions: formatting check, build, vet, and tests with the race detector.

## Roadmap

- [x] **Phase 1 — Core load tester**: concurrent workers, stats, graceful shutdown, tests + CI
- [x] **Phase 2 — Rate limiting**: token-bucket rate control via `-rate`
- [x] **Phase 3 — JSON report**: machine-readable output with `--format json` / `--output`
- [x] **Phase 4 — CLI with Cobra**: subcommands (`run`, `report`, `version`), rich flags, live progress bar
- [x] **Phase 5 — Redis**: distributed job queue, agent workers, centralized result aggregation
- [x] **Phase 6 — Distributed enhancements**: per-agent registration + heartbeat, per-agent metrics breakdown, per-run result isolation
- [ ] **Phase 7 — Job acknowledgment/retry**
- [ ] **Phase 8 — Prometheus/Grafana**: live metrics, dashboards, alerting

## License

Released under the [MIT License](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) — tests, code style, and commit conventions.
