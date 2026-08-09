# Go-Storm

A lightweight HTTP load testing tool written in Go. Send thousands of requests to any endpoint with configurable concurrency and rate limiting, and get instant stats — min/max/avg response times, requests/sec, and status code distribution — in text or JSON.

Built with classic Go concurrency patterns: **producer → rate-limited pipeline → worker pool → consumer**, powered by goroutines, channels, and a `sync.WaitGroup`.

## Features

- Concurrent workers (configurable pool size)
- Rate limiting via token bucket (`-rate` requests per second)
- Supports GET / POST / PUT / DELETE
- Per-request timeout
- Rich statistics: min/avg/max response time, requests/sec, success rate
- Status code distribution breakdown
- JSON report output (`--format json`) with optional file export (`--output`)
- Graceful shutdown on `Ctrl+C` (SIGINT / SIGTERM)
- Panic-safe and race-free result aggregation
- Config validation before every run

## Quick Start

```bash
go run ./cmd/storm
```

Runs 100 requests against `https://hariomtanu.com` with 10 concurrent workers.

## Usage

```bash
go run ./cmd/storm [flags]
```

| Flag       | Default                 | Description                          |
| ---------- | ----------------------- | ------------------------------------ |
| `-url`     | `https://hariomtanu.com` | Target URL                          |
| `-n`       | `100`                    | Total number of requests             |
| `-c`       | `10`                     | Concurrency level (parallel workers) |
| `-method`  | `GET`                    | HTTP method: GET, POST, PUT, DELETE  |
| `-timeout` | `10`                     | Request timeout in seconds           |
| `-rate`    | `0`                      | Max requests per second (0 = unlimited) |
| `--format` | `text`                   | Output format: `text` or `json`      |
| `--output` | ``                       | Write JSON report to a file          |

### Examples

Load test an API with 50 concurrent workers:

```bash
go run ./cmd/storm -url https://api.example.com -n 1000 -c 50
```

Send POST requests with a JSON payload:

```bash
go run ./cmd/storm -url https://api.example.com/users -n 500 -c 20 -method POST -timeout 5
```

Throttle throughput to 500 requests per second:

```bash
go run ./cmd/storm -url https://api.example.com -n 10000 -c 100 -rate 500
```

Export results as JSON for automation/CI:

```bash
go run ./cmd/storm -url https://api.example.com -n 1000 -c 50 --format json --output report.json
```

## Sample Output

### Text format (default)

```
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
Requests/sec: 512.34
Total Duration: 1.95s
------------------------------------------------------------
Status Code Distribution:
   200: 998 requests
   429: 2 requests
============================================================
```

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
  "min_response_time_ns": 1096309,
  "max_response_time_ns": 206533112,
  "avg_response_time_ns": 7362558,
  "requests_per_sec": 526.13,
  "total_duration_ns": 19006763064,
  "status_codes": {
    "200": 10000
  }
}
```

Durations are in nanoseconds (`_ns`) so the output is unambiguous for machines.

## How It Works

```
          +------------------+     +----------------------+     +----------------+
          |   Producer       |     |   Worker Pool (N)    |     |    Consumer    |
  Config  |  produceJobs     | --> |  goroutines (jobs)   | --> | collectResults |
          |   + rate limiter |     |  execute HTTP reqs   |     | + JSONReport   |
          +------------------+     +----------------------+     +----------------+
```

1. **Producer** (`produceJobs`) generates `TotalReqs` jobs. If a rate is set, a shared token-bucket limiter (`golang.org/x/time/rate`) throttles how fast jobs enter the channel.
2. **Workers** (`worker`) — `Concurrency` goroutines — pull jobs, execute the HTTP request, and push results into a results channel.
3. **Consumer** (`collectResults`) aggregates results into `Stats`.
4. A **WaitGroup** ensures the results channel closes only after all workers finish.
5. A shared **context** enables graceful cancellation — workers exit cleanly on `Ctrl+C`, and stats report only the requests actually sent.
6. **Config validation** runs before the test starts (negative rate, zero concurrency, etc. are rejected).

## Concurrency & Rate Limiting Concepts

- **Concurrency (`-c`)** — how many requests run in parallel (worker pool size).
- **Rate (`-rate`)** — total throughput limit across all workers (requests/second).
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

## Project Structure

```
go-storm/
├── cmd/storm/              # CLI entry point
├── internal/
│   ├── config/             # CLI flags + config parsing
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
- [ ] **Phase 4 — CLI with Cobra**: subcommands (`run`, `report`, `config`) and richer flags
- [ ] **Phase 5 — Redis**: distributed job queue, result aggregation, shared state
- [ ] **Phase 6 — Distributed load testing**: run workers across multiple machines, coordinated via Redis
- [ ] **Phase 7 — Prometheus/Grafana**: live metrics, dashboards, alerting

## License

MIT
