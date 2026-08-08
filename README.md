# Go-Storm

A lightweight HTTP load testing tool written in Go. Send thousands of requests to any endpoint with configurable concurrency, and get instant stats — min/max/avg response times, requests/sec, and status code distribution.

Built with classic Go concurrency patterns: **producer → worker pool → consumer**, powered by goroutines, channels, and a `sync.WaitGroup`.

## Features

- Concurrent workers (configurable pool size)
- Supports GET / POST / PUT / DELETE
- Per-request timeout
- Rich statistics: min/avg/max response time, requests/sec, success rate
- Status code distribution breakdown
- Graceful shutdown on `Ctrl+C` (SIGINT / SIGTERM)
- Panic-safe and race-free result aggregation

## Quick Start

```bash
go run main.go
```

Runs 100 requests against `https://hariomtanu.com` with 10 concurrent workers.

## Usage

```bash
go run main.go [flags]
```

| Flag      | Default                 | Description                          |
| --------- | ----------------------- | ------------------------------------ |
| `-url`    | `https://hariomtanu.com` | Target URL                           |
| `-n`      | `100`                    | Total number of requests             |
| `-c`      | `10`                     | Concurrency level (parallel workers) |
| `-method` | `GET`                    | HTTP method: GET, POST, PUT, DELETE  |
| `-timeout`| `10`                     | Request timeout in seconds           |

### Examples

Load test an API with 50 concurrent workers:

```bash
go run main.go -url https://api.example.com -n 1000 -c 50
```

Send POST requests with a JSON payload:

```bash
go run main.go -url https://api.example.com/users -n 500 -c 20 -method POST -timeout 5
```

## Sample Output

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

## How It Works

```
          +--------------+     +----------------------+     +----------------+
  Jobs    |   Producer   | --> |   Worker Pool (N)    | --> |    Consumer    |
 channel  | produceJobs  |     |  goroutines (jobs)   |     | collectResults |
          +--------------+     +----------------------+     +----------------+
```

1. **Producer** (`produceJobs`) generates `TotalReqs` jobs and pushes them into a channel.
2. **Workers** (`worker`) — `Concurrency` goroutines — pull jobs, execute the HTTP request, and push results into a results channel.
3. **Consumer** (`collectResults`) aggregates results into `Stats`.
4. A **WaitGroup** ensures the results channel closes only after all workers finish.
5. A shared **context** enables graceful cancellation — workers exit cleanly on `Ctrl+C`.

## Project Structure

```
go-storm/
├── main.go   # Load tester: config, workers, stats, CLI
├── go.mod
└── README.md
```

## Concurrency Building Blocks Used

- **Goroutines** — lightweight concurrent execution
- **Channels** — typed message passing between producer/workers/consumer
- **`select`** — non-blocking cancellation handling alongside channel ops
- **`sync.WaitGroup`** — waiting for all workers to finish
- **`context.Context`** — cancellation propagation & request timeouts
- **`sync.Mutex`** — safe shared-stats access

## Build

```bash
go build -o go-storm main.go
./go-storm -url https://api.example.com -n 1000 -c 50
```

## Roadmap

- [x] **Phase 1 — Core load tester** (current): concurrent workers, stats, graceful shutdown
- [ ] **Phase 2 — Rate limiting**: control requests per second, ramp-up/ramp-down profiles
- [ ] **Phase 3 — JSON report**: machine-readable output, export results to files
- [ ] **Phase 4 — CLI with Cobra**: subcommands (`run`, `report`, `config`) and richer flags
- [ ] **Phase 5 — Redis**: distributed job queue, result aggregation, shared state
- [ ] **Phase 6 — Distributed load testing**: run workers across multiple machines, coordinated via Redis
- [ ] **Phase 7 — Prometheus/Grafana**: live metrics, dashboards, alerting

## License

MIT
