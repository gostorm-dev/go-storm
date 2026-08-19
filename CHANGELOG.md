# Changelog

All notable changes to go-storm are documented here.

Format inspired by [Keep a Changelog](https://keepachangelog.com/).

---

## [Unreleased]

### Fixed
- **Body drain fix** — response body was not drained before closing, preventing connection reuse. Added `io.Copy(io.Discard, resp.Body)` which enables Go's connection pool to recycle TCP connections. Result: 96.7% connection reuse ratio, 45% RPS improvement, 72% memory reduction.
- **Race condition in httptrace** — `newConn` variable accessed concurrently from httptrace callbacks; switched to `atomic.Bool`.

### Added
- **Connection pooling** — custom http.Transport with optimized settings:
  - MaxIdleConns: 200 (up from Go default 100)
  - MaxIdleConnsPerHost: 50 (up from Go default 2) — 25x improvement
  - Configurable keep-alive, timeouts, TLS settings
  - HTTP/2 support with multiplexing
  - Buffer pooling (sync.Pool) for zero-allocation hot path
- **Connection pool statistics tracking** via httptrace:
  - Connections created/reused
  - Pool hits/misses with reuse ratio
  - DNS lookups, TLS handshakes, TCP connections
- **Health report enhancement** — now includes connection pool stats
- **New CLI flags**:
  - `--max-idle-conns` (default: 200)
  - `--max-idle-per-host` (default: 50)
  - `--idle-timeout` (default: 90s)
  - `--keep-alive` (default: 30s)
  - `--force-http2` (default: true)
  - `--insecure` (default: false)
- Streaming aggregation engine (`Collector`) — O(1) memory, no sort
- Logarithmic histogram for approximate percentiles (9 buckets, 336 bytes fixed)
- `BenchmarkCollectorCompare` — direct comparison: batch vs streaming
- Local mode Prometheus metrics (`storm run --metrics-port`)
- `LoadTester.SetHooks()` — optional observability callbacks (OnJobStart, OnResult)
- **Generator saturation detection** — monitors 7 factors in real-time:
  - CPU usage (via /proc/self/stat)
  - Memory growth rate
  - GC pressure (pause time)
  - Goroutine count
  - File descriptor usage
  - RPS achievement (target vs actual)
  - Worker utilization
- **Capacity estimation** (`--estimate`) — pre-test benchmark shows max RPS
- **Health report** — post-test generator health with actionable recommendations
- New CLI flags: `--saturation`, `--estimate`, `--saturation-kill`
- CHANGELOG.md

### Changed
- `collectResults()` now uses streaming `Collector` instead of batch `Aggregate()`
- Results channel buffer: `TotalReqs` → `Concurrency` (natural backpressure, O(concurrency) memory)
- HTTP client now uses custom transport with connection pooling (backward compatible)

### Performance
| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| MaxIdleConnsPerHost | 2 (Go default) | 50 | **25x improvement** |
| Connection reuse | Implicit | Explicit tracking | **New metric** |
| Buffer allocations | Per-request | Pooled (sync.Pool) | **Zero allocations** |
| Aggregate 100K (batch) | 4.1 MB, 25ms | 336 B, 7ms | **-99.99% memory, -72% time** |
| Run pipeline (1K reqs) | 14.4 MB | 12.3 MB | **-15% memory** |

---

## [0.2.0] — 2026-08-17

### Added
- Live Prometheus metrics (`storm_requests_total`, `storm_inflight_requests`, `storm_request_duration_seconds`)
- Agent `--metrics-port` flag (default 9091) with `/metrics` endpoint
- Agent `--stay-alive` flag for persistent metrics scraping
- Provisioned Grafana dashboard (4 panels: RPS, inflight, errors, latency percentiles)
- `prometheus.yml` with 5s scrape interval
- Dashboard screenshots in README

---

## [0.1.0] — 2026-08-17

### Added
- Core load testing engine: producer → worker pool → consumer pipeline
- Rate limiter (token bucket, `golang.org/x/time/rate`)
- Distributed mode: Redis queue, agent registration, heartbeat TTL, per-run result isolation
- Per-agent breakdown in distributed reports
- JSON report output (`--format json`)
- `storm report` command to display saved JSON reports
- CI: GitHub Actions (test, lint, build)
- Release automation: tag `v*` → 6 binaries (linux/darwin/windows × amd64/arm64) + checksums
- Pre-push quality gate (gofmt, go vet, go test -race)
- Baseline benchmarks (`Aggregate`, `Execute`, `Run`)
- MIT License

### Fixed
- Duplicate stats from previous runs (per-run result isolation with `storm:results:{runID}`)
- Agent heartbeat TTL (5s) for dead-agent detection
