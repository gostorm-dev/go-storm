# Changelog

All notable changes to go-storm are documented here.

Format inspired by [Keep a Changelog](https://keepachangelog.com/).

---

## [0.5.0] — 2026-08-20

### Added
- **CLI world-class overhaul**:
  - ASCII banner with branding
  - Examples in help for all commands
  - Flag validation with actionable error messages
  - 5 output formats: text, json, table, quiet, csv
  - Version system with ldflags (version, commit, buildDate)
  - Makefile with build/install/test/race/bench targets
- **Battle tested against k6**:
  - 10K test: go-storm 1.68x faster
  - 200K test: go-storm finished, k6 crashed
  - 50K rate limit: go-storm 100% accurate
  - 10K POST: go-storm 1.89x faster
  - 100K POST: go-storm 1.57x faster
  - Final score: go-storm 6, k6 0
- **README rewrite** with real benchmark numbers

### Changed
- Default URL removed — `-u` flag is now required
- `--format` flag now supports: text, json, table, quiet, csv
- Health report only shown for text/table formats (not quiet/csv)
- Config output suppressed for quiet/csv formats

---

## [Unreleased]

### Added
- **Duration-based load mode** — `-d/--duration` runs the test for a wall-clock window instead of a request count:
  - Go duration strings: `-d 30s`, `-d 5m`, `-d 1h30m`
  - Composes with `--rate` for the primary use case: constant arrival rate for a fixed window (`-r 1000 -d 30m`)
  - **Graceful deadline drain** — in-flight requests finish naturally at the deadline and are counted; no synthetic errors pollute latency or failure stats. Run tail is bounded by the per-request timeout (`-t`)
  - Time-based progress bar (elapsed/total) with live req/s; report shows both requested and actual duration so drift is visible, never hidden
  - `requested_duration_ms` added to JSON reports (additive, non-breaking); quiet format appends a requested-seconds column in duration mode only
  - Minimum window is 1s — below that, startup transients dominate and results mislead
- **Brand banner on test start** — shown for human-readable formats (`text`, `table`) only; `json`/`csv`/`quiet` output stays machine-clean
- **Custom headers** — `-H "Key: Value"` repeatable flag, curl-style parsing:
  - Works on both `run` and `run-dist` (headers ride the distributed job payload)
  - User-supplied `Content-Type` overrides the default; `Host` handled via wire-level override
  - Invalid specs fail fast with actionable errors: `invalid header "X": expected "Key: Value" format`
- **CI-friendly exit codes** — opt-in threshold gates:
  - `--fail-above-errors N` — exit 2 if failed requests exceed N
  - `--fail-above-p95 MS` — exit 2 if p95 latency exceeds MS milliseconds
  - Exit code `2` (threshold violation) is distinct from `1` (config error), so pipelines can tell "my command was wrong" apart from "the service failed the gate"
  - Fully opt-in — default behavior unchanged; full report still printed before the gate evaluates

### Changed
- **`-n/--requests` default removed (breaking-ish)** — `-n` no longer defaults to 100. Exactly one of `-n` or `-d` is now required; omitting both produces an actionable error:
  ```
  Error: no workload defined: set --requests (-n) OR --duration (-d)
  ```
  Scripts relying on the implicit `-n 100` must pass it explicitly. Specifying both flags is also a validation error.
- **`Job.ID` / `Result.JobID` widened to `int64`** — duration mode can produce unbounded request counts; `int` would overflow on 32-bit platforms during long soaks. Library users referencing these fields need a type adjustment.
- Fixed a latent flag-registration bug: `run` and `run-dist` shared one package-level `total` variable, and pflag writes defaults into the bound variable at registration time — whichever command's init() ran last won. Now each command owns its variable.

### Fixed
- **Memory landmine** — the jobs channel was pre-allocated with one slot per request (`make(chan Job, TotalReqs)`), reserving gigabytes for large `-n` values before a single request fired. Now bounded at `concurrency × 2`:
  - `-n 1000000`: peak RSS 154 MB → 28 MB (measured on real binaries)
  - Channel allocation: 64 MB → 1.5 KB per run (benchmark: 42,000× less)
- **Latency precision** — sub-millisecond truncation eliminated:
  - All outputs (JSON, table, CSV, quiet) now report fractional milliseconds instead of integer-truncated values that showed `p99 = 0ms` on fast targets
  - Histogram percentiles now use linear interpolation within buckets instead of snapping to bucket upper bounds
  - Fixed histogram misclassification of fractional-millisecond observations (1.4ms landed in the ≤1ms bucket)

### Fixed (earlier in cycle)
- **Body drain fix** — response body was not drained before closing, preventing connection reuse. Added `io.Copy(io.Discard, resp.Body)` which enables Go's connection pool to recycle TCP connections. Result: 96.7% connection reuse ratio, 45% RPS improvement, 72% memory reduction.
- **Race condition in httptrace** — `newConn` variable accessed concurrently from httptrace callbacks; switched to `atomic.Bool`.

### Added (earlier in cycle)
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
