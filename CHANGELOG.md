# Changelog

All notable changes to go-storm are documented here.

Format inspired by [Keep a Changelog](https://keepachangelog.com/).

---

## [0.5.6] — 2026-08-23

### Changed
- **Round 2 benchmark investigation corrected** — a four-binary A/B disproves any
  code regression behind the reported v0.5.3 sustained-load drop; GC-cycle counts
  scale with throughput (~79 allocs/request) and were never evidence of a
  regression. %steal is ruled out by sar data (≤0.02% in every Round 2 window).
  The open anomaly: generator kernel-space time (%system 66.5% during storm's
  soak vs 15.6% for k6 at equal ~98% total CPU), consistent with connection-path
  syscalls under real-network latency — invisible on loopback. Root cause stays
  "under investigation"; a pool-sizing A/B runs before Round 3 publishes numbers.
- `bench/analyze.py` GenCPU formula hardened: `%user+%nice+%system` only.
  Hypervisor `%steal` is excluded from generator effort and surfaced separately
  per run (defense-in-depth on shared-vCPU instances).

### Fixed
- `bench/analyze.py` rejected valid 6-column sar CPU averages (`len(row) < 7`
  check), printing `None%` for every tool's generator CPU.

Engine unchanged in this release — measurement hygiene only.

---

## [0.5.7] — 2026-08-23

### Fixed
- **Sustained-load throughput halved by connection-pool churn** — the default idle
  pool (50/host, 200 total) sat below typical concurrency, so workers dialled a
  fresh TCP connection per request. Kernel time hit 66.5% during soaks and
  sustained throughput collapsed on real networks (loopback hid it entirely).
  The engine now auto-sizes idle pools to `max(2×concurrency, 256)` and never
  lowers explicit values; `--max-idle-conns` / `--max-idle-per-host` default to
  `0` = auto; library callers get a properly pooled transport instead of Go's
  stdlib defaults (2 idle per host). Measured on a dedicated c6i pair: 60s soak
  at c=100 rose from ~29K to a stable ~45.8K RPS (+56%), p95 6.57ms → 4.40ms.
  Full evidence trail in BENCHMARKS.md.

### Added
- **Build identity in every result** — `tool_version`, `git_commit` and `built_at` now lead every JSON report and appear in table/text/csv output, so any result file can be traced to the exact binary that produced it (reproducibility requirement). Values are injected at build time into a shared `internal/buildinfo` package consumed by both the CLI (`storm version`) and the engine; release binaries are additionally stripped (`-s -w`, ~30% smaller downloads).
- **Release binaries carry real versions** — the release workflow stamps tag/SHA/build-date via ldflags; previously artifacts reported `version dev`.

### Changed
- README install requirement corrected to Go 1.26+ to match `go.mod`.
- `bench/analyze.py` — generator CPU is now `%user+%nice+%system`; hypervisor `%steal`
  is surfaced separately per run as defense-in-depth on shared-vCPU instances
  (Round 2 sar data showed ≤0.02% steal — see BENCHMARKS.md for what the anomaly
  actually was).

---

## [0.5.5] — 2026-08-22

### Added
- **Distributed engine test coverage** — `internal/dist` (previously 452 untested lines) is now behavior-tested end-to-end against in-process miniredis (no Docker, no live Redis): wire-format round trips, multi-chunk job publishing, queue drain with run-ID tagging, agent registry lifecycle with deterministic TTL expiry + heartbeat renewal (miniredis clock control), waitForAgents success/timeout paths, full RunAgent loop (jobs → HTTP → results → deregistration), coordinator aggregation with per-agent breakdown and stale-run isolation, and Flush. Race-detector clean.

### Changed
- **`internal/dist` split into focused files** — the 452-line monolith is now `redis.go` (client wrapper), `protocol.go` (key layout + wire formats), `queue.go` (job/result operations), `registry.go` (agent registry), `agent.go` (agent runtime), `coordinator.go` (aggregation). Public API unchanged; timing constants (`popTimeout`, `idleTimeout`, `heartbeatTTL`, `agentsWait`) became documented struct fields with identical defaults so tests can shorten them.
- `miniredis` added as a **test-only** dependency (imported exclusively by `_test.go` files; never ships in the binary).

---

## [0.5.4] — 2026-08-22

### Added
- **TTFB percentiles** — time-to-first-byte reported alongside latency (`ttfb_avg_ms`, `ttfb_p50_ms`, `ttfb_p90_ms`, `ttfb_p95_ms`, `ttfb_p99_ms` JSON keys, additive; rows in text/table/csv): server-processing time isolated from transfer time — a metric most load testers never expose. Design note `.plans/DESIGN-latency-semantics.md`.

### Changed
- **Latency now measures the FULL response** (headers + drained body), matching vegeta/k6/ab. Previously the clock stopped at first byte and the body download went uncounted, so body-carrying responses reported systematically low, cross-tool-incomparable numbers; TTFB is now captured separately instead of being silently conflated with latency. Empty-body runs are unaffected within noise. Migration: CI gates calibrated via `--fail-above-p95` may need recalibration when targets return large bodies.

---

## [0.5.3] — 2026-08-22

### Added
- **Bounded-error latency percentiles** — the streaming engine's 9-bucket histogram is replaced by a log-linear histogram with a *guaranteed* maximum relative error of ≤0.78% on every percentile, independent of the latency distribution:
  - Each power-of-two binade of latency space is split into 128 equal intervals; every bucket therefore spans at most a factor 1+1/128. Interpolation never leaves the containing bucket, so p50/p90/p95/p99/p99.9 can no longer be off by bucket-width amounts (previously buckets up to 4s wide made p99 estimates unreliable).
  - Hot-path `Observe` costs ~4.5ns with zero allocations (IEEE-754 bit decomposition, no library calls). Collector throughput at 100k requests improved ~14% vs v0.5.2 despite far higher accuracy.
  - Fixed memory: ~32KB once per run regardless of request count — still O(1) streaming, still no stored latencies.
  - **New percentiles reported everywhere**: p90 and p99.9 join p50/p95/p99 in text, table, csv, quiet, and JSON output (`p90_ms`, `p999_ms` JSON keys, additive).
  - Histograms are **mergeable** (`LogHistogram.Merge`) — combined percentiles across distributed agents become exact once wired into the dist aggregation.
  - Batch `Aggregate()` (distributed coordinator path) keeps exact sort-based percentiles and gains the same new fields.
  - Design note: `.plans/DESIGN-histogram-v2.md`; benchmarks recorded in `bench/`.
- **Virtual-clock arrival scheduler** — `--rate` runs no longer use a token bucket:
  - Duration mode dispatches **exactly** `ceil(rate × duration)` requests: `-r 5000 -d 30s` now sends 150,000 — previously ~155,000, deterministically, because the limiter's burst equaled a full second and started pre-filled (rate overshoot bug).
  - No startup stampede: every request departs on its own scheduled slot (`start + j/rate`), so latency percentiles are clean from request #1.
  - Drift-proof by construction: dispatch position is re-derived from the wall clock each step; timer jitter can never accumulate. All schedule math uses 128-bit intermediates — overflow impossible.
  - **New arrival-accuracy telemetry**: on-time slot %, late-dispatch count, lag p50/p99/max. Surfaces in JSON reports (`arrival_accuracy` object) and the generator health report, with an actionable recommendation when accuracy drops below 95%. Slots above 1000 RPS are graded against a 1 ms floor (OS timer granularity), so scheduler noise is never reported as lateness.
  - Design note `.plans/DESIGN-arrival-scheduler.md`; regression tests pin exact counts (`-r 2000 -d 1s` → exactly 2000).

### Changed
- Rate-limited run totals changed from "`rate × duration` + burst" to exact — this is the fix; CLI flags and output schemas unchanged except additive JSON fields.
- Removed the `golang.org/x/time` dependency.
- **Quiet format columns** — two additive columns (`p90`, `p999`) were inserted after `p99`; the first 9 columns are unchanged. Positional parsers reading beyond column 9 must skip to the new layout: total,succ,fail,rate,avg,p50,p90,p95,p99,p999,rps[,requested_s].
- Removed empty placeholder directories `internal/ratelimit`, `internal/stats`, `internal/tester`.

### Fixed
- **Rate overshoot bug**: `-r R -d T` overshot by exactly one full second of traffic in every run (e.g. +5,000 requests at `-r 5000`); reported RPS exceeded target accordingly.
- Percentile estimates are now covered by an explicit, tested error contract instead of relying on uniform-distribution assumptions inside wide buckets.

---

## [0.5.3] — 2026-08-22 (continued)

### Fixed
- **`--saturation-kill` is now real** — previously the flag only changed a printed label; no kill logic existed. The engine now runs a watchdog during the test that terminates it when generator resource saturation persists (~3 consecutive checks):
  - Only resource signals can terminate a run: CPU, GC pressure, file descriptors, goroutines, memory growth. Worker utilization and RPS achievement never kill — they conflate "target is slow" with "generator is exhausted".
  - Termination is graceful: in-flight requests finish and are counted; results cover only the completed portion.
  - Kill reason and timestamp are recorded in Stats (`killed_on_saturation`, `kill_reason`, `killed_at_ms`) and in the health report verdict.
  - Live watchdog evaluation fixes GC-pause semantics for kill decisions by using per-window deltas instead of process-lifetime totals.

### Changed
- `SetThresholds()` is now honored by `Run()` — previously overwritten unconditionally, making the API dead.
- `EnableSaturationMonitoring()` documentation corrected: monitoring alone reports breaches, it does not terminate runs.
- Worker utilization is now computed from exact accumulated per-request durations instead of an average-based approximation.

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

## [0.5.2] — 2026-08-22

### Changed
- **Module path renamed** to `github.com/gostorm-dev/go-storm` — now matches the README install command, badges, and the published org repository. All internal imports updated; no behavioral change.
- **License documentation corrected** — README now references Apache License 2.0, matching the actual LICENSE file (was incorrectly labeled MIT).
- CONTRIBUTING clone URL points to the org repository.

### Fixed
- **go.mod repaired** via `go mod tidy` — direct dependencies (cobra, go-redis, prometheus client, progressbar, color, x/time) are no longer mislabeled `// indirect`.
- `cmd/storm/banner.go` formatted with gofmt — the CI formatting gate passes again.

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
