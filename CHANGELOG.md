# Changelog

All notable changes to go-storm are documented here.

Format inspired by [Keep a Changelog](https://keepachangelog.com/).

---

## [Unreleased]

### Added
- Streaming aggregation engine (`Collector`) — O(1) memory, no sort
- Logarithmic histogram for approximate percentiles (9 buckets, 336 bytes fixed)
- `BenchmarkCollectorCompare` — direct comparison: batch vs streaming
- Local mode Prometheus metrics (`storm run --metrics-port`)
- `LoadTester.SetHooks()` — optional observability callbacks (OnJobStart, OnResult)
- CHANGELOG.md

### Changed
- `collectResults()` now uses streaming `Collector` instead of batch `Aggregate()`
- Results channel buffer: `TotalReqs` → `Concurrency` (natural backpressure, O(concurrency) memory)

### Performance
| Metric | Before | After | Delta |
|--------|--------|-------|-------|
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
