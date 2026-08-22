// Package storm provides the core HTTP load testing engine.
// This is the public library API — anyone can import it and build
// their own CLI or tooling on top of it.
package storm

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostorm-dev/go-storm/internal/transport"
)

// LoadTester runs the producer → worker pool → consumer pipeline.
type LoadTester struct {
	config     Config
	client     *http.Client
	jobs       chan Job
	results    chan Result
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	stats      Stats
	statsMu    sync.Mutex
	completed  atomic.Int64
	onJobStart func(Job)
	onResult   func(Result)

	// Virtual-clock arrival telemetry — written by the producer goroutine
	// only, read after Run joins it. Nil unless --rate was set.
	arrival *arrivalTelemetry

	monitor          *Monitor
	thresholds       Thresholds
	enableSaturation bool
	healthReport     *HealthReport
	peakMemoryMB     float64

	// Saturation kill mode: terminate the run when resource saturation
	// persists across consecutive watchdog checks. See saturation_watch.go.
	killOnCritical     bool
	killEvent          atomic.Pointer[saturationKill]
	killCheckInterval  time.Duration // zero → defaultKillCheckInterval
	killStreakRequired int           // zero → defaultKillStreak

	// monitorInterval is the background sampler cadence; zero → 1s.
	monitorInterval time.Duration

	// busyNanos accumulates per-request execution time across all workers,
	// giving the watchdog an exact live worker-utilization signal.
	busyNanos atomic.Int64

	// Transport stats for connection pool monitoring
	transportStats *transport.Stats

	// Connection stats for httptrace
	connStats *ConnectionStats

	// Abort on consecutive failures (atomic counter)
	consecutiveFailures atomic.Int64
}

// SetHooks registers optional callbacks for observability.
// onJobStart fires before each HTTP request; onResult fires after.
// Both are safe to leave nil — workers skip them.
func (lt *LoadTester) SetHooks(onJobStart func(Job), onResult func(Result)) {
	lt.onJobStart = onJobStart
	lt.onResult = onResult
}

// SetThresholds overrides the default saturation detection thresholds.
func (lt *LoadTester) SetThresholds(t Thresholds) {
	lt.thresholds = t
}

// EnableSaturationMonitoring turns on background generator health checks
// and a post-run health report. Breaches are reported, never acted upon —
// pair with EnableSaturationKill to terminate the test on sustained
// generator saturation.
func (lt *LoadTester) EnableSaturationMonitoring() {
	lt.enableSaturation = true
}

// EnableSaturationKill terminates the test when the generator stays
// critically saturated (CPU, GC pressure, file descriptors, goroutines or
// memory growth) for several consecutive checks. Termination is graceful:
// in-flight requests finish and are counted. The reason and timestamp land
// in Stats and the health report.
func (lt *LoadTester) EnableSaturationKill() {
	lt.enableSaturation = true
	lt.killOnCritical = true
}

// GetHealthReport returns the generator health report after a run.
// Returns nil if saturation monitoring was not enabled.
func (lt *LoadTester) GetHealthReport() *HealthReport {
	return lt.healthReport
}

// collectResults consumes the results channel and aggregates Stats.
// Uses a streaming Collector — O(concurrency) memory, no sort.
func (lt *LoadTester) collectResults() {
	c := NewCollector()
	for result := range lt.results {
		c.Add(result)
	}

	lt.statsMu.Lock()
	lt.stats = c.Stats()
	lt.statsMu.Unlock()
}

// Run starts workers and producer, waits for completion, returns Stats.
func (lt *LoadTester) Run() (Stats, error) {
	if err := lt.config.Validate(); err != nil {
		return Stats{}, err
	}

	startTime := time.Now()

	// Honor an explicit SetThresholds override; only default when unset.
	var zeroThresholds Thresholds
	if lt.thresholds == zeroThresholds {
		lt.thresholds = DefaultThresholds()
	}

	// Start system monitor
	if lt.enableSaturation {
		interval := lt.monitorInterval
		if interval <= 0 {
			interval = time.Second
		}
		lt.monitor = NewMonitor(interval, 300)
		lt.monitor.Start()
	}

	// Start workers
	for i := 0; i < lt.config.Concurrency; i++ {
		lt.wg.Add(1)
		go lt.worker(i + 1)
	}

	// Start producer
	go lt.produceJobs()

	// Close results when all workers finish
	go func() {
		lt.wg.Wait()
		close(lt.results)
	}()

	// Watchdog: kill the run on sustained generator saturation.
	// watchDone stays open when the watchdog is disabled — closing an
	// already-closed channel would panic.
	watchDone := make(chan struct{})
	if lt.enableSaturation && lt.killOnCritical && lt.monitor != nil {
		go lt.watchSaturation(watchDone)
	}

	// Collect results
	lt.collectResults()

	// Stop the watchdog before stopping the monitor it reads from.
	close(watchDone)

	// Stop monitor
	if lt.monitor != nil {
		lt.monitor.Stop()
	}

	// Final metrics
	totalTime := time.Since(startTime)

	lt.statsMu.Lock()

	lt.stats.TotalDuration = totalTime
	lt.stats.Arrival = lt.arrival.snapshot()

	if totalTime > 0 {
		lt.stats.RequestsPerSec =
			float64(lt.stats.TotalRequests) / totalTime.Seconds()
	}

	stats := lt.stats

	if ev := lt.killEvent.Load(); ev != nil {
		stats.KilledOnSaturation = true
		stats.KillReason = ev.Reason
		stats.KilledAtMS = float64(ev.At.Milliseconds())
	}

	lt.statsMu.Unlock()

	// Generate health report
	if lt.enableSaturation && lt.monitor != nil {
		lt.buildHealthReport(totalTime)
	}

	return stats, nil
}

// buildHealthReport generates the final generator health report.
func (lt *LoadTester) buildHealthReport(elapsed time.Duration) {
	sysStats := lt.monitor.Stats()

	workerUtil := lt.getWorkerUtilization()

	targetRPS := float64(lt.config.Rate)
	achievedRPS := lt.stats.RequestsPerSec
	maxFDs := GetMaxFDs()

	diag := CheckSaturation(
		lt.thresholds,
		sysStats,
		targetRPS,
		achievedRPS,
		lt.config.Concurrency,
		int64(lt.stats.TotalRequests),
		elapsed,
		maxFDs,
		workerUtil,
		lt.peakMemoryMB,
	)

	hr := HealthReport{
		Level:       diag.Level,
		Duration:    elapsed,
		AchievedRPS: achievedRPS,
		TargetRPS:   targetRPS,
		Stats:       sysStats,
		Signals:     diag.Signals,
		MaxMemoryMB: lt.peakMemoryMB,
		Arrival:     lt.arrival.snapshot(),
	}

	if ev := lt.killEvent.Load(); ev != nil {
		hr.KilledOnSaturation = true
		hr.KillReason = ev.Reason
		hr.KilledAt = ev.At
		// A killed run is by definition untrustworthy, regardless of what
		// the post-run diagnosis says — the watchdog already saw sustained
		// critical saturation mid-run.
		hr.Level = CRITICAL
	}

	// Add connection pool stats if available
	if lt.connStats != nil {
		snapshot := lt.connStats.Snapshot()
		totalConns := snapshot.ConnectionsCreated + snapshot.ConnectionsReused
		totalPool := snapshot.PoolHits + snapshot.PoolMisses
		reuseRatio := 0.0
		hitRatio := 0.0
		if totalConns > 0 {
			reuseRatio = float64(snapshot.ConnectionsReused) / float64(totalConns) * 100
		}
		if totalPool > 0 {
			hitRatio = float64(snapshot.PoolHits) / float64(totalPool) * 100
		}
		hr.ConnectionPoolStats = &ConnectionPoolStats{
			ConnectionsCreated:   snapshot.ConnectionsCreated,
			ConnectionsReused:    snapshot.ConnectionsReused,
			PoolHits:             snapshot.PoolHits,
			PoolMisses:           snapshot.PoolMisses,
			ConnectionReuseRatio: reuseRatio,
			PoolHitRatio:         hitRatio,
		}
	}

	hr.Recommendations = GenerateRecommendations(hr)

	lt.healthReport = &hr
}

// getWorkerUtilization returns the ratio of busy time to total time (0-1),
// computed from exact per-request durations accumulated by workers.
func (lt *LoadTester) getWorkerUtilization() float64 {
	return lt.liveWorkerUtilization(lt.stats.TotalDuration)
}

// PrintStats renders the aggregated results to stdout.
func (lt *LoadTester) PrintStats() {
	PrintStatsReport(lt.config, lt.stats)
}

// PrintStatsTable renders results in a formatted table.
func (lt *LoadTester) PrintStatsTable() {
	PrintStatsTable(lt.config, lt.stats)
}

// PrintStatsQuiet renders only numbers, comma-separated (for CI/CD).
func (lt *LoadTester) PrintStatsQuiet() {
	PrintStatsQuiet(lt.config, lt.stats)
}

// PrintStatsCSV renders results in CSV format.
func (lt *LoadTester) PrintStatsCSV() {
	PrintStatsCSV(lt.config, lt.stats)
}

// JSONReport serializes the run results as indented JSON.
func (lt *LoadTester) JSONReport() ([]byte, error) {
	return ReportJSON(lt.config, lt.stats)
}
