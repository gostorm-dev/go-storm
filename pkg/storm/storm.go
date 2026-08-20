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

	"github.com/hariomop12/go-storm/internal/transport"
	"golang.org/x/time/rate"
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
	limiter    *rate.Limiter
	completed  atomic.Int64
	onJobStart func(Job)
	onResult   func(Result)

	monitor          *Monitor
	thresholds       Thresholds
	enableSaturation bool
	healthReport     *HealthReport
	peakMemoryMB     float64

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

// EnableSaturationMonitoring turns on background generator health checks.
// If enabled, the test will be killed when critical thresholds are breached.
func (lt *LoadTester) EnableSaturationMonitoring() {
	lt.enableSaturation = true
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
	lt.thresholds = DefaultThresholds()

	// Start system monitor
	if lt.enableSaturation {
		lt.monitor = NewMonitor(1*time.Second, 300)
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

	// Collect results
	lt.collectResults()

	// Stop monitor
	if lt.monitor != nil {
		lt.monitor.Stop()
	}

	// Final metrics
	totalTime := time.Since(startTime)

	lt.statsMu.Lock()

	lt.stats.TotalDuration = totalTime

	if totalTime > 0 {
		lt.stats.RequestsPerSec =
			float64(lt.stats.TotalRequests) / totalTime.Seconds()
	}

	stats := lt.stats

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

// getWorkerUtilization returns the ratio of busy time to total time (0-1).
func (lt *LoadTester) getWorkerUtilization() float64 {
	if lt.stats.TotalRequests == 0 || lt.stats.TotalDuration == 0 || lt.config.Concurrency <= 0 {
		return 0
	}

	// Each request took some time; total busy = sum of all request durations
	// We approximate from total requests * avg duration
	if lt.stats.AvgResponseTime > 0 {
		busy := time.Duration(lt.stats.TotalRequests) * lt.stats.AvgResponseTime
		maxTime := time.Duration(lt.config.Concurrency) * lt.stats.TotalDuration
		ratio := float64(busy) / float64(maxTime)
		if ratio > 1 {
			ratio = 1
		}
		return ratio
	}

	return 0
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
