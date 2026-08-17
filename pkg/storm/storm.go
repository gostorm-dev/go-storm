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

	"golang.org/x/time/rate"
)

// LoadTester runs the producer → worker pool → consumer pipeline.
type LoadTester struct {
	config    Config
	client    *http.Client
	jobs      chan Job
	results   chan Result
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	stats     Stats
	statsMu   sync.Mutex
	limiter   *rate.Limiter
	completed atomic.Int64
}

// collectResults consumes the results channel and aggregates Stats.
func (lt *LoadTester) collectResults() {
	var results []Result
	for result := range lt.results {
		results = append(results, result)
	}

	lt.statsMu.Lock()
	lt.stats = Aggregate(results)
	lt.statsMu.Unlock()
}

// Run starts workers and producer, waits for completion, returns Stats.
func (lt *LoadTester) Run() (Stats, error) {
	if err := lt.config.Validate(); err != nil {
		return Stats{}, err
	}

	startTime := time.Now()

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

	return stats, nil
}

// PrintStats renders the aggregated results to stdout.
func (lt *LoadTester) PrintStats() {
	PrintStatsReport(lt.config, lt.stats)
}

// JSONReport serializes the run results as indented JSON.
func (lt *LoadTester) JSONReport() ([]byte, error) {
	return ReportJSON(lt.config, lt.stats)
}
