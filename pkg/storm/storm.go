// Package storm provides the core HTTP load testing engine.
// This is the public library API — anyone can import it and build
// their own CLI or tooling on top of it.
package storm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/time/rate"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds the input configuration for a load test.
// Imported by both the CLI (cmd/storm) and any external library user.
type Config struct {
	URL         string        // API endpoint
	TotalReqs   int           // Total requests to send
	Concurrency int           // Number of parallel workers
	Timeout     time.Duration // Request timeout
	Method      string        // GET, POST, PUT, DELETE
	Payload     []byte        // For POST requests
	Rate        int           // Max requests per second (0 = unlimited)
}

// Job represents a single request task.
type Job struct {
	ID     int
	URL    string
	Method string
	Body   []byte
}

// Result captures response metrics for a single request.
type Result struct {
	JobID      int
	Method     string
	StatusCode int
	Duration   time.Duration
	Error      error
	Timestamp  time.Time
}

// Stats aggregates the results of a full load test run.
type Stats struct {
	TotalRequests   int
	Successful      int
	Failed          int
	MinResponseTime time.Duration
	MaxResponseTime time.Duration
	AvgResponseTime time.Duration
	TotalDuration   time.Duration
	P50             time.Duration
	P95             time.Duration
	P99             time.Duration
	RequestsPerSec  float64
	StatusCodes     map[int]int
	Errors          []string
}

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

// Validate checks the config before a run starts.
func (c Config) Validate() error {
	if c.Rate < 0 {
		return fmt.Errorf("rate cannot be negative, got %d", c.Rate)
	}
	if c.TotalReqs <= 0 {
		return fmt.Errorf("total requests must be positive, got %d", c.TotalReqs)
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got %d", c.Concurrency)
	}
	return nil
}

// NewLoadTester builds a LoadTester from config.
// It owns a cancellable context so workers can shut down gracefully.
func NewLoadTester(ctx context.Context, config Config) *LoadTester {
	ctx, cancel := context.WithCancel(ctx)

	lt := &LoadTester{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
		jobs:    make(chan Job, config.TotalReqs),
		results: make(chan Result, config.TotalReqs),
		ctx:     ctx,
		cancel:  cancel,
	}

	if config.Rate > 0 {
		lt.limiter = rate.NewLimiter(rate.Limit(config.Rate), config.Rate)
	}

	return lt
}

// worker is a single concurrent consumer of the jobs channel.
func (lt *LoadTester) worker(id int) {
	defer lt.wg.Done()

	for {
		select {
		case <-lt.ctx.Done():
			return

		case job, ok := <-lt.jobs:
			if !ok {
				return
			}
			result := lt.executeRequest(job)
			lt.completed.Add(1)
			select {
			case lt.results <- result:

			case <-lt.ctx.Done():
				return
			}
		}
	}
}

func (lt *LoadTester) Completed() int64 {
	return lt.completed.Load()
}

// executeRequest performs a single HTTP request and records its result.
func (lt *LoadTester) executeRequest(job Job) Result {
	return Execute(lt.ctx, lt.client, job)
}

// Execute performs a single HTTP request and records its result.
// Exported so distributed agents can reuse the same request logic.
func Execute(ctx context.Context, client *http.Client, job Job) Result {
	start := time.Now()

	req, err := http.NewRequestWithContext(
		ctx,
		job.Method,
		job.URL,
		bytes.NewBuffer(job.Body),
	)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     err,
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	if job.Method == "POST" || job.Method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     err,
			Duration:  duration,
			Timestamp: time.Now(),
		}
	}
	defer resp.Body.Close()

	return Result{
		JobID:      job.ID,
		Method:     job.Method,
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Error:      nil,
		Timestamp:  time.Now(),
	}
}

// produceJobs fills the jobs channel up to TotalReqs.
// It respects cancellation so a shutdown doesn't leak the goroutine.
func (lt *LoadTester) produceJobs() {
	defer close(lt.jobs)

	for i := 0; i < lt.config.TotalReqs; i++ {
		if lt.limiter != nil {
			if err := lt.limiter.Wait(lt.ctx); err != nil {
				return
			}
		}

		job := Job{
			ID:     i + 1,
			URL:    lt.config.URL,
			Method: lt.config.Method,
			Body:   lt.config.Payload,
		}

		select {
		case <-lt.ctx.Done():
			return
		case lt.jobs <- job:
		}
	}
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

// Aggregate combines a set of results into Stats.
// Exported so distributed coordinators can reuse the same aggregation logic.
func Aggregate(results []Result) Stats {
	var (
		totalDuration time.Duration
		minDuration   time.Duration
		maxDuration   time.Duration
		successCount  int
		failCount     int
		statusCodes   = make(map[int]int)
		errors        []string
		durations     []time.Duration
	)

	firstResult := true

	for _, result := range results {
		if result.Error != nil {
			failCount++

			errors = append(
				errors,
				fmt.Sprintf("Job %d: %v", result.JobID, result.Error),
			)
			continue
		}

		statusCodes[result.StatusCode]++
		if result.StatusCode >= 200 && result.StatusCode < 400 {
			successCount++
		} else {
			failCount++
		}

		totalDuration += result.Duration
		durations = append(durations, result.Duration)

		if firstResult {
			minDuration = result.Duration
			maxDuration = result.Duration
			firstResult = false
		}

		if result.Duration < minDuration {
			minDuration = result.Duration
		}

		if result.Duration > maxDuration {
			maxDuration = result.Duration
		}
	}

	var avgDuration time.Duration

	if successCount+failCount > 0 {
		avgDuration = totalDuration / time.Duration(successCount+failCount)
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	return Stats{
		TotalRequests:   len(results),
		Successful:      successCount,
		Failed:          failCount,
		MinResponseTime: minDuration,
		MaxResponseTime: maxDuration,
		AvgResponseTime: avgDuration,
		P50:             percentile(durations, 50),
		P95:             percentile(durations, 95),
		P99:             percentile(durations, 99),
		StatusCodes:     statusCodes,
		Errors:          errors,
	}
}

// percentile returns the duration at the given percentile (0-100)
// of a sorted slice, using the nearest-rank method.
func percentile(durations []time.Duration, pct float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(durations)) * pct / 100))
	if idx < 1 {
		idx = 1
	}
	return durations[idx-1]
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

// PrintStatsReport renders a run's stats to stdout.
// Exported so distributed coordinators can reuse the same output.
func PrintStatsReport(config Config, stats Stats) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("LOAD TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("URL: %s\n", config.URL)
	fmt.Printf("Method: %s\n", config.Method)
	if config.Concurrency > 0 {
		fmt.Printf("Concurrency: %d\n", config.Concurrency)
	}
	fmt.Printf("Total Requests: %d\n", stats.TotalRequests)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Successful: %d\n", stats.Successful)
	fmt.Printf("Failed: %d\n", stats.Failed)
	fmt.Printf("Success Rate: %.2f%%\n",
		float64(stats.Successful)/float64(stats.TotalRequests)*100)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Min Response: %v\n", stats.MinResponseTime)
	fmt.Printf("Max Response: %v\n", stats.MaxResponseTime)
	fmt.Printf("Avg Response: %v\n", stats.AvgResponseTime)
	fmt.Printf("p50 Response: %v\n", stats.P50)
	fmt.Printf("p95 Response: %v\n", stats.P95)
	fmt.Printf("p99 Response: %v\n", stats.P99)
	fmt.Printf("Requests/sec: %.2f\n", stats.RequestsPerSec)
	fmt.Printf("Total Duration: %v\n", stats.TotalDuration)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println("Status Code Distribution:")
	for code, count := range stats.StatusCodes {
		fmt.Printf("   %d: %d requests\n", code, count)
	}
	if len(stats.Errors) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("Errors:")
		for _, err := range stats.Errors {
			fmt.Printf("   %s\n", err)
		}
	}
	fmt.Println(strings.Repeat("=", 60))
}

// Report combines run metadata with results for machine-readable output.
type Report struct {
	URL             string      `json:"url"`
	Method          string      `json:"method"`
	Concurrency     int         `json:"concurrency"`
	Rate            int         `json:"rate"`
	TotalRequests   int         `json:"total_requests"`
	Successful      int         `json:"successful"`
	Failed          int         `json:"failed"`
	SuccessRate     float64     `json:"success_rate"`
	MinResponseTime int64       `json:"min_response_time_ms"`
	MaxResponseTime int64       `json:"max_response_time_ms"`
	AvgResponseTime int64       `json:"avg_response_time_ms"`
	P50             int64       `json:"p50_ms"`
	P95             int64       `json:"p95_ms"`
	P99             int64       `json:"p99_ms"`
	RequestsPerSec  float64     `json:"requests_per_sec"`
	TotalDuration   int64       `json:"total_duration_ms"`
	StatusCodes     map[int]int `json:"status_codes"`
	Errors          []string    `json:"errors,omitempty"`
}

// JSONReport serializes the run results as indented JSON.
func (lt *LoadTester) JSONReport() ([]byte, error) {
	return ReportJSON(lt.config, lt.stats)
}

// ReportJSON serializes a run's stats as indented JSON.
// Exported so distributed coordinators can reuse the same output.
func ReportJSON(config Config, stats Stats) ([]byte, error) {
	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	report := Report{
		URL:             config.URL,
		Method:          config.Method,
		Concurrency:     config.Concurrency,
		Rate:            config.Rate,
		TotalRequests:   stats.TotalRequests,
		Successful:      stats.Successful,
		Failed:          stats.Failed,
		SuccessRate:     successRate,
		MinResponseTime: stats.MinResponseTime.Milliseconds(),
		MaxResponseTime: stats.MaxResponseTime.Milliseconds(),
		AvgResponseTime: stats.AvgResponseTime.Milliseconds(),
		P50:             stats.P50.Milliseconds(),
		P95:             stats.P95.Milliseconds(),
		P99:             stats.P99.Milliseconds(),
		RequestsPerSec:  stats.RequestsPerSec,
		TotalDuration:   stats.TotalDuration.Milliseconds(),
		StatusCodes:     stats.StatusCodes,
		Errors:          stats.Errors,
	}
	return json.MarshalIndent(report, "", "  ")
}
