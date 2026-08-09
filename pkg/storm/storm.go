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
	"net/http"
	"strings"
	"sync"
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
	RequestsPerSec  float64
	StatusCodes     map[int]int
	Errors          []string
}

// LoadTester runs the producer → worker pool → consumer pipeline.
type LoadTester struct {
	config  Config
	client  *http.Client
	jobs    chan Job
	results chan Result
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	stats   Stats
	statsMu sync.Mutex
	limiter *rate.Limiter
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

	fmt.Printf("Worker %d started\n", id)

	for {
		select {
		case <-lt.ctx.Done():
			fmt.Printf("Worker %d stopping\n", id)
			return

		case job, ok := <-lt.jobs:
			if !ok {
				return
			}
			result := lt.executeRequest(job)
			select {
			case lt.results <- result:

			case <-lt.ctx.Done():
				return
			}
		}
	}
}

// executeRequest performs a single HTTP request and records its result.
func (lt *LoadTester) executeRequest(job Job) Result {
	start := time.Now()

	var req *http.Request
	var err error

	req, err = http.NewRequestWithContext(
		lt.ctx,
		job.Method,
		job.URL,
		bytes.NewBuffer(job.Body),
	)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Error:     err,
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	if job.Method == "POST" || job.Method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := lt.client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Error:     err,
			Duration:  duration,
			Timestamp: time.Now(),
		}
	}
	defer resp.Body.Close()

	return Result{
		JobID:      job.ID,
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
	var (
		totalDuration time.Duration
		minDuration   time.Duration
		maxDuration   time.Duration
		successCount  int
		failCount     int
		statusCodes   = make(map[int]int)
		errors        []string
	)

	firstResult := true
	resultsReceived := 0

	for result := range lt.results {
		resultsReceived++
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

	lt.statsMu.Lock()

	lt.stats = Stats{
		TotalRequests:   resultsReceived,
		Successful:      successCount,
		Failed:          failCount,
		MinResponseTime: minDuration,
		MaxResponseTime: maxDuration,
		AvgResponseTime: avgDuration,
		StatusCodes:     statusCodes,
		Errors:          errors,
	}

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
	stats := lt.stats

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("LOAD TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("URL: %s\n", lt.config.URL)
	fmt.Printf("Method: %s\n", lt.config.Method)
	fmt.Printf("Concurrency: %d\n", lt.config.Concurrency)
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
	URL             string        `json:"url"`
	Method          string        `json:"method"`
	Concurrency     int           `json:"concurrency"`
	Rate            int           `json:"rate"`
	TotalRequests   int           `json:"total_requests"`
	Successful      int           `json:"successful"`
	Failed          int           `json:"failed"`
	SuccessRate     float64       `json:"success_rate"`
	MinResponseTime time.Duration `json:"min_response_time_ns"`
	MaxResponseTime time.Duration `json:"max_response_time_ns"`
	AvgResponseTime time.Duration `json:"avg_response_time_ns"`
	RequestsPerSec  float64       `json:"requests_per_sec"`
	TotalDuration   time.Duration `json:"total_duration_ns"`
	StatusCodes     map[int]int   `json:"status_codes"`
	Errors          []string      `json:"errors,omitempty"`
}

// JSONReport serializes the run results as indented JSON.
func (lt *LoadTester) JSONReport() ([]byte, error) {
	stats := lt.stats

	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	report := Report{
		URL:             lt.config.URL,
		Method:          lt.config.Method,
		Concurrency:     lt.config.Concurrency,
		Rate:            lt.config.Rate,
		TotalRequests:   stats.TotalRequests,
		Successful:      stats.Successful,
		Failed:          stats.Failed,
		SuccessRate:     successRate,
		MinResponseTime: stats.MinResponseTime,
		MaxResponseTime: stats.MaxResponseTime,
		AvgResponseTime: stats.AvgResponseTime,
		RequestsPerSec:  stats.RequestsPerSec,
		TotalDuration:   stats.TotalDuration,
		StatusCodes:     stats.StatusCodes,
		Errors:          stats.Errors,
	}
	return json.MarshalIndent(report, "", "  ")
}
