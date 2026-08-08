package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Input Configuration
type Config struct {
	URL         string        // API endpoint
	TotalReqs   int           // Total requests to send
	Concurrency int           // Number of parallel workers
	Timeout     time.Duration // Request timeout
	Method      string        // GET, POST, PUT, DELETE
	Payload     []byte        // For POST requests
}

// Job represents a single request task
type Job struct {
	ID     int
	URL    string
	Method string
	Body   []byte
}

// Result captures response metrics
type Result struct {
	JobID      int
	StatusCode int
	Duration   time.Duration
	Error      error
	Timestamp  time.Time
}

// Aggregate Statistics
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
}

func NewLoadTester(ctx context.Context, config Config) *LoadTester {
	ctx, cancel := context.WithCancel(ctx)

	return &LoadTester{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
		jobs:    make(chan Job, config.TotalReqs),
		results: make(chan Result, config.TotalReqs),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (lt *LoadTester) worker(id int) {
	defer lt.wg.Done()

	fmt.Printf("🚀 Worker %d started\n", id)

	for {
		select {
		case <-lt.ctx.Done():
			fmt.Printf("🛑 Worker %d stopping\n", id)
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

func (lt *LoadTester) produceJobs() {
	defer close(lt.jobs)

	for i := 0; i < lt.config.TotalReqs; i++ {
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
			// job successfully queued
		}
	}
}

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

	for result := range lt.results {

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
		TotalRequests:   lt.config.TotalReqs,
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

func (lt *LoadTester) Run() (Stats, error) {
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

// Pretty print results
func (lt *LoadTester) PrintStats() {
	stats := lt.stats

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 LOAD TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("📍 URL: %s\n", lt.config.URL)
	fmt.Printf("🔧 Method: %s\n", lt.config.Method)
	fmt.Printf("👥 Concurrency: %d\n", lt.config.Concurrency)
	fmt.Printf("📦 Total Requests: %d\n", stats.TotalRequests)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("✅ Successful: %d\n", stats.Successful)
	fmt.Printf("❌ Failed: %d\n", stats.Failed)
	fmt.Printf("📈 Success Rate: %.2f%%\n",
		float64(stats.Successful)/float64(stats.TotalRequests)*100)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("⏱️  Min Response: %v\n", stats.MinResponseTime)
	fmt.Printf("⏱️  Max Response: %v\n", stats.MaxResponseTime)
	fmt.Printf("⏱️  Avg Response: %v\n", stats.AvgResponseTime)
	fmt.Printf("🚀 Requests/sec: %.2f\n", stats.RequestsPerSec)
	fmt.Printf("⏰ Total Duration: %v\n", stats.TotalDuration)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println("📊 Status Code Distribution:")
	for code, count := range stats.StatusCodes {
		fmt.Printf("   %d: %d requests\n", code, count)
	}
	if len(stats.Errors) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("❌ Errors:")
		for _, err := range stats.Errors {
			fmt.Printf("   %s\n", err)
		}
	}
	fmt.Println(strings.Repeat("=", 60))
}

func main() {
	// CLI Arguments
	url := flag.String("url", "https://hariomtanu.com", "Target URL")
	total := flag.Int("n", 100, "Total requests")
	concurrency := flag.Int("c", 10, "Concurrency level")
	method := flag.String("method", "GET", "HTTP Method")
	timeout := flag.Int("timeout", 10, "Request timeout in seconds")

	flag.Parse()

	// Sample payload for POST requests
	var payload []byte
	if *method == "POST" || *method == "PUT" {
		payload = []byte(`{"test": "data"}`)
	}

	config := Config{
		URL:         *url,
		TotalReqs:   *total,
		Concurrency: *concurrency,
		Timeout:     time.Duration(*timeout) * time.Second,
		Method:      *method,
		Payload:     payload,
	}

	fmt.Printf("🚀 Starting Load Test\n")
	fmt.Printf("📍 Target: %s\n", config.URL)
	fmt.Printf("📦 Total: %d requests\n", config.TotalReqs)
	fmt.Printf("👥 Concurrency: %d workers\n", config.Concurrency)
	fmt.Println("\n⏳ Running...")

	// Create and run load tester
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tester := NewLoadTester(ctx, config)
	_, err := tester.Run()
	if err != nil {
		log.Fatal("Test failed:", err)
	}
	// Print results
	tester.PrintStats()
}
