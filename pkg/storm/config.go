package storm

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

// Config holds every parameter the engine needs.
type Config struct {
	URL         string
	TotalReqs   int
	Concurrency int
	Timeout     time.Duration
	Method      string
	Payload     []byte
	Rate        int
}

// Job represents one HTTP request to fire.
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
		jobs:       make(chan Job, config.TotalReqs),
		results:    make(chan Result, config.Concurrency),
		ctx:        ctx,
		cancel:     cancel,
		thresholds: DefaultThresholds(),
	}

	if config.Rate > 0 {
		lt.limiter = rate.NewLimiter(rate.Limit(config.Rate), config.Rate)
	}

	return lt
}
