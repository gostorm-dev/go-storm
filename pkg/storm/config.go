package storm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gostorm-dev/go-storm/internal/transport"
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

	// Duration runs the test for a wall-clock window instead of a request
	// count. Zero means count mode (TotalReqs governs).
	// Exactly one of Duration / TotalReqs must be set.
	Duration time.Duration

	// Headers applied to every request. User-supplied headers always win
	// over engine defaults such as Content-Type.
	Headers http.Header

	// Transport configuration for connection pooling
	TransportConfig *transport.Config
}

// Job represents one HTTP request to fire.
type Job struct {
	ID      int64
	URL     string
	Method  string
	Body    []byte
	Headers http.Header
}

// ParseHeaderSpec parses a curl-style "Key: Value" header specification.
// The split happens on the first colon so values may themselves contain
// colons (URLs, timestamps). The returned key is ready for http.Header,
// which canonicalizes names on Add.
func ParseHeaderSpec(spec string) (key, val string, err error) {
	k, v, found := strings.Cut(spec, ":")
	if !found {
		return "", "", fmt.Errorf("invalid header %q: expected \"Key: Value\" format", spec)
	}
	key = strings.TrimSpace(k)
	val = strings.TrimSpace(v)
	if key == "" {
		return "", "", fmt.Errorf("invalid header %q: empty header name", spec)
	}
	return key, val, nil
}

// jobBufferFactor sizes the jobs channel relative to concurrency.
// A window of 2 jobs per worker absorbs producer stalls (e.g. rate
// limiter ticks) without pre-allocating memory proportional to TotalReqs.
const jobBufferFactor = 2

// Result captures response metrics for a single request.
type Result struct {
	JobID      int64
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

// minDuration is the smallest meaningful duration-mode window. Below this,
// startup transients (connection setup, TLS handshakes, scheduler warm-up)
// dominate the stats and the results mislead more than inform.
const minDuration = time.Second

// Validate checks the config before a run starts.
func (c Config) Validate() error {
	if c.Rate < 0 {
		return fmt.Errorf("rate cannot be negative, got %d", c.Rate)
	}
	switch {
	case c.Duration < 0:
		return fmt.Errorf("duration cannot be negative, got %s", c.Duration)
	case c.Duration > 0 && c.TotalReqs > 0:
		return fmt.Errorf(
			"--requests (-n) and --duration (-d) are mutually exclusive: "+
				"got n=%d AND d=%s — pick one workload definition",
			c.TotalReqs, c.Duration)
	case c.Duration == 0 && c.TotalReqs <= 0:
		return fmt.Errorf("no workload defined: set --requests (-n) OR --duration (-d)")
	case c.Duration > 0 && c.Duration < minDuration:
		return fmt.Errorf(
			"duration %s too short — minimum is %v for meaningful results",
			c.Duration, minDuration)
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

	// Create transport with connection pooling
	var client *http.Client
	var tStats *transport.Stats
	if config.TransportConfig != nil {
		// Use custom transport with connection pooling
		t := transport.NewTransport(*config.TransportConfig)
		tStats = t.Stats()
		client = &http.Client{
			Transport: t.Transport,
			Timeout:   config.Timeout,
		}
	} else {
		// Use default transport (backward compatible)
		client = &http.Client{
			Timeout: config.Timeout,
		}
	}

	// Create connection stats for httptrace
	connStats := &ConnectionStats{}

	lt := &LoadTester{
		config: config,
		client: client,
		// Buffer only a small window of jobs instead of all TotalReqs.
		// Pre-allocating TotalReqs slots costs ~64 bytes per job upfront,
		// which is hundreds of MB (or GBs) for large -n values before the
		// test even starts. produceJobs keeps the small buffer topped up,
		// so workers never starve and memory stays O(concurrency).
		jobs:           make(chan Job, config.Concurrency*jobBufferFactor),
		results:        make(chan Result, config.Concurrency),
		ctx:            ctx,
		cancel:         cancel,
		thresholds:     DefaultThresholds(),
		transportStats: tStats,
		connStats:      connStats,
	}

	if config.Rate > 0 {
		lt.limiter = rate.NewLimiter(rate.Limit(config.Rate), config.Rate)
	}

	return lt
}
