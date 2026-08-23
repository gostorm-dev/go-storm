package storm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gostorm-dev/go-storm/internal/transport"
)

// Config holds every parameter the engine needs.
type Config struct {
	URL         string
	TotalReqs   int
	Concurrency int
	Timeout     time.Duration
	Method      string
	Payload     []byte

	// Rate enables constant-arrival scheduling at exactly Rate jobs/sec:
	// duration mode dispatches exactly ceil(Duration × Rate) requests on
	// fixed slots; count mode paces TotalReqs at Rate per second. Zero
	// means unpaced.
	Rate int

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
	// Duration is the FULL response time: request start until headers and
	// body are both received (ecosystem-comparable with vegeta/k6/ab).
	Duration time.Duration
	// TTFB is time to first byte: request start until response headers
	// arrive. Zero on transport errors. Server-processing insight that
	// most load testers do not expose.
	TTFB      time.Duration
	Error     error
	Timestamp time.Time
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
	P90             time.Duration
	P95             time.Duration
	P99             time.Duration
	P999            time.Duration

	// Time-to-first-byte distribution (zero when nothing succeeded).
	TtfbAvg        time.Duration
	TtfbP50        time.Duration
	TtfbP90        time.Duration
	TtfbP95        time.Duration
	TtfbP99        time.Duration
	RequestsPerSec float64
	StatusCodes    map[int]int
	Errors         []string

	// Schedule adherence for rate-limited runs (nil otherwise): how well
	// the generator held its own arrival slots.
	Arrival *ArrivalAccuracy

	// Set when saturation kill mode ended the run before the requested
	// workload completed. Consumers should treat such results as a warning,
	// not a clean measurement.
	KilledOnSaturation bool    `json:"killed_on_saturation,omitempty"`
	KillReason         string  `json:"kill_reason,omitempty"`
	KilledAtMS         float64 `json:"killed_at_ms,omitempty"`
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

// minAutoIdlePerHost is the floor for auto-sized idle pools. Sizing the
// per-host pool to exactly concurrency still churns: transient checkout
// races momentarily exceed the limit and connections get closed under
// load. Measured on a real network (AWS c6i pair), 2×concurrency held
// sustained throughput rock-stable while exact sizing swung −75%.
const minAutoIdlePerHost = 256

// sizeConnectionPool guarantees an idle pool with enough headroom that
// workers never wait on a dial. A small pool forces connect/close churn
// whose syscall cost dominates kernel time on real networks and halves
// sustained throughput (see BENCHMARKS.md, Rounds 2–3). Values are only
// ever raised, never lowered; concurrency ≤ 0 is a no-op.
func sizeConnectionPool(tc *transport.Config, concurrency int) {
	if tc == nil || concurrency <= 0 {
		return
	}
	target := 2 * concurrency
	if target < minAutoIdlePerHost {
		target = minAutoIdlePerHost
	}
	if tc.MaxIdleConnsPerHost < target {
		tc.MaxIdleConnsPerHost = target
	}
	if tc.MaxIdleConns < target {
		tc.MaxIdleConns = target
	}
}

// NewLoadTester builds a LoadTester from config.
// It owns a cancellable context so workers can shut down gracefully.
func NewLoadTester(ctx context.Context, config Config) *LoadTester {
	ctx, cancel := context.WithCancel(ctx)

	// Resolve transport: the engine never runs on Go's stdlib defaults
	// (2 idle connections per host) — that silently converts a load test
	// into a connection-dial benchmark.
	if config.TransportConfig == nil {
		tc := transport.DefaultConfig()
		config.TransportConfig = &tc
	}
	sizeConnectionPool(config.TransportConfig, config.Concurrency)

	// Create transport with connection pooling
	t := transport.NewTransport(*config.TransportConfig)
	tStats := t.Stats()
	client := &http.Client{
		Transport: t.Transport,
		Timeout:   config.Timeout,
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

	return lt
}
