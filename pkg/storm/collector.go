package storm

import (
	"fmt"
	"math"
	"time"
)

// defaultBuckets defines logarithmic bucket upper bounds in milliseconds.
// Covering 1ms to 10s — typical HTTP latency range for load testing.
// Memory: fixed 9 buckets + 1 overflow = 10 counters total.
var defaultBuckets = []float64{1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

// Histogram counts observations into fixed logarithmic buckets.
// It provides O(1) memory and O(1) per-observation recording, at the cost
// of approximate percentile values (bounded by bucket width).
type Histogram struct {
	buckets []float64 // upper bounds in ms (sorted ascending)
	counts  []int     // count per bucket (+ overflow)
	total   int
}

// NewHistogram creates a histogram with the default bucket boundaries.
func NewHistogram() *Histogram {
	return &Histogram{
		buckets: defaultBuckets,
		counts:  make([]int, len(defaultBuckets)+1),
	}
}

// Observe records one duration into the appropriate bucket.
func (h *Histogram) Observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	for i, bound := range h.buckets {
		if ms <= bound {
			h.counts[i]++
			h.total++
			return
		}
	}
	// Overflow: larger than all buckets
	h.counts[len(h.buckets)]++
	h.total++
}

// Percentile returns the approximate duration at the given percentile (0-100)
// using linear interpolation within the containing histogram bucket, assuming
// observations are uniformly distributed inside it. This is far more accurate
// than snapping to the bucket's upper bound, especially for wide buckets.
func (h *Histogram) Percentile(pct float64) time.Duration {
	if h.total == 0 {
		return 0
	}

	target := int(math.Ceil(float64(h.total) * pct / 100))
	if target < 1 {
		target = 1
	}

	cumulative := 0
	for i, count := range h.counts {
		prev := cumulative
		cumulative += count
		if cumulative >= target && count > 0 {
			if i >= len(h.buckets) {
				// Overflow bucket has no upper bound; report the last known one.
				return time.Duration(h.buckets[len(h.buckets)-1]) * time.Millisecond
			}
			lower := 0.0
			if i > 0 {
				lower = h.buckets[i-1]
			}
			fraction := float64(target-prev) / float64(count)
			value := lower + fraction*(h.buckets[i]-lower)
			return time.Duration(value * float64(time.Millisecond))
		}
	}

	return time.Duration(h.buckets[len(h.buckets)-1]) * time.Millisecond
}

const maxStoredErrors = 100

// Collector incrementally aggregates results into Stats.
// Unlike Aggregate(), it never stores all results — each Add() updates
// running counters and the histogram in O(1) time with fixed memory.
type Collector struct {
	count         int
	successCount  int
	failCount     int
	totalDuration time.Duration
	minDuration   time.Duration
	maxDuration   time.Duration
	firstResult   bool
	statusCodes   map[int]int
	errors        []string
	errorCount    int
	errorSamples  map[string]int
	hist          *Histogram
}

// NewCollector creates a ready-to-use streaming aggregator.
func NewCollector() *Collector {
	return &Collector{
		statusCodes:  make(map[int]int),
		errorSamples: make(map[string]int),
		hist:         NewHistogram(),
		firstResult:  true,
	}
}

// Add incorporates one result into the running aggregation.
func (c *Collector) Add(r Result) {
	c.count++

	if r.Error != nil {
		c.failCount++
		c.errorCount++
		errKey := r.Error.Error()
		c.errorSamples[errKey]++
		// Store up to maxStoredErrors unique error messages
		if len(c.errors) < maxStoredErrors {
			c.errors = append(c.errors, fmt.Sprintf("Job %d: %v", r.JobID, r.Error))
		}
		return
	}

	c.statusCodes[r.StatusCode]++
	if r.StatusCode >= 200 && r.StatusCode < 400 {
		c.successCount++
	} else {
		c.failCount++
	}

	c.totalDuration += r.Duration
	c.hist.Observe(r.Duration)

	if c.firstResult {
		c.minDuration = r.Duration
		c.maxDuration = r.Duration
		c.firstResult = false
	} else {
		if r.Duration < c.minDuration {
			c.minDuration = r.Duration
		}
		if r.Duration > c.maxDuration {
			c.maxDuration = r.Duration
		}
	}
}

// Stats returns the final aggregated statistics.
func (c *Collector) Stats() Stats {
	var avg time.Duration
	if c.count > 0 {
		avg = c.totalDuration / time.Duration(c.count)
	}

	return Stats{
		TotalRequests:   c.count,
		Successful:      c.successCount,
		Failed:          c.failCount,
		MinResponseTime: c.minDuration,
		MaxResponseTime: c.maxDuration,
		AvgResponseTime: avg,
		P50:             c.hist.Percentile(50),
		P95:             c.hist.Percentile(95),
		P99:             c.hist.Percentile(99),
		StatusCodes:     c.statusCodes,
		Errors:          c.errors,
	}
}
