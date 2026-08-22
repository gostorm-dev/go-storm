package storm

import (
	"fmt"
	"time"
)

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
	hist          *LogHistogram
}

// NewCollector creates a ready-to-use streaming aggregator.
func NewCollector() *Collector {
	return &Collector{
		statusCodes:  make(map[int]int),
		errorSamples: make(map[string]int),
		hist:         NewLogHistogram(),
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
		P90:             c.hist.Percentile(90),
		P95:             c.hist.Percentile(95),
		P99:             c.hist.Percentile(99),
		P999:            c.hist.Percentile(99.9),
		StatusCodes:     c.statusCodes,
		Errors:          c.errors,
	}
}
