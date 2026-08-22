package storm

import (
	"testing"
	"time"
)

type testError string

func (e testError) Error() string { return string(e) }

func TestHistogramPercentile(t *testing.T) {
	tests := []struct {
		name      string
		durations []time.Duration
		pct       float64
		want      time.Duration
	}{
		{
			name:      "10 durations, p50",
			durations: []time.Duration{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			pct:       50,
			want:      50 * time.Millisecond,
		},
		{
			name:      "10 durations, p95",
			durations: []time.Duration{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			pct:       95,
			want:      100 * time.Millisecond,
		},
		{
			name:      "10 durations, p99",
			durations: []time.Duration{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			pct:       99,
			want:      100 * time.Millisecond,
		},
		{
			name:      "empty histogram",
			durations: []time.Duration{},
			pct:       50,
			want:      0,
		},
		{
			name:      "single value",
			durations: []time.Duration{50},
			pct:       50,
			want:      50 * time.Millisecond,
		},
		{
			name:      "all same value",
			durations: []time.Duration{10, 10, 10, 10, 10},
			pct:       95,
			want:      10 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHistogram()
			for _, d := range tc.durations {
				h.Observe(d * time.Millisecond)
			}
			got := h.Percentile(tc.pct)
			if got != tc.want {
				t.Errorf("Percentile(%v) = %v, want %v", tc.pct, got, tc.want)
			}
		})
	}
}

func TestCollectorMatchesAggregate(t *testing.T) {
	// Build a set of results and compare streaming Collector with batch Aggregate.
	results := make([]Result, 100)
	for i := range results {
		results[i] = Result{
			JobID:      int64(i + 1),
			StatusCode: 200,
			Duration:   time.Duration(i+1) * time.Millisecond,
		}
	}

	// Batch
	batch := Aggregate(results)

	// Streaming
	c := NewCollector()
	for _, r := range results {
		c.Add(r)
	}
	stream := c.Stats()

	if stream.TotalRequests != batch.TotalRequests {
		t.Errorf("TotalRequests = %d, want %d", stream.TotalRequests, batch.TotalRequests)
	}
	if stream.Successful != batch.Successful {
		t.Errorf("Successful = %d, want %d", stream.Successful, batch.Successful)
	}
	if stream.Failed != batch.Failed {
		t.Errorf("Failed = %d, want %d", stream.Failed, batch.Failed)
	}
	if stream.MinResponseTime != batch.MinResponseTime {
		t.Errorf("MinResponseTime = %v, want %v", stream.MinResponseTime, batch.MinResponseTime)
	}
	if stream.MaxResponseTime != batch.MaxResponseTime {
		t.Errorf("MaxResponseTime = %v, want %v", stream.MaxResponseTime, batch.MaxResponseTime)
	}
	if stream.AvgResponseTime != batch.AvgResponseTime {
		t.Errorf("AvgResponseTime = %v, want %v", stream.AvgResponseTime, batch.AvgResponseTime)
	}
}

func TestCollectorWithErrors(t *testing.T) {
	c := NewCollector()

	c.Add(Result{StatusCode: 200, Duration: 10 * time.Millisecond})
	c.Add(Result{StatusCode: 200, Duration: 20 * time.Millisecond})
	c.Add(Result{Error: testError("connection refused"), JobID: 3})

	s := c.Stats()
	if s.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d, want 3", s.TotalRequests)
	}
	if s.Successful != 2 {
		t.Errorf("Successful = %d, want 2", s.Successful)
	}
	if s.Failed != 1 {
		t.Errorf("Failed = %d, want 1", s.Failed)
	}
	if len(s.Errors) != 1 {
		t.Errorf("Errors = %d, want 1", len(s.Errors))
	}
}

func TestCollectorStatusCodes(t *testing.T) {
	c := NewCollector()

	c.Add(Result{StatusCode: 200, Duration: 10 * time.Millisecond})
	c.Add(Result{StatusCode: 200, Duration: 20 * time.Millisecond})
	c.Add(Result{StatusCode: 500, Duration: 30 * time.Millisecond})
	c.Add(Result{StatusCode: 404, Duration: 5 * time.Millisecond})

	s := c.Stats()
	if s.StatusCodes[200] != 2 {
		t.Errorf("StatusCodes[200] = %d, want 2", s.StatusCodes[200])
	}
	if s.StatusCodes[500] != 1 {
		t.Errorf("StatusCodes[500] = %d, want 1", s.StatusCodes[500])
	}
	if s.StatusCodes[404] != 1 {
		t.Errorf("StatusCodes[404] = %d, want 1", s.StatusCodes[404])
	}
}
