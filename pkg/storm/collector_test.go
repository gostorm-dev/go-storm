package storm

import (
	"testing"
	"time"
)

type testError string

func (e testError) Error() string { return string(e) }

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
