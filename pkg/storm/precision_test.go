package storm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestJSONLatencyPrecision guards against sub-millisecond truncation.
// A fast target (~0.5ms) must never be reported as 0ms.
func TestJSONLatencyPrecision(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Microsecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   20,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	lt := NewLoadTester(context.Background(), cfg)
	stats, err := lt.Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	data, err := ReportJSON(cfg, stats)
	if err != nil {
		t.Fatalf("ReportJSON() error = %v", err)
	}

	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if report.MinResponseTime <= 0 {
		t.Errorf("min_response_time_ms = %v, want > 0 (sub-ms truncation regression)", report.MinResponseTime)
	}
	if report.P50 < 0.4 || report.P50 > 5.0 {
		t.Errorf("p50_ms = %v, want ~0.5 (sleep was 500µs)", report.P50)
	}
}

// TestHistogramBoundaryClassification verifies fractional-millisecond
// observations land in the correct bucket (1.4ms belongs in ≤5, not ≤1).
func TestHistogramBoundaryClassification(t *testing.T) {
	h := NewHistogram()
	h.Observe(1400 * time.Microsecond) // 1.4ms

	// Bucket layout: [1, 5, 10, ...] → counts[0] is "≤1", counts[1] is "≤5".
	if h.counts[0] != 0 {
		t.Errorf("1.4ms wrongly landed in ≤1 bucket (counts=%v)", h.counts)
	}
	if h.counts[1] != 1 {
		t.Errorf("1.4ms should land in ≤5 bucket (counts=%v)", h.counts)
	}
	if h.total != 1 {
		t.Errorf("total = %d, want 1", h.total)
	}
}

// TestPercentileInterpolation verifies percentiles interpolate within a
// bucket instead of snapping to its upper bound. 10 observations of 6ms all
// land in the ≤10 bucket; p50 must estimate 7.5ms (midpoint), not 10ms.
func TestPercentileInterpolation(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < 10; i++ {
		h.Observe(6 * time.Millisecond)
	}

	got := h.Percentile(50)
	want := 7500 * time.Microsecond // 5 + 0.5*(10-5)
	if got != want {
		t.Errorf("Percentile(50) = %v, want %v (interpolated midpoint)", got, want)
	}
}
