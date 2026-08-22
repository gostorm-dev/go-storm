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
