package storm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestLatencyIncludesBodyDownload proves the two latency definitions:
// a server that sends headers instantly but delays the body by 100ms must
// show full-response latency >= 100ms while TTFB stays far below.
// (Pre-change behavior reported ~0ms here — the old TTFB-as-latency bug.)
func TestLatencyIncludesBodyDownload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // push headers onto the wire immediately
		}
		time.Sleep(100 * time.Millisecond) // body transfer delay
		w.Write(make([]byte, 4096))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   10,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.TotalRequests != 10 || stats.Failed != 0 {
		t.Fatalf("total=%d failed=%d, want 10/0", stats.TotalRequests, stats.Failed)
	}

	if stats.P50 < 90*time.Millisecond {
		t.Errorf("full-response p50 = %v, want >= ~100ms (body download must be counted)", stats.P50)
	}
	if stats.AvgResponseTime < 90*time.Millisecond {
		t.Errorf("avg = %v, want >= ~100ms", stats.AvgResponseTime)
	}
	if stats.TtfbP50 == 0 {
		t.Fatal("TtfbP50 = 0, want captured")
	}
	if stats.TtfbP50 > 50*time.Millisecond {
		t.Errorf("ttfb p50 = %v, want << 100ms (headers were flushed immediately)", stats.TtfbP50)
	}
}

// TestTTFBParityOnFastEndpoint checks that without a meaningful body,
// full-response latency and TTFB converge — i.e. historical numbers for
// typical API workloads carry over within noise.
func TestTTFBParityOnFastEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   200,
		Concurrency: 10,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.P50 <= 0 || stats.TtfbP50 <= 0 {
		t.Fatalf("p50=%v ttfb=%v, want both positive", stats.P50, stats.TtfbP50)
	}
	drift := stats.P50 - stats.TtfbP50
	if drift < 0 {
		drift = -drift
	}
	if drift > 5*time.Millisecond {
		t.Errorf("|latency p50 %v − ttfb p50 %v| = %v, want ≈ 0 for empty bodies",
			stats.P50, stats.TtfbP50, drift)
	}
}

// TestBatchAggregateTTFBMirrorsStreaming keeps the dist coordinator path
// consistent with the streaming collector for TTFB aggregation.
func TestBatchAggregateTTFBMirrorsStreaming(t *testing.T) {
	results := make([]Result, 50)
	for i := range results {
		ttfb := time.Duration(i+1) * time.Millisecond
		results[i] = Result{
			StatusCode: 200,
			Duration:   ttfb + 5*time.Millisecond,
			TTFB:       ttfb,
		}
	}

	batch := Aggregate(results)

	c := NewCollector()
	for _, r := range results {
		c.Add(r)
	}
	stream := c.Stats()

	if batch.TtfbAvg != stream.TtfbAvg {
		t.Errorf("TtfbAvg batch=%v stream=%v, want equal", batch.TtfbAvg, stream.TtfbAvg)
	}
	checkRelErr(t, "batch vs stream ttfb p50", stream.TtfbP50, batch.TtfbP50)
}
