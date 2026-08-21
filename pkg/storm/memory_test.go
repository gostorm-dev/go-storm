package storm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestJobsChannelBufferIsBounded guards against the memory landmine where
// the jobs channel was pre-allocated with capacity TotalReqs. A run of
// -n 10000000 must not allocate millions of Job structs before the test
// even starts; the buffer must stay proportional to concurrency.
func TestJobsChannelBufferIsBounded(t *testing.T) {
	config := Config{
		URL:         "http://example.com",
		TotalReqs:   10_000_000,
		Concurrency: 10,
		Timeout:     time.Second,
	}

	lt := NewLoadTester(context.Background(), config)
	defer lt.cancel()

	want := config.Concurrency * jobBufferFactor
	if got := cap(lt.jobs); got != want {
		t.Errorf("jobs channel capacity = %d, want %d (concurrency*%d)",
			got, want, jobBufferFactor)
	}
}

// BenchmarkNewLoadTesterMemory measures the allocations of constructing a
// LoadTester for a very large request count. Before the bounded-buffer fix
// this allocated ~64 bytes per requested job (64MB for -n 1000000).
func BenchmarkNewLoadTesterMemory(b *testing.B) {
	config := Config{
		URL:         "http://example.com",
		TotalReqs:   1_000_000,
		Concurrency: 10,
		Timeout:     time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lt := NewLoadTester(context.Background(), config)
		lt.cancel()
	}
}

// TestLargeRequestCountRunCompletes runs an end-to-end test with a request
// count far above what would fit in a TotalReqs-sized channel buffer,
// proving the pipeline streams jobs instead of pre-allocating them.
func TestLargeRequestCountRunCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	config := Config{
		URL:         srv.URL,
		TotalReqs:   100_000,
		Concurrency: 20,
		Timeout:     5 * time.Second,
	}
	lt := NewLoadTester(context.Background(), config)
	stats, err := lt.Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.TotalRequests != config.TotalReqs {
		t.Errorf("TotalRequests = %d, want %d", stats.TotalRequests, config.TotalReqs)
	}
	if stats.Failed != 0 {
		t.Errorf("Failed = %d, want 0", stats.Failed)
	}
}
