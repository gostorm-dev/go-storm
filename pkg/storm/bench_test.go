package storm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// makeResults builds n synthetic results with varying latencies.
func makeResults(n int) []Result {
	results := make([]Result, n)
	for i := range results {
		results[i] = Result{
			JobID:      int64(i + 1),
			Method:     "GET",
			StatusCode: 200,
			Duration:   time.Duration(i%1000+1) * time.Millisecond,
		}
	}
	return results
}

// BenchmarkAggregate measures the stats engine over growing result sets.
// This is the O(N log N) sort + O(N) memory path the streaming refactor
// targets, so the baseline must be recorded before any change.
func BenchmarkAggregate(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("results=%d", n), func(b *testing.B) {
			results := makeResults(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Aggregate(results)
			}
		})
	}
}

// BenchmarkExecute measures a single HTTP request against a local server.
func BenchmarkExecute(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &http.Client{Timeout: time.Second}
	job := Job{ID: 1, Method: "GET", URL: srv.URL}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Execute(context.Background(), client, job, nil)
	}
}

// BenchmarkRun measures the full producer → worker → consumer pipeline.
func BenchmarkRun(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   1000,
		Concurrency: 10,
		Timeout:     time.Second,
		Method:      "GET",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lt := NewLoadTester(context.Background(), cfg)
		if _, err := lt.Run(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCollectorCompare shows streaming Collector vs batch Aggregate.
// Same data, different memory models: O(concurrency) vs O(N).
func BenchmarkCollectorCompare(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		results := makeResults(n)

		b.Run(fmt.Sprintf("Aggregate/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Aggregate(results)
			}
		})

		b.Run(fmt.Sprintf("Collector/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := NewCollector()
				for _, r := range results {
					c.Add(r)
				}
				_ = c.Stats()
			}
		})
	}
}
