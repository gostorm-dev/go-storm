package transport

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func BenchmarkBufferPoolGetPut(b *testing.B) {
	pool := NewBufferPool(32 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		pool.Put(buf)
	}
}

func BenchmarkBufferPoolGetPutParallel(b *testing.B) {
	pool := NewBufferPool(32 * 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			pool.Put(buf)
		}
	})
}

func BenchmarkStatsRecordConnection(b *testing.B) {
	stats := &Stats{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.RecordConnectionCreated()
		stats.RecordConnectionReused()
		stats.RecordConnectionClosed()
	}
}

func BenchmarkStatsRecordConnectionParallel(b *testing.B) {
	stats := &Stats{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stats.RecordConnectionCreated()
			stats.RecordConnectionReused()
			stats.RecordConnectionClosed()
		}
	})
}

func BenchmarkStatsSnapshot(b *testing.B) {
	stats := &Stats{}
	for i := 0; i < 1000; i++ {
		stats.RecordConnectionCreated()
		stats.RecordConnectionReused()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stats.Snapshot()
	}
}

func BenchmarkNewTransport(b *testing.B) {
	cfg := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(cfg)
	}
}

func BenchmarkNewTransportWithStats(b *testing.B) {
	cfg := DefaultConfig()
	stats := &Stats{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewWithStats(cfg, stats)
	}
}

func BenchmarkTransportSnapshot(b *testing.B) {
	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	for i := 0; i < 1000; i++ {
		transport.Stats().RecordConnectionCreated()
		transport.Stats().RecordConnectionReused()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = transport.Snapshot()
	}
}

func BenchmarkConnectionReuse(b *testing.B) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	transport := New(cfg)
	client := &http.Client{
		Transport: transport,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkConnectionReuseParallel(b *testing.B) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	transport := New(cfg)
	client := &http.Client{
		Transport: transport,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(server.URL)
			if err != nil {
				b.Fatal(err)
			}
			resp.Body.Close()
		}
	})
}

func BenchmarkConnectionPoolContention(b *testing.B) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	var wg sync.WaitGroup
	workers := 50

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(workers)
		for j := 0; j < workers; j++ {
			go func() {
				defer wg.Done()
				client := &http.Client{
					Transport: transport.Transport,
				}
				resp, err := client.Get(server.URL)
				if err != nil {
					return // Ignore errors in benchmark
				}
				resp.Body.Close()
			}()
		}
		wg.Wait()
	}
}
