package transport

import (
	"sync"
	"testing"
)

func TestStatsRecordConnectionCreated(t *testing.T) {
	stats := &Stats{}

	stats.RecordConnectionCreated()
	stats.RecordConnectionCreated()
	stats.RecordConnectionCreated()

	if stats.ConnectionsCreated.Load() != 3 {
		t.Errorf("ConnectionsCreated = %d, want 3", stats.ConnectionsCreated.Load())
	}
	if stats.ActiveConnections.Load() != 3 {
		t.Errorf("ActiveConnections = %d, want 3", stats.ActiveConnections.Load())
	}
}

func TestStatsRecordConnectionReused(t *testing.T) {
	stats := &Stats{}

	stats.RecordConnectionReused()
	stats.RecordConnectionReused()

	if stats.ConnectionsReused.Load() != 2 {
		t.Errorf("ConnectionsReused = %d, want 2", stats.ConnectionsReused.Load())
	}
	if stats.PoolHits.Load() != 2 {
		t.Errorf("PoolHits = %d, want 2", stats.PoolHits.Load())
	}
}

func TestStatsRecordConnectionClosed(t *testing.T) {
	stats := &Stats{}

	stats.RecordConnectionCreated()
	stats.RecordConnectionCreated()
	stats.RecordConnectionClosed()

	if stats.ConnectionsClosed.Load() != 1 {
		t.Errorf("ConnectionsClosed = %d, want 1", stats.ConnectionsClosed.Load())
	}
	if stats.ActiveConnections.Load() != 1 {
		t.Errorf("ActiveConnections = %d, want 1", stats.ActiveConnections.Load())
	}
}

func TestStatsGetConnectionReuseRatio(t *testing.T) {
	stats := &Stats{}

	// No connections
	if ratio := stats.GetConnectionReuseRatio(); ratio != 0 {
		t.Errorf("GetConnectionReuseRatio() = %f, want 0", ratio)
	}

	// 2 created, 8 reused = 80% reuse ratio
	for i := 0; i < 2; i++ {
		stats.RecordConnectionCreated()
	}
	for i := 0; i < 8; i++ {
		stats.RecordConnectionReused()
	}

	ratio := stats.GetConnectionReuseRatio()
	if ratio != 80.0 {
		t.Errorf("GetConnectionReuseRatio() = %f, want 80.0", ratio)
	}
}

func TestStatsGetPoolHitRatio(t *testing.T) {
	stats := &Stats{}

	// 7 hits, 3 misses = 70% hit ratio
	for i := 0; i < 7; i++ {
		stats.RecordConnectionReused()
	}
	for i := 0; i < 3; i++ {
		stats.RecordPoolMiss()
	}

	ratio := stats.GetPoolHitRatio()
	if ratio != 70.0 {
		t.Errorf("GetPoolHitRatio() = %f, want 70.0", ratio)
	}
}

func TestStatsSnapshot(t *testing.T) {
	stats := &Stats{}

	stats.RecordConnectionCreated()
	stats.RecordConnectionReused()
	stats.RecordConnectionClosed()
	stats.RecordPoolMiss()

	snapshot := stats.Snapshot()

	if snapshot.ConnectionsCreated != 1 {
		t.Errorf("Snapshot.ConnectionsCreated = %d, want 1", snapshot.ConnectionsCreated)
	}
	if snapshot.ConnectionsReused != 1 {
		t.Errorf("Snapshot.ConnectionsReused = %d, want 1", snapshot.ConnectionsReused)
	}
	if snapshot.ConnectionsClosed != 1 {
		t.Errorf("Snapshot.ConnectionsClosed = %d, want 1", snapshot.ConnectionsClosed)
	}
	if snapshot.PoolMisses != 1 {
		t.Errorf("Snapshot.PoolMisses = %d, want 1", snapshot.PoolMisses)
	}
}

func TestStatsReset(t *testing.T) {
	stats := &Stats{}

	stats.RecordConnectionCreated()
	stats.RecordConnectionReused()
	stats.RecordConnectionClosed()

	stats.Reset()

	if stats.ConnectionsCreated.Load() != 0 {
		t.Errorf("ConnectionsCreated after reset = %d, want 0", stats.ConnectionsCreated.Load())
	}
	if stats.ConnectionsReused.Load() != 0 {
		t.Errorf("ConnectionsReused after reset = %d, want 0", stats.ConnectionsReused.Load())
	}
	if stats.ConnectionsClosed.Load() != 0 {
		t.Errorf("ConnectionsClosed after reset = %d, want 0", stats.ConnectionsClosed.Load())
	}
}

func TestStatsConcurrent(t *testing.T) {
	stats := &Stats{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats.RecordConnectionCreated()
			stats.RecordConnectionReused()
			stats.RecordConnectionClosed()
		}()
	}
	wg.Wait()

	// All operations should be accounted for
	if stats.ConnectionsCreated.Load() != 100 {
		t.Errorf("ConnectionsCreated = %d, want 100", stats.ConnectionsCreated.Load())
	}
	if stats.ConnectionsReused.Load() != 100 {
		t.Errorf("ConnectionsReused = %d, want 100", stats.ConnectionsReused.Load())
	}
	if stats.ConnectionsClosed.Load() != 100 {
		t.Errorf("ConnectionsClosed = %d, want 100", stats.ConnectionsClosed.Load())
	}
}

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool(1024)

	if pool.Size() != 1024 {
		t.Errorf("BufferPool.Size() = %d, want 1024", pool.Size())
	}

	// Get buffer
	buf := pool.Get()
	if buf == nil {
		t.Fatal("BufferPool.Get() returned nil")
	}
	if len(*buf) != 1024 {
		t.Errorf("Buffer length = %d, want 1024", len(*buf))
	}

	// Put buffer back
	pool.Put(buf)

	// Get another buffer (should reuse)
	buf2 := pool.Get()
	if buf2 == nil {
		t.Fatal("BufferPool.Get() returned nil after Put")
	}
}

func TestBufferPoolConcurrent(t *testing.T) {
	pool := NewBufferPool(1024)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := pool.Get()
			if buf == nil {
				t.Error("BufferPool.Get() returned nil")
				return
			}
			pool.Put(buf)
		}()
	}
	wg.Wait()
}

func TestTransport(t *testing.T) {
	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	if transport == nil {
		t.Fatal("NewTransport() returned nil")
	}
	if transport.Transport == nil {
		t.Error("Transport.Transport is nil")
	}
	if transport.Stats() == nil {
		t.Error("Transport.Stats() returned nil")
	}
	if transport.Config().MaxIdleConns != 200 {
		t.Errorf("Transport.Config().MaxIdleConns = %d, want 200", transport.Config().MaxIdleConns)
	}
}

func TestTransportSnapshot(t *testing.T) {
	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	transport.Stats().RecordConnectionCreated()
	transport.Stats().RecordConnectionReused()

	snapshot := transport.Snapshot()

	if snapshot.ConnectionsCreated != 1 {
		t.Errorf("Snapshot.ConnectionsCreated = %d, want 1", snapshot.ConnectionsCreated)
	}
	if snapshot.ConnectionsReused != 1 {
		t.Errorf("Snapshot.ConnectionsReused = %d, want 1", snapshot.ConnectionsReused)
	}
}

func TestTransportClose(t *testing.T) {
	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	err := transport.Close()
	if err != nil {
		t.Errorf("Transport.Close() error = %v", err)
	}
}

func TestTransportClient(t *testing.T) {
	cfg := DefaultConfig()
	transport := NewTransport(cfg)

	client := transport.Client(30)
	if client == nil {
		t.Fatal("Transport.Client() returned nil")
	}
}
