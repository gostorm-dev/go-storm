package transport

import (
	"net/http"
	"sync"
	"sync/atomic"
)

// Stats tracks HTTP transport connection pool statistics.
type Stats struct {
	// Connection tracking
	ConnectionsCreated atomic.Int64
	ConnectionsReused  atomic.Int64
	ConnectionsClosed  atomic.Int64

	// Pool tracking
	PoolHits   atomic.Int64
	PoolMisses atomic.Int64

	// Error tracking
	ConnectionErrors atomic.Int64
	TLSErrors        atomic.Int64
	TimeoutErrors    atomic.Int64

	// Active connections
	ActiveConnections atomic.Int64
	IdleConnections   atomic.Int64
}

// RecordConnectionCreated records a new connection creation.
func (s *Stats) RecordConnectionCreated() {
	s.ConnectionsCreated.Add(1)
	s.ActiveConnections.Add(1)
}

// RecordConnectionReused records a connection reuse from the pool.
func (s *Stats) RecordConnectionReused() {
	s.ConnectionsReused.Add(1)
	s.PoolHits.Add(1)
}

// RecordConnectionClosed records a connection closure.
func (s *Stats) RecordConnectionClosed() {
	s.ConnectionsClosed.Add(1)
	s.ActiveConnections.Add(-1)
}

// RecordPoolMiss records a pool miss (new connection needed).
func (s *Stats) RecordPoolMiss() {
	s.PoolMisses.Add(1)
}

// RecordConnectionError records a connection error.
func (s *Stats) RecordConnectionError() {
	s.ConnectionErrors.Add(1)
}

// RecordTLSError records a TLS error.
func (s *Stats) RecordTLSError() {
	s.TLSErrors.Add(1)
}

// RecordTimeoutError records a timeout error.
func (s *Stats) RecordTimeoutError() {
	s.TimeoutErrors.Add(1)
}

// GetConnectionReuseRatio returns the connection reuse ratio as a percentage.
func (s *Stats) GetConnectionReuseRatio() float64 {
	total := s.ConnectionsCreated.Load() + s.ConnectionsReused.Load()
	if total == 0 {
		return 0
	}
	return float64(s.ConnectionsReused.Load()) / float64(total) * 100
}

// GetPoolHitRatio returns the pool hit ratio as a percentage.
func (s *Stats) GetPoolHitRatio() float64 {
	total := s.PoolHits.Load() + s.PoolMisses.Load()
	if total == 0 {
		return 0
	}
	return float64(s.PoolHits.Load()) / float64(total) * 100
}

// Snapshot returns a point-in-time snapshot of the stats.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ConnectionsCreated:   s.ConnectionsCreated.Load(),
		ConnectionsReused:    s.ConnectionsReused.Load(),
		ConnectionsClosed:    s.ConnectionsClosed.Load(),
		PoolHits:             s.PoolHits.Load(),
		PoolMisses:           s.PoolMisses.Load(),
		ConnectionErrors:     s.ConnectionErrors.Load(),
		TLSErrors:            s.TLSErrors.Load(),
		TimeoutErrors:        s.TimeoutErrors.Load(),
		ActiveConnections:    s.ActiveConnections.Load(),
		IdleConnections:      s.IdleConnections.Load(),
		ConnectionReuseRatio: s.GetConnectionReuseRatio(),
		PoolHitRatio:         s.GetPoolHitRatio(),
	}
}

// StatsSnapshot is a point-in-time snapshot of transport statistics.
type StatsSnapshot struct {
	ConnectionsCreated   int64
	ConnectionsReused    int64
	ConnectionsClosed    int64
	PoolHits             int64
	PoolMisses           int64
	ConnectionErrors     int64
	TLSErrors            int64
	TimeoutErrors        int64
	ActiveConnections    int64
	IdleConnections      int64
	ConnectionReuseRatio float64
	PoolHitRatio         float64
}

// Reset resets all statistics to zero.
func (s *Stats) Reset() {
	s.ConnectionsCreated.Store(0)
	s.ConnectionsReused.Store(0)
	s.ConnectionsClosed.Store(0)
	s.PoolHits.Store(0)
	s.PoolMisses.Store(0)
	s.ConnectionErrors.Store(0)
	s.TLSErrors.Store(0)
	s.TimeoutErrors.Store(0)
	s.ActiveConnections.Store(0)
	s.IdleConnections.Store(0)
}

// BufferPool is a thread-safe pool for reusing byte buffers.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool creates a new buffer pool with the specified buffer size.
func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
		size: size,
	}
}

// Get retrieves a buffer from the pool.
// The buffer may contain data from a previous use.
func (bp *BufferPool) Get() *[]byte {
	return bp.pool.Get().(*[]byte)
}

// Put returns a buffer to the pool for reuse.
func (bp *BufferPool) Put(buf *[]byte) {
	if buf != nil {
		bp.pool.Put(buf)
	}
}

// Size returns the buffer size in bytes.
func (bp *BufferPool) Size() int {
	return bp.size
}

// Transport wraps http.Transport with statistics tracking.
type Transport struct {
	*http.Transport
	stats      *Stats
	traceStats *TraceStats
	bufferPool *BufferPool
	config     Config
}

// NewTransport creates a new Transport with statistics tracking.
func NewTransport(cfg Config) *Transport {
	stats := &Stats{}
	traceStats := &TraceStats{}
	var bufferPool *BufferPool
	if cfg.EnableBufferPool {
		bufferPool = NewBufferPool(cfg.BufferSize)
	}

	return &Transport{
		Transport:  New(cfg),
		stats:      stats,
		traceStats: traceStats,
		bufferPool: bufferPool,
		config:     cfg,
	}
}

// Stats returns the transport statistics.
func (t *Transport) Stats() *Stats {
	return t.stats
}

// TraceStats returns the trace statistics.
func (t *Transport) TraceStats() *TraceStats {
	return t.traceStats
}

// BufferPool returns the buffer pool.
func (t *Transport) BufferPool() *BufferPool {
	return t.bufferPool
}

// Config returns the transport configuration.
func (t *Transport) Config() Config {
	return t.config
}

// Snapshot returns a point-in-time snapshot of the transport statistics.
func (t *Transport) Snapshot() StatsSnapshot {
	return t.stats.Snapshot()
}

// Close closes the transport and releases resources.
func (t *Transport) Close() error {
	// Close idle connections
	t.Transport.CloseIdleConnections()
	return nil
}

// Client creates an http.Client using this transport.
func (t *Transport) Client(timeout int) *http.Client {
	return &http.Client{
		Transport: t.Transport,
		Timeout:   0, // timeout handled by transport
	}
}
