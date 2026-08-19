package storm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

// ConnectionStats tracks connection-level statistics.
type ConnectionStats struct {
	// Connection tracking
	ConnectionsCreated atomic.Int64
	ConnectionsReused  atomic.Int64

	// Pool tracking
	PoolHits   atomic.Int64
	PoolMisses atomic.Int64

	// DNS tracking
	DNSLookups atomic.Int64

	// TLS tracking
	TLSHandshakes atomic.Int64

	// TCP tracking
	TCPConnections atomic.Int64
}

// Snapshot returns a point-in-time snapshot of the connection stats.
func (cs *ConnectionStats) Snapshot() ConnectionStatsSnapshot {
	return ConnectionStatsSnapshot{
		ConnectionsCreated: cs.ConnectionsCreated.Load(),
		ConnectionsReused:  cs.ConnectionsReused.Load(),
		PoolHits:           cs.PoolHits.Load(),
		PoolMisses:         cs.PoolMisses.Load(),
		DNSLookups:         cs.DNSLookups.Load(),
		TLSHandshakes:      cs.TLSHandshakes.Load(),
		TCPConnections:     cs.TCPConnections.Load(),
	}
}

// ConnectionStatsSnapshot is a point-in-time snapshot of connection statistics.
type ConnectionStatsSnapshot struct {
	ConnectionsCreated int64
	ConnectionsReused  int64
	PoolHits           int64
	PoolMisses         int64
	DNSLookups         int64
	TLSHandshakes      int64
	TCPConnections     int64
}

// executeRequest performs a single HTTP request and records its result.
func (lt *LoadTester) executeRequest(job Job) Result {
	return Execute(lt.ctx, lt.client, job, lt.connStats)
}

// Execute performs a single HTTP request and records its result.
// Exported so distributed agents can reuse the same request logic.
func Execute(ctx context.Context, client *http.Client, job Job, connStats *ConnectionStats) Result {
	start := time.Now()

	req, err := http.NewRequestWithContext(
		ctx,
		job.Method,
		job.URL,
		bytes.NewBuffer(job.Body),
	)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     fmt.Errorf("invalid request: %w", err),
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	if job.Method == "POST" || job.Method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Add httptrace to track connection events.
	// Use atomic bool because ConnectStart fires from a different goroutine.
	var newConn atomic.Bool
	trace := &httptrace.ClientTrace{
		// ConnectStart is called when a new TCP connection starts
		// NOT called when reusing a pooled connection
		ConnectStart: func(network, addr string) {
			newConn.Store(true)
			if connStats != nil {
				connStats.ConnectionsCreated.Add(1)
				connStats.TCPConnections.Add(1)
			}
		},

		// ConnectDone is called when a TCP connection is established
		ConnectDone: func(network, addr string, err error) {
			if err != nil && connStats != nil {
				// Connection failed
				connStats.ConnectionsCreated.Add(-1)
			}
		},

		// DNSStart is called when a DNS lookup starts
		DNSStart: func(dnsInfo httptrace.DNSStartInfo) {
			if connStats != nil {
				connStats.DNSLookups.Add(1)
			}
		},

		// TLSHandshakeStart is called when TLS handshake starts
		TLSHandshakeStart: func() {
			if connStats != nil {
				connStats.TLSHandshakes.Add(1)
			}
		},
	}

	// Add the trace to the request context
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     err,
			Duration:  duration,
			Timestamp: time.Now(),
		}
	}
	defer resp.Body.Close()

	// CRITICAL: Drain the body so the connection can be returned to the pool.
	// Without this, Go discards the TCP connection instead of reusing it.
	io.Copy(io.Discard, resp.Body)

	// Track connection reuse: if ConnectStart did NOT fire, it was a pool hit.
	if connStats != nil {
		if !newConn.Load() {
			connStats.ConnectionsReused.Add(1)
			connStats.PoolHits.Add(1)
		} else {
			connStats.PoolMisses.Add(1)
		}
	}

	return Result{
		JobID:      job.ID,
		Method:     job.Method,
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Error:      nil,
		Timestamp:  time.Now(),
	}
}
