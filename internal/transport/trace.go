package transport

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
)

// TraceStats tracks connection events via httptrace.
type TraceStats struct {
	// Connection tracking
	ConnectionsCreated atomic.Int64
	ConnectionsReused  atomic.Int64

	// Pool tracking
	PoolHits   atomic.Int64
	PoolMisses atomic.Int64

	// DNS tracking
	DNSLookups    atomic.Int64
	DNSLookupTime atomic.Int64 // nanoseconds

	// TLS tracking
	TLSHandshakes    atomic.Int64
	TLSHandshakeTime atomic.Int64 // nanoseconds

	// TCP tracking
	TCPConnections atomic.Int64
	TCPConnectTime atomic.Int64 // nanoseconds
}

// Snapshot returns a point-in-time snapshot of the trace stats.
func (ts *TraceStats) Snapshot() TraceStatsSnapshot {
	return TraceStatsSnapshot{
		ConnectionsCreated: ts.ConnectionsCreated.Load(),
		ConnectionsReused:  ts.ConnectionsReused.Load(),
		PoolHits:           ts.PoolHits.Load(),
		PoolMisses:         ts.PoolMisses.Load(),
		DNSLookups:         ts.DNSLookups.Load(),
		TLSHandshakes:      ts.TLSHandshakes.Load(),
		TCPConnections:     ts.TCPConnections.Load(),
	}
}

// TraceStatsSnapshot is a point-in-time snapshot of trace statistics.
type TraceStatsSnapshot struct {
	ConnectionsCreated int64
	ConnectionsReused  int64
	PoolHits           int64
	PoolMisses         int64
	DNSLookups         int64
	TLSHandshakes      int64
	TCPConnections     int64
}

// Trace wraps an http.Client to track connection events via httptrace.
type Trace struct {
	client *http.Client
	stats  *TraceStats
}

// NewTrace creates a new Trace wrapper around an http.Client.
func NewTrace(client *http.Client) *Trace {
	return &Trace{
		client: client,
		stats:  &TraceStats{},
	}
}

// Stats returns the trace statistics.
func (t *Trace) Stats() *TraceStats {
	return t.stats
}

// Do executes an HTTP request with connection tracing.
func (t *Trace) Do(req *http.Request) (*http.Response, error) {
	// Create httptrace hooks
	trace := &httptrace.ClientTrace{
		// GetConn is called before a connection is obtained
		GetConn: func(hostPort string) {
			// This is called for every request, whether reusing or creating
		},

		// PutIdleConn is called when a connection is returned to the pool
		PutIdleConn: func(err error) {
			if err == nil {
				t.stats.PoolHits.Add(1)
				t.stats.ConnectionsReused.Add(1)
			}
		},

		// ConnectStart is called when a new TCP connection starts
		ConnectStart: func(network, addr string) {
			t.stats.ConnectionsCreated.Add(1)
			t.stats.TCPConnections.Add(1)
		},

		// ConnectDone is called when a TCP connection is established
		ConnectDone: func(network, addr string, err error) {
			if err != nil {
				// Connection failed, will be retried
				t.stats.ConnectionsCreated.Add(-1)
			}
		},

		// DNSStart is called when a DNS lookup starts
		DNSStart: func(dnsInfo httptrace.DNSStartInfo) {
			t.stats.DNSLookups.Add(1)
		},

		// DNSDone is called when a DNS lookup is complete
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			// DNS lookup completed
		},

		// TLSHandshakeStart is called when TLS handshake starts
		TLSHandshakeStart: func() {
			t.stats.TLSHandshakes.Add(1)
		},

		// TLSHandshakeDone is called when TLS handshake is complete
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			// TLS handshake completed
		},
	}

	// Add the trace to the request context
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	// Execute the request
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Client returns the underlying http.Client.
func (t *Trace) Client() *http.Client {
	return t.client
}

// TraceRoundTripper is a RoundTripper that tracks connection events.
type TraceRoundTripper struct {
	inner http.RoundTripper
	stats *TraceStats
}

// NewTraceRoundTripper creates a new TraceRoundTripper.
func NewTraceRoundTripper(inner http.RoundTripper) *TraceRoundTripper {
	return &TraceRoundTripper{
		inner: inner,
		stats: &TraceStats{},
	}
}

// Stats returns the trace statistics.
func (trt *TraceRoundTripper) Stats() *TraceStats {
	return trt.stats
}

// RoundTrip executes a single HTTP transaction with connection tracing.
func (trt *TraceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Create httptrace hooks
	trace := &httptrace.ClientTrace{
		// GetConn is called before a connection is obtained
		GetConn: func(hostPort string) {
			// This is called for every request
		},

		// PutIdleConn is called when a connection is returned to the pool
		PutIdleConn: func(err error) {
			if err == nil {
				trt.stats.PoolHits.Add(1)
				trt.stats.ConnectionsReused.Add(1)
			}
		},

		// ConnectStart is called when a new TCP connection starts
		ConnectStart: func(network, addr string) {
			trt.stats.ConnectionsCreated.Add(1)
			trt.stats.TCPConnections.Add(1)
		},

		// ConnectDone is called when a TCP connection is established
		ConnectDone: func(network, addr string, err error) {
			if err != nil {
				// Connection failed
				trt.stats.ConnectionsCreated.Add(-1)
			}
		},

		// DNSStart is called when a DNS lookup starts
		DNSStart: func(dnsInfo httptrace.DNSStartInfo) {
			trt.stats.DNSLookups.Add(1)
		},

		// TLSHandshakeStart is called when TLS handshake starts
		TLSHandshakeStart: func() {
			trt.stats.TLSHandshakes.Add(1)
		},
	}

	// Add the trace to the request context
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	// Execute the request
	return trt.inner.RoundTrip(req)
}

// Ensure TraceRoundTripper implements http.RoundTripper
var _ http.RoundTripper = (*TraceRoundTripper)(nil)
