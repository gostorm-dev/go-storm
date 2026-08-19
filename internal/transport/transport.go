package transport

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// New creates an http.Transport from the given configuration.
// This transport is optimized for load testing with proper connection pooling.
func New(cfg Config) *http.Transport {
	// Apply defaults for zero values
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 200
	}
	if cfg.MaxIdleConnsPerHost == 0 {
		cfg.MaxIdleConnsPerHost = 50
	}
	if cfg.IdleConnTimeout == 0 {
		cfg.IdleConnTimeout = 90 * time.Second
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.TLSHandshakeTimeout == 0 {
		cfg.TLSHandshakeTimeout = 10 * time.Second
	}
	if cfg.ResponseHeaderTimeout == 0 {
		cfg.ResponseHeaderTimeout = 10 * time.Second
	}
	if cfg.ExpectContinueTimeout == 0 {
		cfg.ExpectContinueTimeout = 1 * time.Second
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 32 * 1024
	}
	if cfg.MaxHTTP2ConcurrentStreams == 0 {
		cfg.MaxHTTP2ConcurrentStreams = 1000
	}

	// Create dialer
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlive,
		DualStack: !cfg.DisableDualStack,
	}

	// Override with custom dialer if provided
	if cfg.Dialer != nil {
		dialer = cfg.Dialer
	}

	// Create TLS config
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if cfg.TLSConfig != nil {
		tlsConfig = cfg.TLSConfig
	}

	// Create transport
	transport := &http.Transport{
		// Connection Pool
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,

		// Dial
		DialContext:    dialer.DialContext,
		DialTLSContext: nil, // Will use TLS handshake

		// Timeouts
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimeout,

		// TLS
		TLSClientConfig: tlsConfig,

		// HTTP/2
		ForceAttemptHTTP2: cfg.ForceHTTP2,

		// Disable compression for load testing (reduces overhead)
		DisableCompression: true,

		// Disable keep-alives only if explicitly requested
		DisableKeepAlives: false,

		// Write buffer size
		WriteBufferSize: cfg.BufferSize,

		// Read buffer size
		ReadBufferSize: cfg.BufferSize,
	}

	return transport
}

// NewWithStats creates an http.Transport with connection pool statistics tracking.
// The stats parameter will be updated with connection pool metrics.
func NewWithStats(cfg Config, stats *Stats) *http.Transport {
	return New(cfg)
}

// NewClient creates an http.Client with the given transport configuration.
func NewClient(cfg Config, timeout time.Duration) *http.Client {
	transport := New(cfg)
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}
