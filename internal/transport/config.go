package transport

import (
	"crypto/tls"
	"net"
	"time"
)

// Config defines the HTTP transport configuration for connection pooling.
type Config struct {
	// Connection Pool Settings
	MaxIdleConns        int           // Total max idle connections across all hosts
	MaxIdleConnsPerHost int           // Max idle connections per target host
	IdleConnTimeout     time.Duration // How long idle connections remain in pool

	// Keep-Alive Settings
	KeepAlive          time.Duration // TCP keep-alive interval
	KeepAliveMaxProbes int           // Max TCP keep-alive probes before declaring dead

	// Timeout Settings
	DialTimeout           time.Duration // TCP dial timeout
	TLSHandshakeTimeout   time.Duration // TLS handshake timeout
	ResponseHeaderTimeout time.Duration // Timeout for receiving response headers
	ExpectContinueTimeout time.Duration // Timeout for 100 Continue expectation

	// TLS Settings
	TLSConfig          *tls.Config // Custom TLS configuration
	InsecureSkipVerify bool        // Skip TLS certificate verification

	// HTTP/2 Settings
	ForceHTTP2                bool // Force HTTP/2 even if server doesn't advertise
	MaxHTTP2ConcurrentStreams int  // Max concurrent HTTP/2 streams per connection

	// Buffer Pool Settings
	EnableBufferPool bool // Enable sync.Pool for request/response buffers
	BufferSize       int  // Initial buffer size in bytes

	// Dial Settings
	DisableDualStack bool        // Disable IPv6 dialing
	Dialer           *net.Dialer // Custom dialer (overrides DialTimeout if set)
}

// DefaultConfig returns the default transport configuration optimized for load testing.
func DefaultConfig() Config {
	return Config{
		// Connection Pool
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,

		// Keep-Alive
		KeepAlive:          30 * time.Second,
		KeepAliveMaxProbes: 3,

		// Timeouts
		DialTimeout:           10 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// TLS
		InsecureSkipVerify: false,

		// HTTP/2
		ForceHTTP2:                true,
		MaxHTTP2ConcurrentStreams: 1000,

		// Buffer Pool
		EnableBufferPool: true,
		BufferSize:       32 * 1024, // 32KB

		// Dial
		DisableDualStack: false,
	}
}

// HighPerformanceConfig returns a configuration optimized for maximum throughput.
func HighPerformanceConfig() Config {
	return Config{
		// Connection Pool - Aggressive settings
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     120 * time.Second,

		// Keep-Alive
		KeepAlive:          30 * time.Second,
		KeepAliveMaxProbes: 5,

		// Timeouts - Shorter for faster failure detection
		DialTimeout:           5 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// TLS
		InsecureSkipVerify: false,

		// HTTP/2
		ForceHTTP2:                true,
		MaxHTTP2ConcurrentStreams: 2000,

		// Buffer Pool
		EnableBufferPool: true,
		BufferSize:       64 * 1024, // 64KB

		// Dial
		DisableDualStack: false,
	}
}

// LowResourceConfig returns a configuration optimized for resource-constrained environments.
func LowResourceConfig() Config {
	return Config{
		// Connection Pool - Conservative settings
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     60 * time.Second,

		// Keep-Alive
		KeepAlive:          60 * time.Second,
		KeepAliveMaxProbes: 3,

		// Timeouts
		DialTimeout:           15 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,

		// TLS
		InsecureSkipVerify: false,

		// HTTP/2
		ForceHTTP2:                false, // Let server negotiate
		MaxHTTP2ConcurrentStreams: 100,

		// Buffer Pool
		EnableBufferPool: true,
		BufferSize:       16 * 1024, // 16KB

		// Dial
		DisableDualStack: true, // IPv4 only
	}
}

// Validate checks the configuration for invalid values.
func (c Config) Validate() error {
	if c.MaxIdleConns < 0 {
		return ErrNegativeMaxIdleConns
	}
	if c.MaxIdleConnsPerHost < 0 {
		return ErrNegativeMaxIdleConnsPerHost
	}
	if c.IdleConnTimeout < 0 {
		return ErrNegativeIdleConnTimeout
	}
	if c.KeepAlive < 0 {
		return ErrNegativeKeepAlive
	}
	if c.DialTimeout < 0 {
		return ErrNegativeDialTimeout
	}
	if c.TLSHandshakeTimeout < 0 {
		return ErrNegativeTLSHandshakeTimeout
	}
	if c.ResponseHeaderTimeout < 0 {
		return ErrNegativeResponseHeaderTimeout
	}
	if c.BufferSize < 0 {
		return ErrNegativeBufferSize
	}
	return nil
}
