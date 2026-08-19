package transport

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	cfg := DefaultConfig()
	transport := New(cfg)

	if transport == nil {
		t.Fatal("New() returned nil")
	}

	// Check pool settings
	if transport.MaxIdleConns != 200 {
		t.Errorf("Transport.MaxIdleConns = %d, want 200", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 50 {
		t.Errorf("Transport.MaxIdleConnsPerHost = %d, want 50", transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("Transport.IdleConnTimeout = %v, want 90s", transport.IdleConnTimeout)
	}

	// Check TLS settings
	if transport.TLSClientConfig == nil {
		t.Error("Transport.TLSClientConfig is nil")
	}
	// TLS 1.2 = 0x0303 = 771
	if transport.TLSClientConfig.MinVersion != 771 {
		t.Errorf("Transport.TLSClientConfig.MinVersion = %d, want 771 (TLS 1.2)", transport.TLSClientConfig.MinVersion)
	}

	// Check HTTP/2
	if !transport.ForceAttemptHTTP2 {
		t.Error("Transport.ForceAttemptHTTP2 = false, want true")
	}

	// Check compression disabled
	if !transport.DisableCompression {
		t.Error("Transport.DisableCompression = false, want true")
	}
}

func TestNewWithZeroValues(t *testing.T) {
	cfg := Config{} // All zero values
	transport := New(cfg)

	if transport == nil {
		t.Fatal("New() returned nil with zero config")
	}

	// Should use defaults
	if transport.MaxIdleConns != 200 {
		t.Errorf("Transport.MaxIdleConns = %d, want 200 (default)", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 50 {
		t.Errorf("Transport.MaxIdleConnsPerHost = %d, want 50 (default)", transport.MaxIdleConnsPerHost)
	}
}

func TestNewWithCustomValues(t *testing.T) {
	cfg := Config{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     2 * time.Minute,
		KeepAlive:           60 * time.Second,
		DialTimeout:         5 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		ForceHTTP2:          false,
		InsecureSkipVerify:  true,
	}

	transport := New(cfg)

	if transport.MaxIdleConns != 1000 {
		t.Errorf("Transport.MaxIdleConns = %d, want 1000", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 200 {
		t.Errorf("Transport.MaxIdleConnsPerHost = %d, want 200", transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 2*time.Minute {
		t.Errorf("Transport.IdleConnTimeout = %v, want 2m", transport.IdleConnTimeout)
	}
	if transport.ForceAttemptHTTP2 {
		t.Error("Transport.ForceAttemptHTTP2 = true, want false")
	}
	if !transport.TLSClientConfig.InsecureSkipVerify {
		t.Error("Transport.TLSClientConfig.InsecureSkipVerify = false, want true")
	}
}

func TestNewClient(t *testing.T) {
	cfg := DefaultConfig()
	timeout := 30 * time.Second

	client := NewClient(cfg, timeout)

	if client == nil {
		t.Fatal("NewClient() returned nil")
	}
	if client.Timeout != timeout {
		t.Errorf("Client.Timeout = %v, want %v", client.Timeout, timeout)
	}
	if client.Transport == nil {
		t.Error("Client.Transport is nil")
	}
}
