package transport

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxIdleConns != 200 {
		t.Errorf("DefaultConfig.MaxIdleConns = %d, want 200", cfg.MaxIdleConns)
	}
	if cfg.MaxIdleConnsPerHost != 50 {
		t.Errorf("DefaultConfig.MaxIdleConnsPerHost = %d, want 50", cfg.MaxIdleConnsPerHost)
	}
	if cfg.IdleConnTimeout != 90*time.Second {
		t.Errorf("DefaultConfig.IdleConnTimeout = %v, want 90s", cfg.IdleConnTimeout)
	}
	if cfg.KeepAlive != 30*time.Second {
		t.Errorf("DefaultConfig.KeepAlive = %v, want 30s", cfg.KeepAlive)
	}
	if !cfg.ForceHTTP2 {
		t.Error("DefaultConfig.ForceHTTP2 = false, want true")
	}
	if cfg.InsecureSkipVerify {
		t.Error("DefaultConfig.InsecureSkipVerify = true, want false")
	}
}

func TestHighPerformanceConfig(t *testing.T) {
	cfg := HighPerformanceConfig()

	if cfg.MaxIdleConns != 500 {
		t.Errorf("HighPerformanceConfig.MaxIdleConns = %d, want 500", cfg.MaxIdleConns)
	}
	if cfg.MaxIdleConnsPerHost != 100 {
		t.Errorf("HighPerformanceConfig.MaxIdleConnsPerHost = %d, want 100", cfg.MaxIdleConnsPerHost)
	}
}

func TestLowResourceConfig(t *testing.T) {
	cfg := LowResourceConfig()

	if cfg.MaxIdleConns != 50 {
		t.Errorf("LowResourceConfig.MaxIdleConns = %d, want 50", cfg.MaxIdleConns)
	}
	if cfg.MaxIdleConnsPerHost != 10 {
		t.Errorf("LowResourceConfig.MaxIdleConnsPerHost = %d, want 10", cfg.MaxIdleConnsPerHost)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     DefaultConfig(),
			wantErr: false,
		},
		{
			name: "negative MaxIdleConns",
			cfg: Config{
				MaxIdleConns:        -1,
				MaxIdleConnsPerHost: 10,
			},
			wantErr: true,
		},
		{
			name: "negative MaxIdleConnsPerHost",
			cfg: Config{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: -1,
			},
			wantErr: true,
		},
		{
			name: "negative IdleConnTimeout",
			cfg: Config{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     -1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "negative KeepAlive",
			cfg: Config{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				KeepAlive:           -1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "negative DialTimeout",
			cfg: Config{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				DialTimeout:         -1 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
