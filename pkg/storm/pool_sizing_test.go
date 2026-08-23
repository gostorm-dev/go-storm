package storm

import (
	"testing"

	"github.com/gostorm-dev/go-storm/internal/transport"
)

func TestSizeConnectionPool(t *testing.T) {
	t.Run("nil config is a no-op", func(t *testing.T) {
		sizeConnectionPool(nil, 100)
	})

	t.Run("zero concurrency is a no-op", func(t *testing.T) {
		tc := transport.DefaultConfig()
		before := tc.MaxIdleConnsPerHost
		sizeConnectionPool(&tc, 0)
		if tc.MaxIdleConnsPerHost != before {
			t.Fatalf("per-host changed: %d -> %d", before, tc.MaxIdleConnsPerHost)
		}
	})

	t.Run("raises undersized pool to 2x concurrency with floor", func(t *testing.T) {
		tc := transport.Config{MaxIdleConns: 200, MaxIdleConnsPerHost: 50}
		sizeConnectionPool(&tc, 100)
		if tc.MaxIdleConnsPerHost != 256 || tc.MaxIdleConns < 256 {
			t.Fatalf("pool not sized with headroom: total=%d perHost=%d",
				tc.MaxIdleConns, tc.MaxIdleConnsPerHost)
		}

		tc2 := transport.Config{}
		sizeConnectionPool(&tc2, 500)
		if tc2.MaxIdleConnsPerHost != 1000 {
			t.Fatalf("high-concurrency sizing wrong: perHost=%d, want 1000", tc2.MaxIdleConnsPerHost)
		}
	})

	t.Run("never lowers explicit larger values", func(t *testing.T) {
		tc := transport.Config{MaxIdleConns: 500, MaxIdleConnsPerHost: 300}
		sizeConnectionPool(&tc, 100)
		if tc.MaxIdleConns != 500 || tc.MaxIdleConnsPerHost != 300 {
			t.Fatalf("explicit values lowered: total=%d perHost=%d",
				tc.MaxIdleConns, tc.MaxIdleConnsPerHost)
		}
	})
}

// NewLoadTester must resolve a pooled transport sized for the workload,
// even when the caller passes no TransportConfig.
func TestNewLoadTesterAutoSizesPool(t *testing.T) {
	lt := NewLoadTester(t.Context(), Config{
		URL:         "http://127.0.0.1:1/",
		Duration:    1 << 20, // far in the future; Run is never called
		Concurrency: 100,
	})
	defer lt.cancel()

	if lt.config.TransportConfig == nil {
		t.Fatal("TransportConfig left nil — engine would dial per request")
	}
	tc := lt.config.TransportConfig
	if tc.MaxIdleConnsPerHost < 2*100 && tc.MaxIdleConnsPerHost < minAutoIdlePerHost {
		t.Fatalf("per-host idle pool %d lacks headroom for concurrency 100", tc.MaxIdleConnsPerHost)
	}
	if tc.MaxIdleConns < 100 {
		t.Fatalf("total idle pool %d < concurrency 100", tc.MaxIdleConns)
	}
}
