package storm

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseHeaderSpec(t *testing.T) {
	testCases := []struct {
		name    string
		spec    string
		wantKey string
		wantVal string
		wantErr bool
	}{
		{name: "simple", spec: "X-Trace: loadtest", wantKey: "X-Trace", wantVal: "loadtest"},
		{name: "extra spaces", spec: "  X-Key :   value  ", wantKey: "X-Key", wantVal: "value"},
		{name: "colon in value", spec: "X-URL: https://example.com:8080", wantKey: "X-URL", wantVal: "https://example.com:8080"},
		{name: "empty value allowed", spec: "X-Empty:", wantKey: "X-Empty", wantVal: ""},
		{name: "no colon", spec: "BrokenHeader", wantErr: true},
		{name: "empty name", spec: ": value", wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key, val, err := ParseHeaderSpec(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseHeaderSpec(%q) expected error, got key=%q val=%q", tc.spec, key, val)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseHeaderSpec(%q) unexpected error: %v", tc.spec, err)
			}
			if key != tc.wantKey || val != tc.wantVal {
				t.Errorf("ParseHeaderSpec(%q) = (%q, %q), want (%q, %q)",
					tc.spec, key, val, tc.wantKey, tc.wantVal)
			}
		})
	}
}

// captureServer records the headers of the first request it receives.
type captureServer struct {
	url      string
	got      http.Header
	gotHost  string
	received atomic.Bool
	done     chan struct{}
}

func newCaptureServer(t *testing.T) *captureServer {
	t.Helper()
	cap := &captureServer{done: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cap.received.CompareAndSwap(false, true) {
			cap.got = r.Header.Clone()
			cap.gotHost = r.Host
			close(cap.done)
		}
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cap.url = srv.URL
	return cap
}

func (c *captureServer) gotHostURL() string { return c.url }

func waitCaptured(t *testing.T, cap *captureServer) {
	t.Helper()
	select {
	case <-cap.done:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received a request")
	}
}

func TestCustomHeadersSent(t *testing.T) {
	cap := newCaptureServer(t)

	cfg := Config{
		URL:         cap.gotHostURL(),
		TotalReqs:   1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "GET",
		Headers: http.Header{
			"Authorization": {"Bearer token123"},
			"X-Custom":      {"abc"},
		},
	}

	if _, err := NewLoadTester(context.Background(), cfg).Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitCaptured(t, cap)

	if got := cap.got.Get("Authorization"); got != "Bearer token123" {
		t.Errorf("Authorization = %q, want %q", got, "Bearer token123")
	}
	if got := cap.got.Get("X-Custom"); got != "abc" {
		t.Errorf("X-Custom = %q, want %q", got, "abc")
	}
}

func TestContentTypeOverride(t *testing.T) {
	cap := newCaptureServer(t)

	cfg := Config{
		URL:         cap.gotHostURL(),
		TotalReqs:   1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "POST",
		Payload:     []byte(`{"x":1}`),
		Headers: http.Header{
			"Content-Type": {"text/plain"},
		},
	}

	if _, err := NewLoadTester(context.Background(), cfg).Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitCaptured(t, cap)

	if got := cap.got.Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q, want user-supplied %q", got, "text/plain")
	}
}

func TestDefaultContentTypeBackwardCompat(t *testing.T) {
	cap := newCaptureServer(t)

	cfg := Config{
		URL:         cap.gotHostURL(),
		TotalReqs:   1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "POST",
		Payload:     []byte(`{"x":1}`),
		// No Headers set — must behave exactly as before the feature.
	}

	if _, err := NewLoadTester(context.Background(), cfg).Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitCaptured(t, cap)

	if got := cap.got.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want default %q", got, "application/json")
	}
}

func TestRepeatedHeadersAccumulate(t *testing.T) {
	cap := newCaptureServer(t)

	cfg := Config{
		URL:         cap.gotHostURL(),
		TotalReqs:   1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "GET",
		Headers: http.Header{
			"X-Multi": {"one", "two"},
		},
	}

	if _, err := NewLoadTester(context.Background(), cfg).Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitCaptured(t, cap)

	got := cap.got.Values("X-Multi")
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("X-Multi values = %v, want [one two]", got)
	}
}

func TestHostHeaderSpecialCase(t *testing.T) {
	cap := newCaptureServer(t)

	cfg := Config{
		URL:         cap.gotHostURL(),
		TotalReqs:   1,
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "GET",
		Headers: http.Header{
			"Host": {"virtual.example.com"},
		},
	}

	if _, err := NewLoadTester(context.Background(), cfg).Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitCaptured(t, cap)

	if cap.gotHost != "virtual.example.com" {
		t.Errorf("wire Host = %q, want %q", cap.gotHost, "virtual.example.com")
	}
}
