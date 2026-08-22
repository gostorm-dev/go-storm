package storm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- Validation rules (design §8.1) ---

func TestValidate_DurationAndCountExclusive(t *testing.T) {
	cfg := Config{URL: "http://x", TotalReqs: 100, Duration: 30 * time.Second, Concurrency: 1}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when both -n and -d are set")
	}
	want := "--requests (-n) and --duration (-d) are mutually exclusive"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q must name both flags (%q)", err.Error(), want)
	}
}

func TestValidate_NeitherSet(t *testing.T) {
	cfg := Config{URL: "http://x", Concurrency: 1}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when neither -n nor -d is set")
	}
	want := "no workload defined: set --requests (-n) OR --duration (-d)"
	if err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

func TestValidate_CountOnlyUnchanged(t *testing.T) {
	cfg := Config{URL: "http://x", TotalReqs: 100, Concurrency: 4}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("legacy count-only config must stay valid, got %v", err)
	}
}

func TestValidate_DurationTooShort(t *testing.T) {
	cfg := Config{URL: "http://x", Duration: 500 * time.Millisecond, Concurrency: 1}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for sub-second duration")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_NegativeDuration(t *testing.T) {
	cfg := Config{URL: "http://x", Duration: -5 * time.Second, Concurrency: 1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative duration")
	}
}

// --- Scheduler behavior (design §8.1) ---

// TestScheduler_DurationStopsProduction proves the producer stops creating
// jobs at the deadline even while a consumer drains continuously.
func TestScheduler_DurationStopsProduction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lt := &LoadTester{
		config: Config{URL: srv.URL, Method: "GET", Duration: 150 * time.Millisecond, Concurrency: 1},
		jobs:   make(chan Job, 4),
		ctx:    context.Background(),
	}

	start := time.Now()
	go lt.produceJobs()

	count := 0
	for range lt.jobs {
		count++
	}
	elapsed := time.Since(start)

	if count == 0 {
		t.Fatal("producer dispatched no jobs")
	}
	if elapsed < 140*time.Millisecond {
		t.Fatalf("producer closed jobs after %v — deadline ignored", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("producer ran %v — far past deadline", elapsed)
	}
}

// TestScheduler_JobIDsIncrementAcrossModes checks ID continuity in both
// termination modes (count mode must be exactly 1..N as before).
func TestScheduler_JobIDsIncrementAcrossModes(t *testing.T) {
	collect := func(lt *LoadTester) []int64 {
		var ids []int64
		go lt.produceJobs()
		for j := range lt.jobs {
			ids = append(ids, j.ID)
		}
		return ids
	}

	t.Run("count mode", func(t *testing.T) {
		lt := &LoadTester{
			config: Config{URL: "http://x", TotalReqs: 10, Concurrency: 1},
			jobs:   make(chan Job, 16),
			ctx:    context.Background(),
		}
		ids := collect(lt)
		if len(ids) != 10 {
			t.Fatalf("want 10 jobs, got %d", len(ids))
		}
		for i, id := range ids {
			if id != int64(i+1) {
				t.Fatalf("ID at %d = %d, want %d", i, id, i+1)
			}
		}
	})

	t.Run("duration mode", func(t *testing.T) {
		lt := &LoadTester{
			config: Config{URL: "http://x", Duration: 100 * time.Millisecond, Concurrency: 1},
			jobs:   make(chan Job, 1024),
			ctx:    context.Background(),
		}
		ids := collect(lt)
		if len(ids) < 2 {
			t.Fatalf("expected multiple jobs in window, got %d", len(ids))
		}
		if ids[0] != 1 {
			t.Fatalf("first ID = %d, want 1", ids[0])
		}
		for i := 1; i < len(ids); i++ {
			if ids[i] != ids[i-1]+1 {
				t.Fatalf("IDs not contiguous: %v -> %v", ids[i-1], ids[i])
			}
		}
	})
}

// --- Integration tests (design §8.2) ---

func fastServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
}

func TestDurationMode_RunCompletes(t *testing.T) {
	srv := fastServer()
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    time.Second,
		Concurrency: 5,
		Timeout:     2 * time.Second,
	}
	tester := NewLoadTester(context.Background(), cfg)

	start := time.Now()
	stats, err := tester.Run()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if stats.TotalRequests == 0 {
		t.Fatal("no requests completed in duration mode")
	}
	if stats.Failed != 0 {
		t.Fatalf("unexpected failures: %d", stats.Failed)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("run finished too early: %v", elapsed)
	}
	// Graceful drain tail bounded by request timeout; generous CI margin.
	if elapsed > 3*time.Second {
		t.Fatalf("run overran badly: %v", elapsed)
	}
}

func TestDurationMode_WithRateLimit(t *testing.T) {
	srv := fastServer()
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    2 * time.Second,
		Concurrency: 20,
		Rate:        50,
		Timeout:     2 * time.Second,
	}
	tester := NewLoadTester(context.Background(), cfg)

	stats, err := tester.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Limiter starts with a full burst bucket (burst = Rate), then sustains
	// Rate/s: 50 burst + 50/s × 2s = ~150 total; wide bounds so scheduler
	// jitter on loaded machines does not flake.
	if stats.TotalRequests < 120 || stats.TotalRequests > 180 {
		t.Fatalf("rate+duration produced %d requests, want ~150 (120..180)", stats.TotalRequests)
	}
}

func TestDurationMode_CancellationMidRun(t *testing.T) {
	srv := fastServer()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		URL:         srv.URL,
		Duration:    5 * time.Second,
		Concurrency: 4,
		Timeout:     2 * time.Second,
	}
	tester := NewLoadTester(ctx, cfg)

	type runOut struct {
		stats Stats
		err   error
	}
	outCh := make(chan runOut, 1)
	go func() {
		stats, err := tester.Run()
		outCh <- runOut{stats, err}
	}()

	time.Sleep(400 * time.Millisecond)
	cancel()

	select {
	case out := <-outCh:
		if out.err != nil {
			t.Fatalf("cancellation should return partial stats cleanly, got error: %v", out.err)
		}
		if out.stats.TotalRequests == 0 {
			t.Fatal("partial stats empty after mid-run cancellation")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not return after cancellation — goroutine hang")
	}
}

// TestDurationMode_GracefulDrain proves no synthetic errors appear when the
// deadline hits with requests still in flight (graceful drain semantics).
func TestDurationMode_GracefulDrain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    time.Second,
		Concurrency: 10,
		Timeout:     5 * time.Second,
	}
	tester := NewLoadTester(context.Background(), cfg)

	stats, err := tester.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if stats.TotalRequests == 0 {
		t.Fatal("no requests recorded")
	}
	if stats.Failed != 0 {
		t.Fatalf("drain injected failures: %d failed of %d — graceful drain broken",
			stats.Failed, stats.TotalRequests)
	}
}
