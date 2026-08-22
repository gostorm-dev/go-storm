package storm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestSaturationKillTerminatesRunEarly drives the watchdog with an
// artificially low CPU threshold so sustained critical saturation is
// guaranteed, and verifies the run ends early with a recorded kill reason
// instead of completing the requested workload.
func TestSaturationKillTerminatesRunEarly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   100000,
		Concurrency: 4,
		Timeout:     5 * time.Second,
	}

	tester := NewLoadTester(context.Background(), cfg)
	tester.EnableSaturationKill()

	th := DefaultThresholds()
	th.CPUWarn = 0.0000005
	th.CPUCritical = 0.000001 // any real CPU activity breaches this
	tester.SetThresholds(th)

	// Tighten watchdog cadence so the test stays fast.
	tester.monitorInterval = 25 * time.Millisecond
	tester.killCheckInterval = 25 * time.Millisecond
	tester.killStreakRequired = 2

	start := time.Now()
	stats, err := tester.Run()
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	elapsed := time.Since(start)

	if !stats.KilledOnSaturation {
		t.Fatalf("expected run to be killed on saturation, but it completed normally (completed=%d)", stats.TotalRequests)
	}
	if stats.KillReason == "" {
		t.Fatal("kill reason is empty")
	}
	if stats.KilledAtMS <= 0 {
		t.Fatalf("killed_at_ms should be positive, got %v", stats.KilledAtMS)
	}
	if stats.TotalRequests >= cfg.TotalReqs {
		t.Fatalf("run should have ended early: completed %d of %d requests",
			stats.TotalRequests, cfg.TotalReqs)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("kill took too long: %v — watchdog did not fire in time", elapsed)
	}

	hr := tester.GetHealthReport()
	if hr == nil {
		t.Fatal("health report missing after killed run")
	}
	if !hr.KilledOnSaturation {
		t.Error("health report should carry KilledOnSaturation")
	}
	if hr.Level != CRITICAL {
		t.Errorf("health report level = %v, want CRITICAL after kill", hr.Level)
	}

	// The threshold override must survive into the diagnosis signals —
	// guards the SetThresholds repair in Run().
	foundCPU := false
	for _, s := range hr.Signals {
		if s.Factor == "CPU" {
			foundCPU = true
			if s.Threshold > 0.001 {
				t.Errorf("CPU signal threshold = %v, custom override was not applied", s.Threshold)
			}
		}
	}
	if !foundCPU {
		t.Error("health report missing CPU signal")
	}
}

// TestNoKillWhenHealthy verifies kill mode does not interfere with a normal,
// healthy run: every request completes and no kill event is recorded.
func TestNoKillWhenHealthy(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const total = 50
	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   total,
		Concurrency: 4,
		Timeout:     5 * time.Second,
	}

	tester := NewLoadTester(context.Background(), cfg)
	tester.EnableSaturationKill() // default thresholds must never trip here

	stats, err := tester.Run()
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if stats.KilledOnSaturation {
		t.Fatalf("healthy run was killed: %s", stats.KillReason)
	}
	if stats.TotalRequests != total {
		t.Errorf("completed %d of %d requests — workload was cut short", stats.TotalRequests, total)
	}
	if hits.Load() != total {
		t.Errorf("server saw %d hits, want %d", hits.Load(), total)
	}
	if tester.GetHealthReport() == nil {
		t.Error("health report should exist when saturation monitoring is enabled")
	}
}

// TestCriticalResourceSignalsIgnoresWorkerAndRPS pins down the kill-scope
// rule: worker utilization and RPS achievement must never terminate a run,
// because they conflate a slow target or demand-fed pipeline with generator
// exhaustion.
func TestCriticalResourceSignalsIgnoresWorkerAndRPS(t *testing.T) {
	diag := Diagnosis{
		Level: CRITICAL,
		Signals: []Signal{
			{Factor: "Worker Utilization", Severity: CRITICAL, Message: "99.5%"},
			{Factor: "RPS Achievement", Severity: CRITICAL, Message: "500 / 1000"},
			{Factor: "CPU", Severity: OK, Message: "42.1%"},
		},
	}
	if got := criticalResourceSignals(diag); got != "" {
		t.Errorf("non-resource CRITICAL signals triggered kill reason %q", got)
	}

	diag.Signals = append(diag.Signals, Signal{
		Factor: "GC Pause", Severity: CRITICAL, Message: "512.0 ms",
	})
	if got := criticalResourceSignals(diag); got != "512.0 ms" {
		t.Errorf("resource CRITICAL signal not detected, got %q", got)
	}
}
