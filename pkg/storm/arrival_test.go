package storm

import (
	"context"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// refFloor returns floor(num/den) via exact rational arithmetic.
func refFloor(num, den int64) *big.Int {
	return new(big.Int).Quo(big.NewInt(num), big.NewInt(den))
}

// TestArrivalMathMatchesBigRat cross-checks the 128-bit scheduler math
// against arbitrary-precision arithmetic across extreme inputs.
func TestArrivalMathMatchesBigRat(t *testing.T) {
	rates := []uint64{1, 2, 7, 999, 5000, 1_000_000, 12_345_678}
	elapseds := []int64{0, 1, 999, 1_000_000, nsPerSec - 1, nsPerSec, 30 * nsPerSec}

	for _, rate := range rates {
		for _, e := range elapseds {
			want := refFloor(e*int64(rate), nsPerSec)
			if want.Sign() < 0 || !want.IsInt64() {
				continue
			}
			if got := dueIndex(e, rate); got != want.Int64() {
				t.Errorf("dueIndex(%d, %d) = %d, want %d", e, rate, got, want.Int64())
			}
		}

		for _, j := range elapseds {
			wantNum := new(big.Int).Mul(big.NewInt(j), big.NewInt(nsPerSec))
			wantDen := big.NewInt(int64(rate))
			want := new(big.Int).Quo(wantNum, wantDen)
			if !want.IsInt64() {
				continue // beyond representable horizon — clamp path
			}
			if got := slotOffsetNS(j, rate); got != want.Int64() {
				t.Errorf("slotOffsetNS(%d, %d) = %d, want %d", j, rate, got, want.Int64())
			}
		}
	}
}

// TestSlotOffsetClampsBeyondHorizon guards the overflow clamp: a departure
// past int64 nanoseconds must saturate, not wrap.
func TestSlotOffsetClampsBeyondHorizon(t *testing.T) {
	if got := slotOffsetNS(1<<40, 1); got != math.MaxInt64 {
		t.Errorf("slotOffsetNS(2^40, 1) = %d, want MaxInt64 clamp", got)
	}
}

func TestArrivalLimitMatrix(t *testing.T) {
	cases := []struct {
		dur  time.Duration
		rate uint64
		want int64
	}{
		{30 * time.Second, 5000, 150000}, // THE reported bug scenario
		{time.Second, 2000, 2000},
		{time.Second, 7, 7},
		{250 * time.Millisecond, 10000, 2500},
		{100 * time.Millisecond, 5, 1}, // only slot j=0 fits inside 100ms
		{2 * time.Second, 3000, 6000},
		{0, 5000, 0},
		{time.Second, 0, 0},
	}
	for _, tc := range cases {
		if got := arrivalLimit(tc.dur, tc.rate); got != tc.want {
			t.Errorf("arrivalLimit(%v, %d) = %d, want %d", tc.dur, tc.rate, got, tc.want)
		}
	}
}

func TestTelemetryClassifiesLateSlots(t *testing.T) {
	tel := newArrivalTelemetry(200) // interval = 5ms, graded = 5ms
	tel.record(1 * time.Millisecond)
	tel.record(4 * time.Millisecond)
	tel.record(20 * time.Millisecond)

	snap := tel.snapshot()
	if snap == nil {
		t.Fatal("snapshot = nil, want summary")
	}
	if snap.Sent != 3 || snap.Late != 1 {
		t.Errorf("Sent=%d Late=%d, want 3/1", snap.Sent, snap.Late)
	}
	if snap.AccuracyPct > 66.67 || snap.AccuracyPct < 66.66 {
		t.Errorf("AccuracyPct = %.2f, want ~66.67", snap.AccuracyPct)
	}
	if snap.MaxLagMS < 19 || snap.MaxLagMS > 21 {
		t.Errorf("MaxLagMS = %.2f, want ~20", snap.MaxLagMS)
	}
}

// TestTelemetryHighRateGrading guards the timer-granularity floor: above
// 1000 RPS the slot interval shrinks below OS timer quantum, so sub-ms
// jitter must not be graded as lateness — only genuine multi-ms lag.
func TestTelemetryHighRateGrading(t *testing.T) {
	tel := newArrivalTelemetry(5000)   // interval = 0.2ms, graded floor = 1ms
	tel.record(300 * time.Microsecond) // within slot interval? no — within graded floor: on-time
	tel.record(900 * time.Microsecond) // > interval but < 1ms floor
	tel.record(2 * time.Millisecond)   // genuine lateness

	snap := tel.snapshot()
	if snap.Sent != 3 {
		t.Fatalf("Sent = %d, want 3", snap.Sent)
	}
	if snap.Late != 1 {
		t.Errorf("Late = %d, want 1 (only the 2ms dispatch)", snap.Late)
	}
	if snap.AccuracyPct < 66.66 || snap.AccuracyPct > 66.67 {
		t.Errorf("AccuracyPct = %.2f, want ~66.67", snap.AccuracyPct)
	}
	if snap.GradedMS != 1.0 {
		t.Errorf("GradedMS = %.2f, want 1.00 (timer-granularity floor)", snap.GradedMS)
	}
}

// arrivalTestServer spins up an httptest server answering immediately.
func arrivalTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

// TestScheduledDurationExactCount is THE regression test for the rate
// overshoot bug: -r 2000 -d 1s used to send ~4000 (burst + pacing); the
// virtual-clock scheduler must send exactly 2000.
func TestScheduledDurationExactCount(t *testing.T) {
	srv := arrivalTestServer()
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    time.Second,
		Rate:        2000,
		Concurrency: 50,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.TotalRequests != 2000 {
		t.Errorf("TotalRequests = %d, want EXACTLY 2000", stats.TotalRequests)
	}
	if stats.Failed != 0 {
		t.Errorf("Failed = %d, want 0", stats.Failed)
	}
	if stats.Arrival == nil {
		t.Fatal("stats.Arrival = nil, want telemetry for rate-limited run")
	}
	if stats.Arrival.Sent != 2000 {
		t.Errorf("Arrival.Sent = %d, want 2000", stats.Arrival.Sent)
	}
}

// TestScheduledCountModeExactAndPaced verifies -n N -r R dispatches exactly
// N requests paced over N/R seconds.
func TestScheduledCountModeExactAndPaced(t *testing.T) {
	srv := arrivalTestServer()
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   300,
		Rate:        300,
		Concurrency: 50,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	start := time.Now()
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.TotalRequests != 300 {
		t.Errorf("TotalRequests = %d, want exactly 300", stats.TotalRequests)
	}
	// Last slot departs at 299/300 s; allow generous slack but catch an
	// unpaced or burst run finishing near-instantly.
	if elapsed < 900*time.Millisecond {
		t.Errorf("paced run finished in %v, want >= 900ms", elapsed)
	}
}

// TestNoStartupBurst proves the old t≈0 stampede is gone: in the first
// 500ms of a 200 RPS run at most the scheduled ~100 arrivals (+slack) may
// appear. The token-bucket limiter sent its entire 200-request burst here.
func TestNoStartupBurst(t *testing.T) {
	var mu sync.Mutex
	var stamps []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		stamps = append(stamps, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    time.Second,
		Rate:        200,
		Concurrency: 20,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.TotalRequests != 200 {
		t.Fatalf("TotalRequests = %d, want 200", stats.TotalRequests)
	}

	mu.Lock()
	defer mu.Unlock()
	first := stamps[0]
	cut := first.Add(500 * time.Millisecond)
	inWindow := 0
	for _, ts := range stamps {
		if ts.Before(cut) {
			inWindow++
		}
	}
	// Schedule allows exactly 100 arrivals in 500ms; timer jitter under CI
	// load gets modest slack. The old limiter sent all 200.
	if inWindow > 130 {
		t.Errorf("%d arrivals within first 500ms, want <= ~100 scheduled (startup burst regression)", inWindow)
	}
}

// TestSlowConsumerLagDetected verifies saturation honesty: when workers
// cannot keep up with the requested rate, the lag is measured and reported,
// not hidden.
func TestSlowConsumerLagDetected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond) // capacity ≈ 100/s per worker
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    time.Second,
		Rate:        200, // demands 2x what one worker can absorb
		Concurrency: 1,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	a := stats.Arrival
	if a == nil {
		t.Fatal("stats.Arrival = nil, want telemetry")
	}
	if a.Sent != 200 {
		t.Errorf("Sent = %d, want exactly 200 (limit holds even when behind)", a.Sent)
	}
	if a.Late == 0 {
		t.Error("Late = 0, want > 0 when generator falls behind its own schedule")
	}
	if a.AccuracyPct >= 90 {
		t.Errorf("AccuracyPct = %.2f, want < 90 when worker capacity is half the target", a.AccuracyPct)
	}
}
