package storm

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// checkRelErr fails the test when got deviates from the true value by more
// than the guaranteed histogram error bound.
func checkRelErr(t *testing.T, name string, got, want time.Duration) {
	t.Helper()
	if got <= 0 || want <= 0 {
		t.Fatalf("%s: got %v, want %v — both must be positive", name, got, want)
	}
	g := float64(got) / float64(time.Millisecond)
	w := float64(want) / float64(time.Millisecond)
	if rel := math.Abs(g-w) / w; rel > histRelErr+1e-9 {
		t.Errorf("%s = %.4fms, true %.4fms — relative error %.3f%% exceeds bound %.3f%%",
			name, g, w, rel*100, histRelErr*100)
	}
}

// TestLogHistogramMatchesExactSort is the core guarantee proof: over many
// trials of mixed latency distributions (fast local, typical network, slow
// tail, bimodal spikes), every reported percentile must stay within
// histRelErr of the exact nearest-rank percentile of a full sort.
func TestLogHistogramMatchesExactSort(t *testing.T) {
	rng := rand.New(rand.NewSource(42)) // deterministic: failures reproducible

	for trial := 0; trial < 25; trial++ {
		n := 5000 + rng.Intn(20000)
		durations := make([]time.Duration, n)
		h := NewLogHistogram()

		for i := range durations {
			var d time.Duration
			switch rng.Intn(4) {
			case 0: // sub-millisecond local responses
				d = time.Duration(1+rng.Float64()*999) * time.Microsecond
			case 1: // typical network latencies
				d = time.Duration(1+rng.Float64()*199) * time.Millisecond
			case 2: // long tail
				d = time.Duration(100+rng.Float64()*4000) * time.Millisecond
			default: // bimodal spikes
				if rng.Float64() < 0.9 {
					d = time.Duration(5+rng.Float64()*10) * time.Millisecond
				} else {
					d = time.Duration(3000+rng.Float64()*15000) * time.Millisecond
				}
			}
			durations[i] = d
			h.Observe(d)
		}

		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

		for _, p := range []float64{50, 90, 95, 99, 99.9} {
			name := fmt.Sprintf("trial=%d n=%d p=%v", trial, n, p)
			checkRelErr(t, name, h.Percentile(p), percentile(durations, p))
		}
	}
}

// TestLogHistogramBucketIndexing verifies the IEEE-754 bit decomposition
// maps known values to their exact buckets: binade starts land on s=0,
// interior values split the binade linearly, out-of-range values clamp.
func TestLogHistogramBucketIndexing(t *testing.T) {
	cases := []struct {
		d       time.Duration
		wantIdx int
	}{
		{0, 0},                     // zero/negative clamp under-range
		{400 * time.Nanosecond, 0}, // 0.4µs — below the ~0.98µs floor
		{900 * time.Nanosecond, 0}, // still under the floor
		{10 * time.Millisecond, 13<<histSubBits | 32},   // 1.25 · 2^3 → s=32
		{16 * time.Millisecond, 14 << histSubBits},      // binade start → s=0
		{255 * time.Millisecond, 17<<histSubBits | 127}, // near top of [128,256)
		{35 * time.Minute, histBuckets},                 // beyond 34.9min ceiling → overflow
	}

	for _, tc := range cases {
		h := NewLogHistogram()
		h.Observe(tc.d)
		idx := -1
		for i, c := range h.counts {
			if c == 1 {
				if idx != -1 {
					t.Fatalf("d=%v landed in multiple buckets (%d, %d)", tc.d, idx, i)
				}
				idx = i
			}
		}
		if idx != tc.wantIdx {
			t.Errorf("d=%v indexed at %d, want %d", tc.d, idx, tc.wantIdx)
		}
		if h.total != 1 {
			t.Errorf("d=%v: total = %d, want 1", tc.d, h.total)
		}
	}
}

// TestLogHistogramEdges covers contract edges: empty reads, degenerate
// percent arguments, single-sample worst case, all-equal samples, and the
// overflow under-estimate.
func TestLogHistogramEdges(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		h := NewLogHistogram()
		if got := h.Percentile(50); got != 0 {
			t.Errorf("empty Percentile(50) = %v, want 0", got)
		}
	})

	t.Run("degenerate pct", func(t *testing.T) {
		h := NewLogHistogram()
		h.Observe(time.Millisecond)
		if got := h.Percentile(-5); got != 0 {
			t.Errorf("Percentile(-5) = %v, want 0", got)
		}
		if got := h.Percentile(150); got <= 0 {
			t.Errorf("Percentile(150) = %v, want clamped to max sample (>0)", got)
		}
	})

	t.Run("single value respects bound", func(t *testing.T) {
		h := NewLogHistogram()
		v := 10 * time.Millisecond
		h.Observe(v)
		checkRelErr(t, "single p99.9", h.Percentile(99.9), v)
	})

	t.Run("all-equal worst case", func(t *testing.T) {
		h := NewLogHistogram()
		v := 250 * time.Millisecond
		for i := 0; i < 1000; i++ {
			h.Observe(v)
		}
		// Every observation sits in one bucket — the interpolation worst
		// case — yet the bound must still hold for any percentile.
		for _, p := range []float64{1, 25, 50, 75, 95, 99, 99.9} {
			checkRelErr(t, fmt.Sprintf("all-equal p=%v", p), h.Percentile(p), v)
		}
	})

	t.Run("overflow reports ceiling", func(t *testing.T) {
		h := NewLogHistogram()
		h.Observe(time.Hour) // far beyond the 34.9 min covered ceiling
		want := time.Duration(math.Ldexp(1, histMaxExp) * float64(time.Millisecond))
		if got := h.Percentile(99); got != want {
			t.Errorf("overflow Percentile(99) = %v, want documented ceiling %v", got, want)
		}
	})
}

// TestLogHistogramMerge proves mergeability: two independently filled
// histograms must produce the same percentiles as one histogram fed every
// sample — the property distributed aggregation will rely on.
func TestLogHistogramMerge(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	var all []time.Duration
	a, b := NewLogHistogram(), NewLogHistogram()

	add := func(h *LogHistogram, n int) {
		for i := 0; i < n; i++ {
			d := time.Duration(1+rng.Float64()*5000) * time.Millisecond
			all = append(all, d)
			h.Observe(d)
		}
	}
	add(a, 8000)
	add(b, 12000)

	merged := NewLogHistogram()
	merged.Merge(a)
	merged.Merge(b)

	if merged.total != len(all) {
		t.Fatalf("merged.total = %d, want %d", merged.total, len(all))
	}

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for _, p := range []float64{50, 90, 95, 99, 99.9} {
		checkRelErr(t, fmt.Sprintf("merged p=%v", p),
			merged.Percentile(p), percentile(all, p))
	}
}

// TestCollectorExposesNewPercentiles guards the Stats wiring end-to-end:
// P90/P999 flow from the streaming collector, not just P50/P95/P99.
func TestCollectorExposesNewPercentiles(t *testing.T) {
	c := NewCollector()
	for i := 1; i <= 100; i++ {
		c.Add(Result{StatusCode: 200, Duration: time.Duration(i) * time.Millisecond})
	}
	s := c.Stats()
	if s.P90 <= 0 || s.P90 > s.P95 {
		t.Errorf("P90 = %v, want in (0, P95=%v]", s.P90, s.P95)
	}
	if s.P999 < s.P99 {
		t.Errorf("P999 = %v, want >= P99=%v", s.P999, s.P99)
	}
	checkRelErr(t, "collector p90", s.P90, 91*time.Millisecond) // nearest-rank of 1..100ms
}
