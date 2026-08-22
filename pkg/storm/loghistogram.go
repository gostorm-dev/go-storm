package storm

import (
	"math"
	"time"
)

// The latency histogram partitions each power-of-two binade
// [2^e ms , 2^(e+1) ms) into histSub equal linear sub-intervals:
//
//	bucket (b, s) covers [ 2^(b-10) · (1 + s/histSub) ,
//	                      2^(b-10) · (1 + (s+1)/histSub) )  ms
//
// The widest ratio between consecutive boundaries occurs at s=0 and equals
// (histSub+1)/histSub, so ANY estimate taken from inside the correct bucket
// carries a guaranteed relative error ≤ 1/histSub (histRelErr) — independent
// of how latencies are distributed. No uniformity assumption can break this,
// because interpolation never leaves the bucket.
//
// Coverage: [2^-10 ms , 2^21 ms) ≈ [0.98 µs , 34.9 min).
//   - Values below the floor clamp into bucket 0 (<1µs absolute error).
//   - Values at/above the ceiling land in one overflow bucket; with the
//     default 10s timeout this is unreachable.
const (
	histSubBits int = 7                // 2^7 = 128 sub-intervals per binade
	histSub     int = 1 << histSubBits // 128
	histMinExp      = -10              // floor exponent, in ms
	histMaxExp      = 21               // exclusive ceiling exponent, in ms

	histBinades int = histMaxExp - histMinExp // 31 binades covered
	histBuckets int = histBinades * histSub   // 3968 regular buckets

	// histBiasOffset converts an IEEE-754 biased exponent to a binade
	// number: 1023 (float64 bias) + histMinExp. Biased exponents below this
	// are under-range; above it (+histBinades) they overflow.
	histBiasOffset = 1023 + histMinExp // 1013

	histSubF = float64(histSub) // for boundary arithmetic in Percentile
)

// histRelErr is the guaranteed maximum relative percentile error:
// (histSub+1)/histSub - 1 = 1/histSub ≈ 0.78%.
const histRelErr = 1.0 / float64(histSub)

// LogHistogram records durations into fixed log-linear buckets with O(1)
// observation cost and ~32KB of memory regardless of sample count.
// It is NOT safe for concurrent use — the streaming Collector consumes
// results from a single goroutine, which matches this design.
type LogHistogram struct {
	counts [histBuckets + 1]int // last slot: overflow (≥ 2^21 ms)
	total  int
}

// NewLogHistogram creates a ready-to-use empty histogram.
func NewLogHistogram() *LogHistogram {
	return &LogHistogram{}
}

// Observe records one duration. Hot path: one float conversion, one shift,
// one mask, one OR, one increment — no allocations, no library calls.
func (h *LogHistogram) Observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	b := math.Float64bits(ms)
	e := int(b>>52) & 0x7FF // biased exponent field

	switch {
	case e < histBiasOffset:
		// Under-range: zero, negative (clock-skew defense), or sub-µs
		// values clamp into the first bucket.
		h.counts[0]++
	case e >= histBiasOffset+histBinades:
		// Over-range: ≥ 34.9 min; also catches Inf/NaN defensively.
		h.counts[histBuckets]++
	default:
		idx := ((e - histBiasOffset) << histSubBits) |
			int(b>>(52-histSubBits))&(histSub-1)
		h.counts[idx]++
	}
	h.total++
}

// Percentile returns the approximate duration at pct (0–100), nearest-rank,
// linearly interpolated inside the containing bucket. The result carries at
// most histRelErr (~0.78%) relative error against the true nearest-rank
// value. Returns 0 when nothing has been observed or pct ≤ 0.
func (h *LogHistogram) Percentile(pct float64) time.Duration {
	if h.total == 0 || pct <= 0 {
		return 0
	}

	target := int(math.Ceil(float64(h.total) * pct / 100))
	if target > h.total {
		target = h.total
	}
	if target < 1 {
		target = 1
	}

	cumulative := 0
	for g, count := range h.counts[:histBuckets] {
		if count == 0 {
			continue
		}
		if cumulative+count >= target {
			binade := g >> histSubBits
			s := g & (histSub - 1)
			lower := math.Ldexp(float64(histSub+s)/histSubF, binade+histMinExp)
			upper := math.Ldexp(float64(histSub+s+1)/histSubF, binade+histMinExp)
			fraction := float64(target-cumulative) / float64(count)
			value := lower + fraction*(upper-lower)
			return time.Duration(value * float64(time.Millisecond))
		}
		cumulative += count
	}

	// Overflow bucket: true values lie beyond the covered ceiling, so the
	// ceiling itself is reported as a documented under-estimate. Unreachable
	// for sane HTTP timeouts (default 10s).
	return time.Duration(math.Ldexp(1, histMaxExp) * float64(time.Millisecond))
}

// Merge folds another histogram into this one by summing bucket counts.
// This makes combined percentiles across agents exact — the same property
// batch Aggregate() gets from sorting all samples locally.
func (h *LogHistogram) Merge(other *LogHistogram) {
	for i := range h.counts {
		h.counts[i] += other.counts[i]
	}
	h.total += other.total
}
