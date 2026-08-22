package storm

import (
	"math"
	"math/bits"
	"time"
)

// Virtual-clock arrival scheduling.
//
// Job j (0-based) is due at start + j/rate seconds. The producer re-derives
// its position from the wall clock on every iteration, so timer jitter can
// never accumulate: there is no incremental error term anywhere. All math
// uses 128-bit intermediates (bits.Mul64/Div64), making overflow impossible
// for any rate and duration combination that fits in memory.
//
// This replaced a token-bucket limiter whose burst equaled one full second
// of traffic; the pre-filled bucket deterministically overshot duration
// runs by exactly `rate` requests (-r 5000 -d 30s sent ~155k, not 150k)
// and stamped the first second of latency percentiles.

const nsPerSec = int64(1_000_000_000)

// minGradedLag is the tightest lag a dispatch can be graded against. At
// rates above 1000/s the slot interval drops below OS timer granularity
// (~tens of µs to ~1ms under load), so per-slot punctuality finer than
// this would classify scheduler noise as lateness — a healthy run showed
// "70% accuracy" purely from 200µs slots vs sub-ms jitter. Real generator
// saturation manifests as multi-millisecond or growing lag, which this
// floor still flags.
const minGradedLagNS = int64(time.Millisecond)

// dueIndex returns how many arrival slots have fully elapsed after elapsedNS:
// floor(elapsedNS × rate / 1e9).
func dueIndex(elapsedNS int64, rate uint64) int64 {
	if elapsedNS <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(elapsedNS), rate)
	q, _ := bits.Div64(hi, lo, uint64(nsPerSec))
	return int64(q)
}

// slotOffsetNS returns the scheduled departure of job j (0-based):
// floor(j × 1e9 / rate) nanoseconds after schedule start.
// Departures beyond the representable horizon (~292 years) clamp to MaxInt64.
func slotOffsetNS(j int64, rate uint64) int64 {
	hi, lo := bits.Mul64(uint64(j), uint64(nsPerSec))
	if hi >= rate {
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, rate)
	return int64(q)
}

// arrivalLimit returns how many scheduled slots fall inside a window of dur:
// ceil(dur × rate / 1e9). Slot times are j/rate for j = 0..limit-1, all
// strictly less than dur — computed once up front so the dispatch count is
// fixed before the first byte leaves the process.
func arrivalLimit(dur time.Duration, rate uint64) int64 {
	if dur <= 0 || rate == 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(dur.Nanoseconds()), rate)
	q, r := bits.Div64(hi, lo, uint64(nsPerSec))
	if r > 0 {
		q++
	}
	return int64(q)
}

// ArrivalAccuracy is the post-run summary of how well actual dispatches
// matched the pre-computed arrival schedule. A slot is "late" when its lag
// exceeded one full slot interval (1/rate) — i.e. the generator, not the
// target, fell behind.
type ArrivalAccuracy struct {
	Sent        int64   `json:"dispatched"`
	Late        int64   `json:"late_dispatches"`
	IntervalMS  float64 `json:"slot_interval_ms"`
	GradedMS    float64 `json:"late_threshold_ms"`
	AccuracyPct float64 `json:"arrival_accuracy_pct"`
	LagP50MS    float64 `json:"schedule_lag_p50_ms"`
	LagP99MS    float64 `json:"schedule_lag_p99_ms"`
	MaxLagMS    float64 `json:"schedule_lag_max_ms"`
}

// arrivalTelemetry records dispatch lag. Written only by the producer
// goroutine and read after Run joins it — no synchronization needed.
type arrivalTelemetry struct {
	intervalNS int64 // slot width = 1s/rate (informational)
	gradedNS   int64 // lateness threshold: max(interval, minGradedLagNS)
	sent       int64
	late       int64
	maxLagNS   int64
	lag        *LogHistogram
}

func newArrivalTelemetry(rate int) *arrivalTelemetry {
	interval := nsPerSec / int64(rate)
	graded := interval
	if graded < minGradedLagNS {
		graded = minGradedLagNS
	}
	return &arrivalTelemetry{
		intervalNS: interval,
		gradedNS:   graded,
		lag:        NewLogHistogram(),
	}
}

// record notes one dispatched job's deviation from its scheduled slot.
func (a *arrivalTelemetry) record(lag time.Duration) {
	a.sent++
	ns := int64(lag)
	if ns < 0 {
		ns = 0 // early dispatch cannot happen by design; clock-skew defense
	}
	if ns > a.gradedNS {
		a.late++
	}
	if ns > a.maxLagNS {
		a.maxLagNS = ns
	}
	a.lag.Observe(time.Duration(ns))
}

// snapshot returns nil when scheduling was disabled or nothing dispatched.
func (a *arrivalTelemetry) snapshot() *ArrivalAccuracy {
	if a == nil || a.sent == 0 {
		return nil
	}
	return &ArrivalAccuracy{
		Sent:        a.sent,
		Late:        a.late,
		IntervalMS:  float64(a.intervalNS) / 1e6,
		GradedMS:    float64(a.gradedNS) / 1e6,
		AccuracyPct: float64(a.sent-a.late) / float64(a.sent) * 100,
		LagP50MS:    ms(a.lag.Percentile(50)),
		LagP99MS:    ms(a.lag.Percentile(99)),
		MaxLagMS:    float64(a.maxLagNS) / 1e6,
	}
}

// produceScheduled dispatches jobs on fixed virtual-clock slots.
//
// The limit is fixed before the run starts: ceil(duration × rate) arrivals
// inside the window in duration mode, TotalReqs in count mode. When workers
// fall behind and the jobs channel blocks, catch-up batches are sent as
// soon as space allows and every affected slot is recorded as late —
// generator-caused deviation is measured, never hidden.
func (lt *LoadTester) produceScheduled() {
	rate := uint64(lt.config.Rate)

	limit := int64(lt.config.TotalReqs)
	if lt.config.Duration > 0 {
		limit = arrivalLimit(lt.config.Duration, rate)
	}
	if limit <= 0 {
		return
	}

	start := time.Now()
	tel := newArrivalTelemetry(lt.config.Rate)
	lt.arrival = tel

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	var j int64
	for {
		due := dueIndex(int64(time.Since(start)), rate)
		if due >= limit {
			due = limit - 1
		}

		for ; j <= due; j++ {
			select {
			case <-lt.ctx.Done():
				return
			case lt.jobs <- lt.job(j + 1):
			}
			tel.record(time.Since(start) - time.Duration(slotOffsetNS(j, rate)))
		}
		if j >= limit {
			return
		}

		wait := time.Duration(slotOffsetNS(j, rate)) - time.Since(start)
		if wait > 0 {
			timer.Reset(wait)
			select {
			case <-lt.ctx.Done():
				return
			case <-timer.C:
			}
		}
	}
}
