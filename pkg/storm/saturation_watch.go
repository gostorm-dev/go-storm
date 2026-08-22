package storm

import (
	"time"
)

// The watchdog terminates a run when the GENERATOR — not the target — is the
// bottleneck. A single critical sample can be a transient spike (GC pause,
// scheduler hiccup), so termination requires sustained saturation:
// killStreakRequired consecutive critical checks spaced killCheckInterval apart.
//
// Only resource signals can trigger a kill: CPU, GC pressure, file descriptors,
// goroutines and memory growth. Worker utilization and RPS achievement are
// reported but never kill, because both conflate "target is slow" or "pipeline
// is demand-fed" with "generator is exhausted" — killing on them would
// terminate healthy runs against slow targets.
const (
	// defaultKillCheckInterval matches the Monitor sampling cadence; checking
	// faster than the monitor samples only re-reads the same snapshot.
	defaultKillCheckInterval = time.Second

	// defaultKillStreak requires ~3 seconds of sustained critical saturation
	// before terminating.
	defaultKillStreak = 3

	// liveCheckWarmup delays RPS-achievement judgment until ramp-up
	// (connection setup, TLS handshakes) has settled.
	liveCheckWarmup = 5 * time.Second
)

// saturationKill records why and when the watchdog terminated a run.
type saturationKill struct {
	Reason string
	At     time.Duration
}

// resourceSignalFactors are the only factors eligible to terminate a run.
var resourceSignalFactors = map[string]bool{
	"CPU":              true,
	"Memory Growth":    true,
	"GC Pause":         true,
	"Goroutines":       true,
	"File Descriptors": true,
}

// criticalResourceSignals returns the CRITICAL messages among resource
// factors only, or "" when no resource factor is critical.
func criticalResourceSignals(diag Diagnosis) string {
	var reasons []string
	for _, s := range diag.Signals {
		if s.Severity == CRITICAL && resourceSignalFactors[s.Factor] {
			reasons = append(reasons, s.Message)
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	return reasons[0]
}

// watchSaturation evaluates generator health while the test runs. On
// sustained critical resource saturation it records the kill event and
// cancels the engine context — workers then finish in-flight requests and
// the pipeline drains gracefully.
//
// Lifecycle: started by Run() only when saturation monitoring AND kill mode
// are enabled; terminated by close(done) after collection ends, or
// self-terminates right after firing a kill.
func (lt *LoadTester) watchSaturation(done <-chan struct{}) {
	interval := lt.killCheckInterval
	if interval <= 0 {
		interval = defaultKillCheckInterval
	}
	streakNeeded := lt.killStreakRequired
	if streakNeeded <= 0 {
		streakNeeded = defaultKillStreak
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	streak := 0
	var prevStats SystemStats
	havePrev := false

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			now := time.Now()
			stats, diag := lt.evaluateLiveSaturation(now.Sub(start), prevStats, havePrev)

			if reason := criticalResourceSignals(diag); reason != "" {
				streak++
				if streak >= streakNeeded {
					lt.killEvent.Store(&saturationKill{
						Reason: reason,
						At:     now.Sub(start),
					})
					lt.cancel() // graceful drain: producer stops, workers finish in-flight requests
					return
				}
			} else {
				streak = 0
			}
			prevStats = stats
			havePrev = true
		}
	}
}

// evaluateLiveSaturation builds a Diagnosis from the current system snapshot.
// Cumulative counters are converted to window deltas so live thresholds mean
// what they say: GCPauseNS becomes the pause accumulated during this interval,
// and peak memory tracks the highest snapshot seen during the run.
func (lt *LoadTester) evaluateLiveSaturation(elapsed time.Duration, prev SystemStats, havePrev bool) (SystemStats, Diagnosis) {
	stats := lt.monitor.Stats()

	peak := lt.peakMemoryMB
	if stats.MemoryMB > peak {
		peak = stats.MemoryMB
	}

	gcDelta := uint64(0)
	if havePrev && stats.GCPauseNS > prev.GCPauseNS {
		gcDelta = stats.GCPauseNS - prev.GCPauseNS
	}
	windowStats := stats
	windowStats.GCPauseNS = gcDelta

	targetRPS := float64(lt.config.Rate)
	if elapsed < liveCheckWarmup {
		targetRPS = 0 // early samples would false-trigger the RPS signal
	}
	achievedRPS := float64(lt.completed.Load())
	if elapsed > 0 {
		achievedRPS /= elapsed.Seconds()
	}

	diag := CheckSaturation(
		lt.thresholds,
		windowStats,
		targetRPS,
		achievedRPS,
		lt.config.Concurrency,
		lt.completed.Load(),
		elapsed,
		GetMaxFDs(),
		lt.liveWorkerUtilization(elapsed),
		peak,
	)
	return stats, diag
}

// liveWorkerUtilization computes the busy-time ratio from per-request
// durations accumulated by workers — exact, no averaging approximation.
func (lt *LoadTester) liveWorkerUtilization(elapsed time.Duration) float64 {
	if elapsed <= 0 || lt.config.Concurrency <= 0 {
		return 0
	}
	capacity := time.Duration(lt.config.Concurrency) * elapsed
	ratio := float64(time.Duration(lt.busyNanos.Load())) / float64(capacity)
	if ratio > 1 {
		ratio = 1
	}
	return ratio
}
