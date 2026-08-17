package storm

import (
	"fmt"
	"strings"
	"time"
)

// Severity indicates how bad a saturation signal is.
type Severity int

const (
	OK       Severity = iota
	WARN              // approaching limit — test continues
	CRITICAL          // limit breached — test should stop
)

func (s Severity) String() string {
	switch s {
	case WARN:
		return "WARN"
	case CRITICAL:
		return "CRITICAL"
	default:
		return "OK"
	}
}

// Thresholds defines the saturation detection limits.
type Thresholds struct {
	// CPU usage percentage (0-100).
	CPUWarn     float64
	CPUCritical float64

	// Memory growth MB/min — detects runaway allocation.
	MemoryWarnMB     float64
	MemoryCriticalMB float64

	// GC pause total in milliseconds over a 1-second window.
	GCPauseWarnMS     float64
	GCPauseCriticalMS float64

	// Goroutine count as multiple of concurrency.
	GoroutineWarnMult     float64
	GoroutineCriticalMult float64

	// File descriptor usage ratio (0-1).
	FDWarnRatio     float64
	FDCriticalRatio float64

	// RPS achievement ratio (achieved/target).
	RPSWarnRatio     float64
	RPSCriticalRatio float64

	// Worker utilization ratio (busy / total time).
	WorkerWarnRatio     float64
	WorkerCriticalRatio float64
}

// DefaultThresholds returns sensible defaults for most environments.
func DefaultThresholds() Thresholds {
	return Thresholds{
		CPUWarn:               85,
		CPUCritical:           95,
		MemoryWarnMB:          100,
		MemoryCriticalMB:      500,
		GCPauseWarnMS:         100,
		GCPauseCriticalMS:     500,
		GoroutineWarnMult:     10,
		GoroutineCriticalMult: 20,
		FDWarnRatio:           0.80,
		FDCriticalRatio:       0.95,
		RPSWarnRatio:          0.90,
		RPSCriticalRatio:      0.70,
		WorkerWarnRatio:       0.95,
		WorkerCriticalRatio:   0.99,
	}
}

// Signal is one saturation check result for a single factor.
type Signal struct {
	Factor    string
	Severity  Severity
	Value     float64
	Threshold float64
	Message   string
}

// Diagnosis is the full result of a saturation check.
type Diagnosis struct {
	Level      Severity // worst severity across all signals
	Signals    []Signal // individual signals
	KillReason string   // non-empty when Level == CRITICAL
}

// HealthReport is the final generator health summary.
type HealthReport struct {
	Level           Severity
	Duration        time.Duration
	AchievedRPS     float64
	TargetRPS       float64
	Stats           SystemStats
	Signals         []Signal
	MaxMemoryMB     float64
	Recommendations []string
}

// CheckSaturation evaluates all signals and returns a Diagnosis.
func CheckSaturation(
	th Thresholds,
	stats SystemStats,
	targetRPS float64,
	achievedRPS float64,
	concurrency int,
	completed int64,
	elapsed time.Duration,
	maxFDs int,
	workerUtil float64,
	peakMemoryMB float64,
) Diagnosis {
	d := Diagnosis{Level: OK}

	// --- 1. CPU ---
	d.addSignal(Signal{
		Factor:    "CPU",
		Value:     stats.CPUUsage,
		Threshold: th.CPUCritical,
	}, evaluateThreshold(stats.CPUUsage, th.CPUWarn, th.CPUCritical, "%"))

	// --- 2. Memory growth rate ---
	if elapsed.Seconds() > 5 && peakMemoryMB > 0 {
		memRate := peakMemoryMB / elapsed.Seconds() * 60 // MB/min
		d.addSignal(Signal{
			Factor:    "Memory Growth",
			Value:     memRate,
			Threshold: th.MemoryCriticalMB,
		}, evaluateThreshold(memRate, th.MemoryWarnMB, th.MemoryCriticalMB, "MB/min"))
	}

	// --- 3. GC pressure ---
	if stats.GCCycles > 0 {
		gcPauseMS := float64(stats.GCPauseNS) / 1e6
		d.addSignal(Signal{
			Factor:    "GC Pause",
			Value:     gcPauseMS,
			Threshold: th.GCPauseCriticalMS,
		}, evaluateThreshold(gcPauseMS, th.GCPauseWarnMS, th.GCPauseCriticalMS, "ms"))
	}

	// --- 4. Goroutines ---
	if concurrency > 0 {
		goratio := float64(stats.Goroutines) / float64(concurrency)
		d.addSignal(Signal{
			Factor:    "Goroutines",
			Value:     float64(stats.Goroutines),
			Threshold: th.GoroutineCriticalMult * float64(concurrency),
		}, evaluateThreshold(goratio, th.GoroutineWarnMult, th.GoroutineCriticalMult, "x concurrency"))
	}

	// --- 5. File descriptors ---
	if maxFDs > 0 {
		fdRatio := float64(stats.FDCount) / float64(maxFDs)
		d.addSignal(Signal{
			Factor:    "File Descriptors",
			Value:     float64(stats.FDCount),
			Threshold: th.FDCriticalRatio * float64(maxFDs),
		}, evaluateThreshold(fdRatio, th.FDWarnRatio, th.FDCriticalRatio, "x limit"))
	}

	// --- 6. RPS achievement ---
	if targetRPS > 0 {
		rpsRatio := achievedRPS / targetRPS
		d.addSignal(Signal{
			Factor:    "RPS Achievement",
			Value:     achievedRPS,
			Threshold: th.RPSCriticalRatio * targetRPS,
		}, evaluateThresholdInverse(rpsRatio, th.RPSWarnRatio, th.RPSCriticalRatio))
	}

	// --- 7. Worker utilization ---
	d.addSignal(Signal{
		Factor:    "Worker Utilization",
		Value:     workerUtil * 100,
		Threshold: th.WorkerCriticalRatio * 100,
	}, evaluateThreshold(workerUtil, th.WorkerWarnRatio, th.WorkerCriticalRatio, ""))

	// --- Build kill reason ---
	if d.Level == CRITICAL {
		var reasons []string
		for _, s := range d.Signals {
			if s.Severity == CRITICAL {
				reasons = append(reasons, s.Message)
			}
		}
		d.KillReason = strings.Join(reasons, "; ")
	}

	return d
}

func (d *Diagnosis) addSignal(s Signal, sev Severity) {
	s.Severity = sev
	if s.Message == "" {
		s.Message = formatSignalMessage(s)
	}
	if sev > d.Level {
		d.Level = sev
	}
	d.Signals = append(d.Signals, s)
}

func formatSignalMessage(s Signal) string {
	switch s.Factor {
	case "CPU":
		return fmt.Sprintf("%.1f%%", s.Value)
	case "Memory Growth":
		return fmt.Sprintf("%.0f MB/min", s.Value)
	case "GC Pause":
		return fmt.Sprintf("%.1f ms", s.Value)
	case "Goroutines":
		return fmt.Sprintf("%.0f", s.Value)
	case "File Descriptors":
		return fmt.Sprintf("%.0f", s.Value)
	case "RPS Achievement":
		return fmt.Sprintf("%.0f / %.0f", s.Value, s.Threshold)
	case "Worker Utilization":
		return fmt.Sprintf("%.1f%%", s.Value)
	default:
		return fmt.Sprintf("%.1f", s.Value)
	}
}

// evaluateThreshold returns WARN or CRITICAL based on value vs thresholds.
func evaluateThreshold(value, warn, critical float64, unit string) Severity {
	if value >= critical {
		return CRITICAL
	}
	if value >= warn {
		return WARN
	}
	return OK
}

// evaluateThresholdInverse returns WARN/CRITICAL when value is BELOW threshold
// (used for RPS achievement where lower = worse).
func evaluateThresholdInverse(ratio, warnRatio, criticalRatio float64) Severity {
	if ratio <= criticalRatio {
		return CRITICAL
	}
	if ratio <= warnRatio {
		return WARN
	}
	return OK
}

// FormatHealthReport returns a human-readable generator health report.
func FormatHealthReport(hr HealthReport) string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString("═══════════════════════════════════════════════\n")
	b.WriteString("        GENERATOR HEALTH REPORT\n")
	b.WriteString("═══════════════════════════════════════════════\n\n")

	// --- Load ---
	b.WriteString("Load\n")
	b.WriteString(fmt.Sprintf("  Target RPS:       %s\n", formatFloat(hr.TargetRPS)))
	b.WriteString(fmt.Sprintf("  Achieved RPS:     %s", formatFloat(hr.AchievedRPS)))
	if hr.TargetRPS > 0 {
		ratio := hr.AchievedRPS / hr.TargetRPS * 100
		if ratio >= 95 {
			b.WriteString("  ✅\n")
		} else if ratio >= 70 {
			b.WriteString(fmt.Sprintf("  ⚠️  (%.0f%%)\n", ratio))
		} else {
			b.WriteString(fmt.Sprintf("  🔴 (%.0f%%)\n", ratio))
		}
	} else {
		b.WriteString("\n")
	}
	b.WriteString("\n")

	// --- System Resources ---
	b.WriteString("System Resources\n")
	cpuIcon := iconFor(hr.Stats.CPUUsage, 85, 95)
	b.WriteString(fmt.Sprintf("  CPU Usage:      %5.1f%% %s\n", hr.Stats.CPUUsage, cpuIcon))
	b.WriteString(fmt.Sprintf("  Memory:        %6.1f MB (Heap: %.1f MB)\n", hr.Stats.MemoryMB, hr.Stats.HeapMB))
	b.WriteString(fmt.Sprintf("  Goroutines:       %d\n", hr.Stats.Goroutines))
	b.WriteString(fmt.Sprintf("  GC Cycles:        %d\n", hr.Stats.GCCycles))
	if hr.Stats.GCCycles > 0 {
		b.WriteString(fmt.Sprintf("  GC Total Pause: %.1f ms\n", float64(hr.Stats.GCPauseNS)/1e6))
	}
	if hr.Stats.FDCount > 0 {
		b.WriteString(fmt.Sprintf("  File Descriptors: %d\n", hr.Stats.FDCount))
	}
	b.WriteString("\n")

	// --- Signals ---
	b.WriteString("Checks\n")
	for _, s := range hr.Signals {
		icon := "✅"
		switch s.Severity {
		case WARN:
			icon = "⚠️ "
		case CRITICAL:
			icon = "🔴"
		}
		b.WriteString(fmt.Sprintf("  %s %-20s %s\n", icon, s.Factor+":", s.Message))
	}
	b.WriteString("\n")

	// --- Verdict ---
	b.WriteString("───────────────────────────────────────────────\n")
	switch hr.Level {
	case OK:
		b.WriteString("  ✅ GENERATOR HEALTHY\n")
		b.WriteString("  Results are trustworthy.\n")
	case WARN:
		b.WriteString("  ⚠️  GENERATOR UNDER PRESSURE\n")
		b.WriteString("  Results are likely valid but monitor closely.\n")
	case CRITICAL:
		b.WriteString("  🔴 GENERATOR SATURATED\n")
		b.WriteString("  Results may NOT be representative of target.\n")
	}
	b.WriteString("───────────────────────────────────────────────\n")

	// --- Recommendations ---
	if len(hr.Recommendations) > 0 {
		b.WriteString("\nRecommendations\n")
		for _, r := range hr.Recommendations {
			b.WriteString(fmt.Sprintf("  • %s\n", r))
		}
	}

	b.WriteString("\n═══════════════════════════════════════════════\n")
	return b.String()
}

// GenerateRecommendations builds actionable advice from the diagnosis.
func GenerateRecommendations(hr HealthReport) []string {
	var recs []string

	if hr.TargetRPS > 0 && hr.AchievedRPS/hr.TargetRPS < 0.90 {
		recs = append(recs, "Reduce target RPS or use distributed mode")
	}
	if hr.Stats.CPUUsage > 90 {
		recs = append(recs, "Reduce concurrency to lower CPU pressure")
	}
	if hr.Stats.GCCycles > 100 {
		recs = append(recs, "High GC pressure — reduce allocation rate or concurrency")
	}
	if hr.Stats.Goroutines > 10000 {
		recs = append(recs, "Very high goroutine count — reduce concurrency")
	}
	return recs
}

func iconFor(val, warn, crit float64) string {
	if val >= crit {
		return "🔴"
	}
	if val >= warn {
		return "⚠️ "
	}
	return "✅"
}

func formatFloat(f float64) string {
	if f >= 1000000 {
		return fmt.Sprintf("%.0f", f)
	}
	if f >= 1000 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.1f", f)
}
