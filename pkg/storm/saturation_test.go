package storm

import (
	"testing"
	"time"
)

func TestMonitorStartStop(t *testing.T) {
	m := NewMonitor(100*time.Millisecond, 10)
	m.Start()

	// Wait for at least one sample
	time.Sleep(250 * time.Millisecond)

	stats := m.Stats()
	if stats.Goroutines == 0 {
		t.Error("expected goroutine count > 0")
	}
	if stats.MemoryMB <= 0 {
		t.Error("expected memory > 0")
	}

	samples := m.Samples()
	if len(samples) < 2 {
		t.Errorf("expected at least 2 samples, got %d", len(samples))
	}

	m.Stop()
}

func TestMonitorSampleBuffer(t *testing.T) {
	m := NewMonitor(50*time.Millisecond, 3)
	m.Start()
	time.Sleep(300 * time.Millisecond)
	m.Stop()

	samples := m.Samples()
	if len(samples) > 3 {
		t.Errorf("expected at most 3 samples, got %d", len(samples))
	}
}

func TestMonitorCPU(t *testing.T) {
	m := NewMonitor(100*time.Millisecond, 10)
	m.Start()
	time.Sleep(350 * time.Millisecond)
	m.Stop()

	stats := m.Stats()
	if stats.CPUUsage < 0 || stats.CPUUsage > 100 {
		t.Errorf("CPU usage out of range: %f", stats.CPUUsage)
	}
}

func TestMonitorFDs(t *testing.T) {
	fds := sampleFDs()
	if fds <= 0 {
		t.Skip("FD sampling not supported on this platform")
	}
	if fds < 3 {
		t.Errorf("expected at least 3 FDs (stdin/stdout/stderr), got %d", fds)
	}
}

func TestDefaultThresholds(t *testing.T) {
	th := DefaultThresholds()
	if th.CPUWarn >= th.CPUCritical {
		t.Error("CPUWarn should be < CPUCritical")
	}
	if th.RPSWarnRatio <= th.RPSCriticalRatio {
		t.Error("RPSWarnRatio should be > RPSCriticalRatio")
	}
	if th.WorkerWarnRatio >= th.WorkerCriticalRatio {
		t.Error("WorkerWarnRatio should be < WorkerCriticalRatio")
	}
}

func TestCheckSaturationOK(t *testing.T) {
	th := DefaultThresholds()
	stats := SystemStats{
		CPUUsage:   50,
		Goroutines: 100,
		GCCycles:   5,
		GCPauseNS:  1000000, // 1ms
		FDCount:    50,
	}

	diag := CheckSaturation(th, stats, 10000, 10000, 100, 5000, 10*time.Second, 1024, 0.5, 0)

	if diag.Level != OK {
		t.Errorf("expected OK, got %v", diag.Level)
		for _, s := range diag.Signals {
			t.Logf("  %s: %v (threshold %.1f)", s.Factor, s.Severity, s.Threshold)
		}
	}
	if diag.KillReason != "" {
		t.Errorf("expected no kill reason, got %s", diag.KillReason)
	}
}

func TestCheckSaturationCriticalCPU(t *testing.T) {
	th := DefaultThresholds()
	stats := SystemStats{
		CPUUsage:   98,
		Goroutines: 100,
		FDCount:    50,
	}

	diag := CheckSaturation(th, stats, 10000, 7000, 100, 3000, 10*time.Second, 1024, 0.9, 100)

	if diag.Level != CRITICAL {
		t.Errorf("expected CRITICAL, got %v", diag.Level)
	}
	if diag.KillReason == "" {
		t.Error("expected kill reason for critical CPU")
	}
}

func TestCheckSaturationCriticalRPSDrop(t *testing.T) {
	th := DefaultThresholds()
	stats := SystemStats{
		CPUUsage:   60,
		Goroutines: 100,
		FDCount:    50,
	}

	// Target 100k, achieved only 60k = 60% → CRITICAL
	diag := CheckSaturation(th, stats, 100000, 60000, 100, 3000, 10*time.Second, 1024, 0.5, 100)

	if diag.Level != CRITICAL {
		t.Errorf("expected CRITICAL for RPS drop, got %v", diag.Level)
	}
}

func TestCheckSaturationWarn(t *testing.T) {
	th := DefaultThresholds()
	stats := SystemStats{
		CPUUsage:   88, // between warn (85) and critical (95)
		Goroutines: 100,
		FDCount:    50,
	}

	diag := CheckSaturation(th, stats, 10000, 9000, 100, 5000, 10*time.Second, 1024, 0.5, 0)

	if diag.Level != WARN {
		t.Errorf("expected WARN, got %v", diag.Level)
		for _, s := range diag.Signals {
			t.Logf("  %s: %v (threshold %.1f)", s.Factor, s.Severity, s.Threshold)
		}
	}
}

func TestFormatCapacityReport(t *testing.T) {
	cr := CapacityResult{
		MaxRPS:      50000,
		AvgLatency:  5 * time.Millisecond,
		CPUAtMax:    85,
		MemoryAtMax: 500,
		SampleSize:  200,
		SuccessRate: 99.5,
	}

	report := FormatCapacityReport(cr, 100000)
	if report == "" {
		t.Error("expected non-empty report")
	}
	// Should mention NOT ACHIEVABLE since target > max
	if !containsStr(report, "NOT ACHIEVABLE") {
		t.Error("expected NOT ACHIEVABLE in report")
	}
}

func TestFormatCapacityReportAchievable(t *testing.T) {
	cr := CapacityResult{
		MaxRPS:      100000,
		AvgLatency:  3 * time.Millisecond,
		CPUAtMax:    70,
		MemoryAtMax: 300,
		SampleSize:  200,
		SuccessRate: 100,
	}

	report := FormatCapacityReport(cr, 50000)
	if report == "" {
		t.Error("expected non-empty report")
	}
	if !containsStr(report, "ACHIEVABLE") {
		t.Error("expected ACHIEVABLE in report")
	}
}

func TestHealthReportFormat(t *testing.T) {
	hr := HealthReport{
		Level:       OK,
		Duration:    10 * time.Second,
		AchievedRPS: 45000,
		TargetRPS:   50000,
		Stats: SystemStats{
			CPUUsage:   72,
			MemoryMB:   1200,
			Goroutines: 500,
			GCCycles:   23,
			GCPauseNS:  50000000, // 50ms
		},
		Signals: []Signal{
			{Factor: "CPU", Severity: OK, Message: "72.0%"},
			{Factor: "Memory", Severity: OK, Message: "1200 MB"},
		},
	}

	report := FormatHealthReport(hr)
	if report == "" {
		t.Error("expected non-empty report")
	}
	if !containsStr(report, "GENERATOR HEALTHY") {
		t.Error("expected HEALTHY in report")
	}
}

func TestGenerateRecommendations(t *testing.T) {
	hr := HealthReport{
		Level:       WARN,
		AchievedRPS: 30000,
		TargetRPS:   50000,
		Stats: SystemStats{
			CPUUsage:   95,
			GCCycles:   200,
			Goroutines: 15000,
		},
	}

	recs := GenerateRecommendations(hr)
	if len(recs) == 0 {
		t.Error("expected recommendations")
	}
}
