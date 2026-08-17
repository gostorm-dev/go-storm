package storm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// CapacityResult holds the outcome of a quick capacity benchmark.
type CapacityResult struct {
	MaxRPS          float64
	AvgLatency      time.Duration
	P99Latency      time.Duration
	CPUAtMax        float64
	MemoryAtMax     float64
	GoroutinesAtMax int
	GCCycles        uint32
	SuccessRate     float64
	SampleSize      int
}

// EstimateCapacity runs a short benchmark to find the machine's max RPS.
// It fires sampleSize requests at high concurrency, measures throughput,
// and samples system stats during the run.
func EstimateCapacity(
	ctx context.Context,
	url string,
	method string,
	concurrency int,
	sampleSize int,
	timeout time.Duration,
) (CapacityResult, error) {
	if sampleSize <= 0 {
		sampleSize = 200
	}
	if concurrency <= 0 {
		concurrency = 50
	}

	client := &http.Client{Timeout: timeout}
	monitor := NewMonitor(200*time.Millisecond, 10)
	monitor.Start()

	// Create jobs channel
	jobs := make(chan Job, sampleSize)
	var completed atomic.Int64
	var successCount atomic.Int64
	var totalLatency atomic.Int64

	// Start workers
	workerWg := &atomic.Int64{}
	workerWg.Store(int64(concurrency))

	for i := 0; i < concurrency; i++ {
		go func() {
			defer workerWg.Add(-1)
			for job := range jobs {
				req, err := http.NewRequestWithContext(ctx, job.Method, job.URL, nil)
				if err != nil {
					completed.Add(1)
					continue
				}
				start := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(start)
				totalLatency.Add(lat.Nanoseconds())
				completed.Add(1)
				if err == nil && resp.StatusCode < 500 {
					successCount.Add(1)
					resp.Body.Close()
				} else if resp != nil {
					resp.Body.Close()
				}
			}
		}()
	}

	// Feed jobs
	for i := 0; i < sampleSize; i++ {
		jobs <- Job{ID: i, URL: url, Method: method}
	}
	close(jobs)

	// Wait for workers to finish
	monitorWg := make(chan struct{})
	go func() {
		for workerWg.Load() > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(monitorWg)
	}()

	select {
	case <-monitorWg:
	case <-ctx.Done():
		monitor.Stop()
		return CapacityResult{}, ctx.Err()
	}

	monitor.Stop()

	comp := completed.Load()
	if comp == 0 {
		return CapacityResult{}, fmt.Errorf("no requests completed during capacity estimation")
	}

	stats := monitor.Stats()

	avgLat := time.Duration(totalLatency.Load() / comp)

	result := CapacityResult{
		SampleSize:      int(comp),
		AvgLatency:      avgLat,
		CPUAtMax:        stats.CPUUsage,
		MemoryAtMax:     stats.MemoryMB,
		GoroutinesAtMax: stats.Goroutines,
		GCCycles:        stats.GCCycles,
		SuccessRate:     float64(successCount.Load()) / float64(comp) * 100,
	}

	// Calculate RPS from the samples collected during the run
	samples := monitor.Samples()
	if len(samples) >= 2 {
		first := samples[0].Timestamp
		last := samples[len(samples)-1].Timestamp
		dur := last.Sub(first).Seconds()
		if dur > 0 {
			result.MaxRPS = float64(comp) / dur
		}
	}

	// Fallback: estimate from total time if no samples
	if result.MaxRPS == 0 && comp > 0 {
		result.MaxRPS = float64(comp) / 3.0 // rough fallback
	}

	return result, nil
}

// FormatCapacityReport returns a human-readable capacity estimation.
func FormatCapacityReport(cr CapacityResult, targetRPS float64) string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString("═══════════════════════════════════════════════\n")
	b.WriteString("     CAPACITY ESTIMATION REPORT\n")
	b.WriteString("═══════════════════════════════════════════════\n\n")

	b.WriteString("Quick Benchmark Results\n")
	b.WriteString(fmt.Sprintf("  Sample Size:       %d requests\n", cr.SampleSize))
	b.WriteString(fmt.Sprintf("  Max RPS:           %.0f\n", cr.MaxRPS))
	b.WriteString(fmt.Sprintf("  Avg Latency:       %s\n", cr.AvgLatency.Round(time.Microsecond)))
	b.WriteString(fmt.Sprintf("  Success Rate:      %.1f%%\n", cr.SuccessRate))
	b.WriteString("\n")

	b.WriteString("System at Load\n")
	b.WriteString(fmt.Sprintf("  CPU:               %.1f%%\n", cr.CPUAtMax))
	b.WriteString(fmt.Sprintf("  Memory:            %.1f MB\n", cr.MemoryAtMax))
	b.WriteString(fmt.Sprintf("  Goroutines:        %d\n", cr.GoroutinesAtMax))
	b.WriteString(fmt.Sprintf("  GC Cycles:         %d\n", cr.GCCycles))
	b.WriteString("\n")

	b.WriteString("───────────────────────────────────────────────\n")

	if targetRPS > 0 {
		ratio := cr.MaxRPS / targetRPS
		if ratio >= 1.0 {
			b.WriteString(fmt.Sprintf("  Target: %.0f RPS  ✅ ACHIEVABLE\n", targetRPS))
			b.WriteString(fmt.Sprintf("  (Your machine can handle %.0f RPS)\n", cr.MaxRPS))
		} else if ratio >= 0.85 {
			b.WriteString(fmt.Sprintf("  Target: %.0f RPS  ⚠️  TIGHT\n", targetRPS))
			b.WriteString(fmt.Sprintf("  (Your machine can handle ~%.0f RPS)\n", cr.MaxRPS))
			b.WriteString("  Results may show some generator pressure.\n")
		} else {
			b.WriteString(fmt.Sprintf("  Target: %.0f RPS  ❌ NOT ACHIEVABLE\n", targetRPS))
			b.WriteString(fmt.Sprintf("  (Your machine can handle ~%.0f RPS)\n", cr.MaxRPS))
			b.WriteString("\n")
			b.WriteString("  Recommendation:\n")
			b.WriteString("  • Use distributed mode: storm run-dist --agents 2\n")
			b.WriteString(fmt.Sprintf("  • Or reduce target: storm run -n %d\n", int(cr.MaxRPS*0.9)))
		}
	} else {
		b.WriteString(fmt.Sprintf("  Your machine can handle: ~%.0f RPS\n", cr.MaxRPS))
	}

	b.WriteString("───────────────────────────────────────────────\n")
	b.WriteString("\n═══════════════════════════════════════════════\n")

	return b.String()
}
