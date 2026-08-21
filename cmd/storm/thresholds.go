package main

import (
	"fmt"
	"time"

	"github.com/hariomop12/go-storm/pkg/storm"
)

// Threshold limits for CI gate evaluation. Negative values mean disabled.
var (
	failAboveErrors int
	failAboveP95    float64
)

// evaluateThresholds checks run results against the opt-in --fail-above-*
// limits. It returns a violation description when a limit is exceeded;
// nil means the gate passed (or no limits were set).
func evaluateThresholds(stats storm.Stats) string {
	if failAboveErrors >= 0 && stats.Failed > failAboveErrors {
		return fmt.Sprintf("failed requests %d exceed --fail-above-errors %d", stats.Failed, failAboveErrors)
	}
	if failAboveP95 >= 0 {
		p95ms := float64(stats.P95) / float64(time.Millisecond)
		if p95ms > failAboveP95 {
			return fmt.Sprintf("p95 latency %.2fms exceeds --fail-above-p95 %.2fms", p95ms, failAboveP95)
		}
	}
	return ""
}
