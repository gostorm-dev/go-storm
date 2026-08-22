package main

import (
	"testing"
	"time"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

func TestEvaluateThresholds(t *testing.T) {
	restore := func() {
		failAboveErrors = -1
		failAboveP95 = -1
	}
	defer restore()

	stats := storm.Stats{
		Failed: 47,
		P95:    842 * time.Millisecond,
	}

	testCases := []struct {
		name        string
		errLimit    int
		p95Limit    float64
		wantViolate bool
	}{
		{name: "disabled by default", errLimit: -1, p95Limit: -1, wantViolate: false},
		{name: "error limit exceeded", errLimit: 20, p95Limit: -1, wantViolate: true},
		{name: "error limit exactly met", errLimit: 47, p95Limit: -1, wantViolate: false},
		{name: "zero tolerance with failures", errLimit: 0, p95Limit: -1, wantViolate: true},
		{name: "p95 exceeded", errLimit: -1, p95Limit: 500, wantViolate: true},
		{name: "p95 within limit", errLimit: -1, p95Limit: 900, wantViolate: false},
		{name: "both pass", errLimit: 100, p95Limit: 1000, wantViolate: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			failAboveErrors = tc.errLimit
			failAboveP95 = tc.p95Limit
			got := evaluateThresholds(stats)
			if tc.wantViolate && got == "" {
				t.Errorf("evaluateThresholds() = \"\", want violation (errors=%d p95=%v)", tc.errLimit, tc.p95Limit)
			}
			if !tc.wantViolate && got != "" {
				t.Errorf("evaluateThresholds() = %q, want none", got)
			}
		})
	}
}

func TestEvaluateThresholdsCleanStats(t *testing.T) {
	restore := func() { failAboveErrors = -1; failAboveP95 = -1 }
	defer restore()

	clean := storm.Stats{Failed: 0, P95: 120 * time.Millisecond}
	failAboveErrors = 0
	failAboveP95 = 500
	if got := evaluateThresholds(clean); got != "" {
		t.Errorf("clean stats flagged: %q", got)
	}
}
