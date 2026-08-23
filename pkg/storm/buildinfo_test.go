package storm

import (
	"context"
	"encoding/json"

	"testing"
	"time"
)

// TestReportCarriesBuildIdentity pins the reproducibility contract: every
// JSON result must name the exact build that produced it (AGENTS.md §19).
func TestReportCarriesBuildIdentity(t *testing.T) {
	srv := arrivalTestServer()
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		TotalReqs:   5,
		Concurrency: 2,
		Timeout:     5 * time.Second,
		Method:      "GET",
	}
	stats, err := NewLoadTester(context.Background(), cfg).Run()
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	data, err := ReportJSON(cfg, stats)
	if err != nil {
		t.Fatalf("ReportJSON() error = %v", err)
	}

	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if report.ToolVersion == "" {
		t.Error("tool_version is empty — results cannot be traced to a build")
	}
	if report.GitCommit == "" {
		t.Error("git_commit is empty")
	}
	if report.BuiltAt == "" {
		t.Error("built_at is empty")
	}
}
