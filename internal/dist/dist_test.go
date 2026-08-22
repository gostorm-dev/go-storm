package dist

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

// newTestRedis spins up an in-process miniredis and hands back a Redis
// wrapper with shortened timings so timeout/expiry paths stay fast.
// Timings stay ABOVE miniredis's internal 1-second duration floor — values
// below it are silently truncated to 1s, which would make expiry math lie.
func newTestRedis(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	r := NewRedis(mr.Addr())
	r.PopTimeout = 1200 * time.Millisecond
	r.IdleTimeout = 500 * time.Millisecond // client-side wall clock — unaffected by the floor
	// Whole seconds: Redis EXPIRE truncates to integer seconds, so a
	// sub-second value here would renew for LESS than it appears to.
	r.HeartbeatTTL = 2 * time.Second
	r.AgentsWait = 2 * time.Second
	t.Cleanup(func() { _ = r.Client.Close() })
	return r, mr
}

func makeJobs(n int) []storm.Job {
	jobs := make([]storm.Job, n)
	for i := range jobs {
		jobs[i] = storm.Job{
			ID:      int64(i + 1),
			URL:     "http://target.local/ping",
			Method:  "GET",
			Headers: http.Header{"X-Test": {"v"}},
		}
	}
	return jobs
}

// TestWireFormatJobRoundTrip proves jobs survive the JSON wire format —
// including the http.Header map, which must not silently drop values.
func TestWireFormatJobRoundTrip(t *testing.T) {
	orig := makeJobs(1)[0]
	dj := distJob{RunID: "run-42", Job: orig}

	b, err := json.Marshal(dj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back distJob
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if back.RunID != "run-42" || back.Job.ID != orig.ID || back.Job.URL != orig.URL ||
		back.Job.Method != orig.Method || back.Job.Headers.Get("X-Test") != "v" {
		t.Fatalf("round trip lost data: %+v", back)
	}
}

// TestWireFormatResultRoundTrip covers both result shapes: success results
// keep status/duration; failure results carry the error message as a string.
func TestWireFormatResultRoundTrip(t *testing.T) {
	ok := storm.Result{JobID: 7, StatusCode: 200, Duration: 15 * time.Millisecond, Timestamp: time.Now()}
	failed := storm.Result{JobID: 8, Error: errors.New("connection refused")}

	for _, tc := range []struct {
		name   string
		result storm.Result
	}{
		{"success", ok},
		{"failure", failed},
	} {
		dr := toDistResult("agent-x", tc.result)
		back := dr.toResult()

		if back.JobID != tc.result.JobID {
			t.Errorf("%s: job id %d != %d", tc.name, back.JobID, tc.result.JobID)
		}
		if (back.Error == nil) != (tc.result.Error == nil) {
			t.Errorf("%s: error presence changed: %v", tc.name, back.Error)
		}
		if back.Error != nil && back.Error.Error() != tc.result.Error.Error() {
			t.Errorf("%s: error text %q != %q", tc.name, back.Error, tc.result.Error)
		}
		if back.StatusCode != tc.result.StatusCode || back.Duration != tc.result.Duration {
			t.Errorf("%s: status/duration drifted: %+v", tc.name, back)
		}
	}
}

// TestPushJobsAndPopRoundTrip pushes more than one chunk worth of jobs and
// verifies every job comes back exactly once with the right run ID.
// Order is intentionally NOT asserted: LPUSH chunking makes pop order
// chunk-reversed by design; correctness is about the set, not the order.
func TestPushJobsAndPopRoundTrip(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()

	const n = 250 // > chunkSize(100): exercises multi-chunk publishing
	if err := r.PushJobs(ctx, "run-A", makeJobs(n)); err != nil {
		t.Fatalf("PushJobs: %v", err)
	}
	if l, _ := r.Client.LLen(ctx, queueKey).Result(); int(l) != n {
		t.Fatalf("queue length = %d, want %d", l, n)
	}

	seen := map[int64]bool{}
	for i := 0; i < n; i++ {
		job, runID, ok, err := r.PopJob(ctx)
		if err != nil || !ok {
			t.Fatalf("pop %d: ok=%v err=%v", i, ok, err)
		}
		if runID != "run-A" {
			t.Fatalf("job %d carried runID %q", job.ID, runID)
		}
		if seen[job.ID] {
			t.Fatalf("job %d delivered twice", job.ID)
		}
		seen[job.ID] = true
	}
	if len(seen) != n {
		t.Fatalf("delivered %d unique jobs, want %d", len(seen), n)
	}

	if _, _, ok, _ := r.PopJob(ctx); ok {
		t.Fatal("empty queue still returned a job")
	}
}

// TestResultsCountAndDecode verifies the result list grows per push and the
// coordinator-side decode path recovers original values.
func TestResultsCountAndDecode(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()
	runID := "run-B"

	for i := 1; i <= 5; i++ {
		res := storm.Result{JobID: int64(i), StatusCode: 200, Duration: time.Duration(i) * time.Millisecond, Timestamp: time.Now()}
		if i == 5 {
			res = storm.Result{JobID: 5, Error: errors.New("boom"), Timestamp: time.Now()}
		}
		if err := r.PushResult(ctx, runID, "agent-1", res); err != nil {
			t.Fatalf("PushResult %d: %v", i, err)
		}
		if c, _ := r.ResultsCount(ctx, runID); c != int64(i) {
			t.Fatalf("count after push %d = %d", i, c)
		}
	}

	items, err := r.Client.LRange(ctx, resultKey(runID), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	errTextSeen := false
	for _, item := range items {
		var dr distResult
		if err := json.Unmarshal([]byte(item), &dr); err != nil {
			t.Fatalf("decode stored result: %v", err)
		}
		if dr.AgentID != "agent-1" {
			t.Errorf("stored result lost agent id: %+v", dr)
		}
		if dr.Error == "boom" {
			errTextSeen = true
		}
	}
	if !errTextSeen {
		t.Error("error result text did not survive storage")
	}
}

// TestRegistryLifecycle walks register → visible → unregister → gone.
func TestRegistryLifecycle(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()

	if err := r.RegisterAgent(ctx, "alpha"); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	agents, err := r.Agents(ctx)
	if err != nil {
		t.Fatalf("Agents: %v", err)
	}
	if len(agents) != 1 || agents[0].ID != "alpha" || agents[0].Hostname == "" {
		t.Fatalf("agents = %+v, want [alpha] with hostname", agents)
	}

	if err := r.UnregisterAgent(ctx, "alpha"); err != nil {
		t.Fatalf("UnregisterAgent: %v", err)
	}
	if agents, _ = r.Agents(ctx); len(agents) != 0 {
		t.Fatalf("agents after unregister = %+v, want empty", agents)
	}
}

// TestAgentTTLExpiryAndHeartbeatRenewal proves dead-agent detection is real.
// All time movement is via miniredis's FastForward — no wall-clock sleeps,
// so the sequence is deterministic:
//
//	register (TTL 2s) → +1s → heartbeat renews (EXPIRE 2s) → alive at +1s
//	more, then dead once the renewed window lapses.
func TestAgentTTLExpiryAndHeartbeatRenewal(t *testing.T) {
	r, mr := newTestRedis(t)
	ctx := context.Background()

	if err := r.RegisterAgent(ctx, "ghost"); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}

	// Consume half of the original TTL, then renew.
	mr.FastForward(1 * time.Second)
	if err := r.Heartbeat(ctx, "ghost"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	mr.FastForward(1 * time.Second) // inside the renewed 2s window

	agents, err := r.Agents(ctx)
	if err != nil {
		t.Fatalf("Agents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("renewed agent expired early: %+v", agents)
	}

	// No further heartbeat → the renewed window lapses.
	mr.FastForward(1200 * time.Millisecond)
	if agents, _ = r.Agents(ctx); len(agents) != 0 {
		t.Fatalf("dead agent still listed: %+v", agents)
	}
}

// TestWaitForAgentsReturnsWhenReady checks the happy path returns promptly.
func TestWaitForAgentsReturnsWhenReady(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = r.RegisterAgent(ctx, "late-agent")
	}()

	start := time.Now()
	if err := r.waitForAgents(ctx, 1); err != nil {
		t.Fatalf("waitForAgents: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Errorf("waited %v for one agent, too slow", elapsed)
	}
}

// TestWaitForAgentsTimesOut ensures the coordinator errors out instead of
// hanging forever when agents never show up.
func TestWaitForAgentsTimesOut(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()

	err := r.waitForAgents(ctx, 3)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want timeout message", err)
	}
}

// TestRunAgentProcessesAllJobs runs the full agent loop against miniredis:
// every queued job is executed via HTTP and its result lands in the run's
// list; the agent deregisters on exit.
func TestRunAgentProcessesAllJobs(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const n = 10
	jobs := makeJobs(n)
	for i := range jobs {
		jobs[i].URL = srv.URL
	}
	runID := "run-e2e"
	if err := r.PushJobs(ctx, runID, jobs); err != nil {
		t.Fatalf("PushJobs: %v", err)
	}

	var observed atomic.Int64
	processed, err := r.RunAgent(ctx, "worker-1", 3, 2*time.Second,
		false,
		func(storm.Job) { observed.Add(1) },
		func(storm.Result) {},
	)
	if err != nil {
		t.Fatalf("RunAgent: %v", err)
	}
	if processed != n || hits.Load() != n || observed.Load() != n {
		t.Fatalf("processed=%d http=%d hooks=%d, want %d each",
			processed, hits.Load(), observed.Load(), n)
	}

	if c, _ := r.ResultsCount(ctx, runID); c != n {
		t.Fatalf("results in store = %d, want %d", c, n)
	}
	if agents, _ := r.Agents(ctx); len(agents) != 0 {
		t.Fatalf("agent still registered after exit: %+v", agents)
	}
}

// TestCoordinatorAggregationAndIsolation pins the two core coordinator
// guarantees: per-agent breakdown math, and stale-run isolation — results
// from an older run must never leak into this report even when present.
func TestCoordinatorAggregationAndIsolation(t *testing.T) {
	r, _ := newTestRedis(t)
	ctx := context.Background()
	runID := "run-current"

	_ = r.RegisterAgent(ctx, "a1")
	_ = r.RegisterAgent(ctx, "a2")

	// Stale results from an earlier run — must be invisible below.
	stale := storm.Result{JobID: 999, StatusCode: 500, Timestamp: time.Now()}
	if err := r.PushResult(ctx, "run-STALE", "a1", stale); err != nil {
		t.Fatalf("stale PushResult: %v", err)
	}

	jobs := makeJobs(6)
	current := map[string][]storm.Result{
		"a1": {
			{JobID: 1, StatusCode: 200, Duration: 10 * time.Millisecond, Timestamp: time.Now()},
			{JobID: 2, StatusCode: 200, Duration: 20 * time.Millisecond, Timestamp: time.Now()},
			{JobID: 3, StatusCode: 200, Duration: 30 * time.Millisecond, Timestamp: time.Now()},
			{JobID: 4, StatusCode: 200, Duration: 40 * time.Millisecond, Timestamp: time.Now()},
		},
		"a2": {
			{JobID: 5, StatusCode: 200, Duration: 50 * time.Millisecond, Timestamp: time.Now()},
			{JobID: 6, StatusCode: 503, Duration: 60 * time.Millisecond, Timestamp: time.Now()},
		},
	}

	// Pre-seed current-run results (the queued jobs themselves are left
	// unconsumed; the coordinator only waits for result count).
	for agentID, results := range current {
		for _, res := range results {
			if err := r.PushResult(ctx, runID, agentID, res); err != nil {
				t.Fatalf("seed PushResult: %v", err)
			}
		}
	}

	stats, breakdown, err := r.RunCoordinator(ctx, runID, jobs, 0)
	if err != nil {
		t.Fatalf("RunCoordinator: %v", err)
	}

	if stats.TotalRequests != 6 {
		t.Errorf("total = %d, want 6 (stale run leaked?)", stats.TotalRequests)
	}
	if stats.Failed != 1 { // only the 503 counts as failed
		t.Errorf("failed = %d, want 1", stats.Failed)
	}
	if stats.RequestsPerSec <= 0 {
		t.Errorf("rps = %f, want > 0", stats.RequestsPerSec)
	}

	if len(breakdown) != 2 {
		t.Fatalf("breakdown size = %d, want 2", len(breakdown))
	}
	if breakdown[0].Agent.ID != "a1" || breakdown[1].Agent.ID != "a2" {
		t.Errorf("breakdown order = [%s,%s], want sorted [a1,a2]",
			breakdown[0].Agent.ID, breakdown[1].Agent.ID)
	}
	if breakdown[0].Stats.TotalRequests != 4 || breakdown[1].Stats.TotalRequests != 2 {
		t.Errorf("per-agent totals = %d/%d, want 4/2",
			breakdown[0].Stats.TotalRequests, breakdown[1].Stats.TotalRequests)
	}
}

// TestFlushClearsQueueAndResults guarantees a fresh start: leftover jobs and
// result lists from previous runs are wiped.
func TestFlushClearsQueueAndResults(t *testing.T) {
	r, mr := newTestRedis(t)
	ctx := context.Background()

	if err := r.PushJobs(ctx, "run-old", makeJobs(3)); err != nil {
		t.Fatalf("PushJobs: %v", err)
	}
	if err := r.PushResult(ctx, "run-old", "a1", storm.Result{JobID: 1, StatusCode: 200}); err != nil {
		t.Fatalf("PushResult: %v", err)
	}

	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if l, _ := r.Client.LLen(ctx, queueKey).Result(); l != 0 {
		t.Errorf("queue length after flush = %d, want 0", l)
	}
	keys := mr.Keys() // miniredis helper: all keys
	for _, k := range keys {
		if strings.HasPrefix(k, resultsPrefix) {
			t.Errorf("result key survived flush: %s", k)
		}
	}
}
