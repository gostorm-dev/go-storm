package dist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hariomop12/go-storm/pkg/storm"
)

const (
	queueKey      = "storm:jobs"
	agentsPattern = "storm:agent:*"
	popTimeout    = 2 * time.Second  // BLPop wait before reporting empty
	idleTimeout   = 5 * time.Second  // agent exits after this long with no jobs
	heartbeatTTL  = 5 * time.Second  // agent entry expires without a heartbeat
	agentsWait    = 30 * time.Second // coordinator gives up waiting for agents
)

func agentKey(id string) string {
	return "storm:agent:" + id
}

func resultKey(runID string) string {
	return "storm:results:" + runID
}

// Redis wraps a go-redis client with the queue operations both sides use.
type Redis struct {
	Client *redis.Client
}

// NewRedis creates a client for the given address, e.g. "localhost:6379".
func NewRedis(addr string) *Redis {
	return &Redis{Client: redis.NewClient(&redis.Options{Addr: addr})}
}

// Ping verifies the connection.
func (r *Redis) Ping(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}

// distJob is the wire format for jobs in the queue. It carries the run ID so
// every result can be tagged with the run it belongs to — stale results from
// earlier runs can never pollute a new report.
type distJob struct {
	RunID string    `json:"run_id"`
	Job   storm.Job `json:"job"`
}

// NewRunID returns a unique id for one coordinator run.
func NewRunID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// PushJobs publishes all jobs to the queue in chunks (LPUSH), tagging each
// with the run ID.
func (r *Redis) PushJobs(ctx context.Context, runID string, jobs []storm.Job) error {
	const chunkSize = 100

	for i := 0; i < len(jobs); i += chunkSize {
		end := i + chunkSize
		if end > len(jobs) {
			end = len(jobs)
		}

		payloads := make([]interface{}, 0, end-i)
		for _, job := range jobs[i:end] {
			b, err := json.Marshal(distJob{RunID: runID, Job: job})
			if err != nil {
				return err
			}
			payloads = append(payloads, string(b))
		}

		if err := r.Client.LPush(ctx, queueKey, payloads...).Err(); err != nil {
			return err
		}
	}
	return nil
}

// PopJob blocks up to popTimeout for a job. Returns ok=false when empty.
func (r *Redis) PopJob(ctx context.Context) (storm.Job, string, bool, error) {
	item, err := r.Client.BLPop(ctx, popTimeout, queueKey).Result()
	if errors.Is(err, redis.Nil) {
		return storm.Job{}, "", false, nil
	}
	if err != nil {
		return storm.Job{}, "", false, err
	}

	var dj distJob
	if err := json.Unmarshal([]byte(item[1]), &dj); err != nil {
		return storm.Job{}, "", false, err
	}
	return dj.Job, dj.RunID, true, nil
}

// AgentInfo describes a registered agent.
type AgentInfo struct {
	ID        string    `json:"id"`
	Hostname  string    `json:"hostname"`
	StartedAt time.Time `json:"started_at"`
}

// RegisterAgent records the agent in Redis with a short TTL. The agent must
// keep calling Heartbeat or its entry expires, which is how the coordinator
// detects a dead agent.
func (r *Redis) RegisterAgent(ctx context.Context, id string) error {
	info := AgentInfo{ID: id, StartedAt: time.Now()}
	if host, err := os.Hostname(); err == nil {
		info.Hostname = host
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, agentKey(id), string(b), heartbeatTTL).Err()
}

// Heartbeat renews the agent's TTL. Run it in a goroutine while the agent
// is alive.
func (r *Redis) Heartbeat(ctx context.Context, id string) error {
	return r.Client.Expire(ctx, agentKey(id), heartbeatTTL).Err()
}

// UnregisterAgent removes the agent entry on clean shutdown.
func (r *Redis) UnregisterAgent(ctx context.Context, id string) error {
	return r.Client.Del(ctx, agentKey(id)).Err()
}

// Agents lists the currently alive agents (those whose TTL hasn't expired).
func (r *Redis) Agents(ctx context.Context) ([]AgentInfo, error) {
	var agents []AgentInfo

	iter := r.Client.Scan(ctx, 0, agentsPattern, 100).Iterator()
	for iter.Next(ctx) {
		val, err := r.Client.Get(ctx, iter.Val()).Result()
		if errors.Is(err, redis.Nil) {
			continue // expired between SCAN and GET
		}
		if err != nil {
			return nil, err
		}
		var info AgentInfo
		if err := json.Unmarshal([]byte(val), &info); err != nil {
			continue
		}
		agents = append(agents, info)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return agents, nil
}

// distResult is the wire format for results pushed to Redis. It carries the
// error message and the agent ID as strings so JSON survives the round trip.
type distResult struct {
	JobID      int64         `json:"job_id"`
	AgentID    string        `json:"agent_id"`
	StatusCode int           `json:"status_code"`
	Duration   time.Duration `json:"duration_ns"`
	Error      string        `json:"error,omitempty"`
	Timestamp  time.Time     `json:"timestamp"`
}

func (d distResult) toResult() storm.Result {
	result := storm.Result{
		JobID:      d.JobID,
		StatusCode: d.StatusCode,
		Duration:   d.Duration,
		Timestamp:  d.Timestamp,
	}
	if d.Error != "" {
		result.Error = errors.New(d.Error)
	}
	return result
}

func toDistResult(agentID string, result storm.Result) distResult {
	d := distResult{
		JobID:      result.JobID,
		AgentID:    agentID,
		StatusCode: result.StatusCode,
		Duration:   result.Duration,
		Timestamp:  result.Timestamp,
	}
	if result.Error != nil {
		d.Error = result.Error.Error()
	}
	return d
}

// PushResult stores one agent result in the given run's results list (LPUSH).
func (r *Redis) PushResult(ctx context.Context, runID, agentID string, result storm.Result) error {
	b, err := json.Marshal(toDistResult(agentID, result))
	if err != nil {
		return err
	}
	return r.Client.LPush(ctx, resultKey(runID), string(b)).Err()
}

// ResultsCount returns how many results for a run have been collected so far.
func (r *Redis) ResultsCount(ctx context.Context, runID string) (int64, error) {
	return r.Client.LLen(ctx, resultKey(runID)).Result()
}

// RunAgent consumes jobs from the queue with `workers` goroutines and pushes
// results back tagged with the agent id. It registers + heartbeats so the
// coordinator can see it, and returns when the context is cancelled or no
// jobs arrive for idleTimeout.
func (r *Redis) RunAgent(ctx context.Context, id string, workers int, requestTimeout time.Duration,
	stayAlive bool, onJobStart func(storm.Job), onResult func(storm.Result)) (int, error) {
	if err := r.RegisterAgent(ctx, id); err != nil {
		return 0, err
	}
	defer r.UnregisterAgent(ctx, id)

	// Heartbeat goroutine: renews the agent's TTL every second.
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				_ = r.Heartbeat(hbCtx, id)
			}
		}
	}()

	client := &http.Client{Timeout: requestTimeout}

	var wg sync.WaitGroup
	var processed atomic.Int64
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lastJobAt := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				job, runID, ok, err := r.PopJob(ctx)
				if err != nil {
					errCh <- err
					return
				}

				if !ok {
					// Queue empty: leave after a short idle window, unless
					// stay-alive keeps the agent up for metrics scraping.
					if stayAlive {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if time.Since(lastJobAt) > idleTimeout {
						return
					}
					continue
				}

				lastJobAt = time.Now()
				if onJobStart != nil {
					onJobStart(job)
				}
				result := storm.Execute(ctx, client, job, nil)
				if onResult != nil {
					onResult(result)
				}
				if err := r.PushResult(ctx, runID, id, result); err != nil {
					errCh <- err
					return
				}
				processed.Add(1)
			}
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return int(processed.Load()), err
	default:
		return int(processed.Load()), nil
	}
}

// AgentStats is one agent's share of a distributed run.
type AgentStats struct {
	Agent AgentInfo
	Stats storm.Stats
}

// waitForAgents blocks until at least n agents have registered, up to a
// 30s timeout so run-dist can't hang forever.
func (r *Redis) waitForAgents(ctx context.Context, n int) error {
	deadline := time.Now().Add(agentsWait)

	for {
		agents, err := r.Agents(ctx)
		if err != nil {
			return err
		}
		if len(agents) >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d agents (only %d registered)", n, len(agents))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// RunCoordinator pushes all jobs, waits until every result has landed, and
// aggregates them into Stats plus a per-agent breakdown. Every run uses its
// own results key (keyed by runID) so results from older runs can never leak
// into this report. If waitFor > 0 it first blocks until that many agents
// have registered.
func (r *Redis) RunCoordinator(ctx context.Context, runID string, jobs []storm.Job, waitFor int) (storm.Stats, []AgentStats, error) {
	if waitFor > 0 {
		if err := r.waitForAgents(ctx, waitFor); err != nil {
			return storm.Stats{}, nil, err
		}
	}

	start := time.Now()

	if err := r.PushJobs(ctx, runID, jobs); err != nil {
		return storm.Stats{}, nil, err
	}

	total := int64(len(jobs))

	// Wait for all results for THIS run.
	for {
		select {
		case <-ctx.Done():
			return storm.Stats{}, nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
			n, err := r.ResultsCount(ctx, runID)
			if err != nil {
				return storm.Stats{}, nil, err
			}
			if n >= total {
				goto done
			}
		}
	}

done:
	items, err := r.Client.LRange(ctx, resultKey(runID), 0, -1).Result()
	if err != nil {
		return storm.Stats{}, nil, err
	}

	// Decode results and group them by agent.
	grouped := make(map[string][]storm.Result)
	var all []storm.Result
	for _, item := range items {
		var dr distResult
		if err := json.Unmarshal([]byte(item), &dr); err != nil {
			return storm.Stats{}, nil, err
		}
		result := dr.toResult()
		all = append(all, result)
		if dr.AgentID != "" {
			grouped[dr.AgentID] = append(grouped[dr.AgentID], result)
		}
	}

	stats := storm.Aggregate(all)
	elapsed := time.Since(start)
	stats.TotalDuration = elapsed
	if elapsed > 0 {
		stats.RequestsPerSec = float64(stats.TotalRequests) / elapsed.Seconds()
	}

	// Map agent IDs to their registration info.
	agents, err := r.Agents(ctx)
	if err != nil {
		return stats, nil, err
	}
	infoByID := make(map[string]AgentInfo)
	for _, a := range agents {
		infoByID[a.ID] = a
	}

	breakdown := make([]AgentStats, 0, len(grouped))
	for id, results := range grouped {
		info := infoByID[id]
		if info.ID == "" {
			info = AgentInfo{ID: id}
		}
		breakdown = append(breakdown, AgentStats{Agent: info, Stats: storm.Aggregate(results)})
	}
	sort.Slice(breakdown, func(i, j int) bool {
		return breakdown[i].Agent.ID < breakdown[j].Agent.ID
	})

	return stats, breakdown, nil
}

// Flush clears leftover jobs and results from previous runs. The job queue is
// cleared so old jobs can't leak into a new run; every run's result key is
// removed too (a new coordinator run generates a fresh key anyway, but this
// keeps Redis tidy).
func (r *Redis) Flush(ctx context.Context) error {
	if err := r.Client.Del(ctx, queueKey).Err(); err != nil {
		return err
	}

	var keys []string
	iter := r.Client.Scan(ctx, 0, "storm:results:*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return r.Client.Del(ctx, keys...).Err()
	}
	return nil
}
