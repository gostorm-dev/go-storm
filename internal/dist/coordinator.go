package dist

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

// AgentStats is one agent's share of a distributed run.
type AgentStats struct {
	Agent AgentInfo
	Stats storm.Stats
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
