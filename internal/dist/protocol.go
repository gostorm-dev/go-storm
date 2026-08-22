package dist

import (
	"errors"
	"fmt"
	"time"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

// This file defines the Redis namespace (key layout) and the JSON payload
// schemas that cross it. Both sides — coordinator and agents — must agree
// on these; they are the distributed wire format.

const (
	queueKey      = "storm:jobs"
	agentsPattern = "storm:agent:*"
	resultsPrefix = "storm:results:"
)

func agentKey(id string) string {
	return "storm:agent:" + id
}

func resultKey(runID string) string {
	return resultsPrefix + runID
}

// NewRunID returns a unique id for one coordinator run.
func NewRunID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// distJob is the wire format for jobs in the queue. It carries the run ID so
// every result can be tagged with the run it belongs to — stale results from
// earlier runs can never pollute a new report.
type distJob struct {
	RunID string    `json:"run_id"`
	Job   storm.Job `json:"job"`
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
