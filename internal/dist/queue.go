package dist

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/redis/go-redis/v9"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

// Queue operations: the coordinator produces jobs and consumes results,
// agents do the reverse. Everything here is LPUSH/BLPOP/LLEN based so a
// plain Redis instance is enough — no streams, no scripts.

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

// PopJob blocks up to PopTimeout for a job. Returns ok=false when empty.
func (r *Redis) PopJob(ctx context.Context) (storm.Job, string, bool, error) {
	item, err := r.Client.BLPop(ctx, r.PopTimeout, queueKey).Result()
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

// Flush clears leftover jobs and results from previous runs. The job queue is
// cleared so old jobs can't leak into a new run; every run's result key is
// removed too (a new coordinator run generates a fresh key anyway, but this
// keeps Redis tidy).
func (r *Redis) Flush(ctx context.Context) error {
	if err := r.Client.Del(ctx, queueKey).Err(); err != nil {
		return err
	}

	var keys []string
	iter := r.Client.Scan(ctx, 0, resultsPrefix+"*", 100).Iterator()
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
