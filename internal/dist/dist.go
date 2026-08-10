// Package dist implements distributed load testing over Redis.
// Agents pull jobs from a shared queue, execute HTTP requests, and push
// results back. A coordinator pushes all jobs and aggregates the results.
package dist

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hariomop12/go-storm/pkg/storm"
)

const (
	queueKey    = "storm:jobs"
	resultKey   = "storm:results"
	popTimeout  = 2 * time.Second // BLPop wait before reporting empty
	idleTimeout = 5 * time.Second // agent exits after this long with no jobs
)

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

// PushJobs publishes all jobs to the queue in chunks (LPUSH).
func (r *Redis) PushJobs(ctx context.Context, jobs []storm.Job) error {
	const chunkSize = 100

	for i := 0; i < len(jobs); i += chunkSize {
		end := i + chunkSize
		if end > len(jobs) {
			end = len(jobs)
		}

		payloads := make([]interface{}, 0, end-i)
		for _, job := range jobs[i:end] {
			b, err := json.Marshal(job)
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
func (r *Redis) PopJob(ctx context.Context) (storm.Job, bool, error) {
	item, err := r.Client.BLPop(ctx, popTimeout, queueKey).Result()
	if errors.Is(err, redis.Nil) {
		return storm.Job{}, false, nil
	}
	if err != nil {
		return storm.Job{}, false, err
	}

	var job storm.Job
	if err := json.Unmarshal([]byte(item[1]), &job); err != nil {
		return storm.Job{}, false, err
	}
	return job, true, nil
}

// distResult is the wire format for results pushed to Redis.
// It carries the error message as a string so JSON survives the round trip.
type distResult struct {
	JobID      int           `json:"job_id"`
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

func toDistResult(result storm.Result) distResult {
	d := distResult{
		JobID:      result.JobID,
		StatusCode: result.StatusCode,
		Duration:   result.Duration,
		Timestamp:  result.Timestamp,
	}
	if result.Error != nil {
		d.Error = result.Error.Error()
	}
	return d
}

// PushResult stores one agent result in the shared results list (LPUSH).
func (r *Redis) PushResult(ctx context.Context, result storm.Result) error {
	b, err := json.Marshal(toDistResult(result))
	if err != nil {
		return err
	}
	return r.Client.LPush(ctx, resultKey, string(b)).Err()
}

// ResultsCount returns how many results have been collected so far.
func (r *Redis) ResultsCount(ctx context.Context) (int64, error) {
	return r.Client.LLen(ctx, resultKey).Result()
}

// RunAgent consumes jobs from the queue with `workers` goroutines and pushes
// results back. It returns when the context is cancelled or no jobs arrive
// for idleTimeout.
func (r *Redis) RunAgent(ctx context.Context, workers int, requestTimeout time.Duration) (int, error) {
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

				job, ok, err := r.PopJob(ctx)
				if err != nil {
					errCh <- err
					return
				}

				if !ok {
					// Queue empty: leave after a short idle window.
					if time.Since(lastJobAt) > idleTimeout {
						return
					}
					continue
				}

				lastJobAt = time.Now()
				result := storm.Execute(ctx, client, job)
				if err := r.PushResult(ctx, result); err != nil {
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

// RunCoordinator pushes all jobs, waits until every result has landed, and
// aggregates them into Stats (using the same logic as local runs).
func (r *Redis) RunCoordinator(ctx context.Context, jobs []storm.Job) (storm.Stats, error) {
	start := time.Now()

	if err := r.PushJobs(ctx, jobs); err != nil {
		return storm.Stats{}, err
	}

	total := int64(len(jobs))

	// Wait for all results.
	for {
		select {
		case <-ctx.Done():
			return storm.Stats{}, ctx.Err()
		case <-time.After(300 * time.Millisecond):
			n, err := r.ResultsCount(ctx)
			if err != nil {
				return storm.Stats{}, err
			}
			if n >= total {
				goto done
			}
		}
	}

done:
	items, err := r.Client.LRange(ctx, resultKey, 0, -1).Result()
	if err != nil {
		return storm.Stats{}, err
	}

	results := make([]storm.Result, 0, len(items))
	for _, item := range items {
		var dr distResult
		if err := json.Unmarshal([]byte(item), &dr); err != nil {
			return storm.Stats{}, err
		}
		results = append(results, dr.toResult())
	}

	stats := storm.Aggregate(results)
	elapsed := time.Since(start)
	stats.TotalDuration = elapsed
	if elapsed > 0 {
		stats.RequestsPerSec = float64(stats.TotalRequests) / elapsed.Seconds()
	}
	return stats, nil
}

// Flush clears the job and result queues. Call between test runs.
func (r *Redis) Flush(ctx context.Context) error {
	return r.Client.Del(ctx, queueKey, resultKey).Err()
}
