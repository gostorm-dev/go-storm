package dist

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

// RunAgent consumes jobs from the queue with `workers` goroutines and pushes
// results back tagged with the agent id. It registers + heartbeats so the
// coordinator can see it, and returns when the context is cancelled or no
// jobs arrive for IdleTimeout.
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
				// Best-effort: a missed heartbeat only shortens the
				// registry entry's life; results are unaffected.
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
					// Queue empty: leave after an idle window, unless
					// stay-alive keeps the agent up for metrics scraping.
					if stayAlive {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if time.Since(lastJobAt) > r.IdleTimeout {
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
