package storm

import "time"

// produceJobs fills the jobs channel according to the configured workload
// termination condition: TotalReqs dispatched (count mode) or the wall-clock
// deadline reached (duration mode). It respects cancellation so a shutdown
// doesn't leak the goroutine.
//
// The deadline is captured once, before the loop starts, and never written
// again — no synchronization is needed. time.Now() is called once per
// produced job (never per request in workers), which is noise even at
// 100k+ jobs/sec.
func (lt *LoadTester) produceJobs() {
	defer close(lt.jobs)

	var deadline time.Time
	if lt.config.Duration > 0 {
		deadline = time.Now().Add(lt.config.Duration)
	}

	var i int64
	for {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return
		}
		if deadline.IsZero() && i >= int64(lt.config.TotalReqs) {
			return
		}
		i++

		if lt.limiter != nil {
			if err := lt.limiter.Wait(lt.ctx); err != nil {
				return
			}
		}

		job := Job{
			ID:      i,
			URL:     lt.config.URL,
			Method:  lt.config.Method,
			Body:    lt.config.Payload,
			Headers: lt.config.Headers,
		}

		select {
		case <-lt.ctx.Done():
			return
		case lt.jobs <- job:
		}
	}
}
