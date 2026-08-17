package storm

// produceJobs fills the jobs channel up to TotalReqs.
// It respects cancellation so a shutdown doesn't leak the goroutine.
func (lt *LoadTester) produceJobs() {
	defer close(lt.jobs)

	for i := 0; i < lt.config.TotalReqs; i++ {
		if lt.limiter != nil {
			if err := lt.limiter.Wait(lt.ctx); err != nil {
				return
			}
		}

		job := Job{
			ID:     i + 1,
			URL:    lt.config.URL,
			Method: lt.config.Method,
			Body:   lt.config.Payload,
		}

		select {
		case <-lt.ctx.Done():
			return
		case lt.jobs <- job:
		}
	}
}
