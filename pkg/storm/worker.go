package storm

const defaultAbortThreshold = 100

// worker is a single concurrent consumer of the jobs channel.
func (lt *LoadTester) worker(id int) {
	defer lt.wg.Done()

	for {
		select {
		case <-lt.ctx.Done():
			return

		case job, ok := <-lt.jobs:
			if !ok {
				return
			}
			if lt.onJobStart != nil {
				lt.onJobStart(job)
			}
			result := lt.executeRequest(job)
			if lt.onResult != nil {
				lt.onResult(result)
			}
			lt.completed.Add(1)
			lt.busyNanos.Add(int64(result.Duration))

			// Track consecutive failures across all workers.
			if result.Error != nil {
				newCount := lt.consecutiveFailures.Add(1)
				if newCount >= int64(defaultAbortThreshold) {
					lt.cancel()
					return
				}
			} else {
				lt.consecutiveFailures.Store(0)
			}

			select {
			case lt.results <- result:

			case <-lt.ctx.Done():
				return
			}
		}
	}
}

// Completed returns how many requests have been finished so far.
func (lt *LoadTester) Completed() int64 {
	return lt.completed.Load()
}
