package storm

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
			result := lt.executeRequest(job)
			lt.completed.Add(1)
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
