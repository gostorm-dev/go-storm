package storm

import "time"

// produceJobs fills the jobs channel according to the configured workload.
//
// With --rate set, dispatch follows a virtual-clock arrival schedule
// (see arrival.go): job j is due at start + j·(1s/rate), and in duration
// mode exactly ceil(rate×duration) jobs are dispatched. Without --rate,
// behavior is unchanged — count mode streams until TotalReqs, duration
// mode until the wall-clock deadline.
func (lt *LoadTester) produceJobs() {
	defer close(lt.jobs)

	if lt.config.Rate > 0 {
		lt.produceScheduled()
		return
	}

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

		select {
		case <-lt.ctx.Done():
			return
		case lt.jobs <- lt.job(i):
		}
	}
}

// job builds a request for one arrival slot. The returned Job shares the
// config's URL/body/header storage — workers must treat them as read-only.
func (lt *LoadTester) job(id int64) Job {
	return Job{
		ID:      id,
		URL:     lt.config.URL,
		Method:  lt.config.Method,
		Body:    lt.config.Payload,
		Headers: lt.config.Headers,
	}
}
