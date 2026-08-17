package storm

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

// executeRequest performs a single HTTP request and records its result.
func (lt *LoadTester) executeRequest(job Job) Result {
	return Execute(lt.ctx, lt.client, job)
}

// Execute performs a single HTTP request and records its result.
// Exported so distributed agents can reuse the same request logic.
func Execute(ctx context.Context, client *http.Client, job Job) Result {
	start := time.Now()

	req, err := http.NewRequestWithContext(
		ctx,
		job.Method,
		job.URL,
		bytes.NewBuffer(job.Body),
	)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     fmt.Errorf("invalid request: %w", err),
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	if job.Method == "POST" || job.Method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{
			JobID:     job.ID,
			Method:    job.Method,
			Error:     err,
			Duration:  duration,
			Timestamp: time.Now(),
		}
	}
	defer resp.Body.Close()

	return Result{
		JobID:      job.ID,
		Method:     job.Method,
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Error:      nil,
		Timestamp:  time.Now(),
	}
}
