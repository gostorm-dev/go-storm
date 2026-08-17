package storm

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Aggregate combines a set of results into Stats.
// Exported so distributed coordinators can reuse the same aggregation logic.
func Aggregate(results []Result) Stats {
	var (
		totalDuration time.Duration
		minDuration   time.Duration
		maxDuration   time.Duration
		successCount  int
		failCount     int
		statusCodes   = make(map[int]int)
		errors        []string
		durations     []time.Duration
	)

	firstResult := true

	for _, result := range results {
		if result.Error != nil {
			failCount++

			errors = append(
				errors,
				fmt.Sprintf("Job %d: %v", result.JobID, result.Error),
			)
			continue
		}

		statusCodes[result.StatusCode]++
		if result.StatusCode >= 200 && result.StatusCode < 400 {
			successCount++
		} else {
			failCount++
		}

		totalDuration += result.Duration
		durations = append(durations, result.Duration)

		if firstResult {
			minDuration = result.Duration
			maxDuration = result.Duration
			firstResult = false
		}

		if result.Duration < minDuration {
			minDuration = result.Duration
		}

		if result.Duration > maxDuration {
			maxDuration = result.Duration
		}
	}

	var avgDuration time.Duration

	if successCount+failCount > 0 {
		avgDuration = totalDuration / time.Duration(successCount+failCount)
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	return Stats{
		TotalRequests:   len(results),
		Successful:      successCount,
		Failed:          failCount,
		MinResponseTime: minDuration,
		MaxResponseTime: maxDuration,
		AvgResponseTime: avgDuration,
		P50:             percentile(durations, 50),
		P95:             percentile(durations, 95),
		P99:             percentile(durations, 99),
		StatusCodes:     statusCodes,
		Errors:          errors,
	}
}

// percentile returns the duration at the given percentile (0-100)
// of a sorted slice, using the nearest-rank method.
func percentile(durations []time.Duration, pct float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(durations)) * pct / 100))
	if idx < 1 {
		idx = 1
	}
	return durations[idx-1]
}
