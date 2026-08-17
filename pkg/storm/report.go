package storm

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Report combines run metadata with results for machine-readable output.
type Report struct {
	URL             string      `json:"url"`
	Method          string      `json:"method"`
	Concurrency     int         `json:"concurrency"`
	Rate            int         `json:"rate"`
	TotalRequests   int         `json:"total_requests"`
	Successful      int         `json:"successful"`
	Failed          int         `json:"failed"`
	SuccessRate     float64     `json:"success_rate"`
	MinResponseTime int64       `json:"min_response_time_ms"`
	MaxResponseTime int64       `json:"max_response_time_ms"`
	AvgResponseTime int64       `json:"avg_response_time_ms"`
	P50             int64       `json:"p50_ms"`
	P95             int64       `json:"p95_ms"`
	P99             int64       `json:"p99_ms"`
	RequestsPerSec  float64     `json:"requests_per_sec"`
	TotalDuration   int64       `json:"total_duration_ms"`
	StatusCodes     map[int]int `json:"status_codes"`
	Errors          []string    `json:"errors,omitempty"`
}

// PrintStatsReport renders a run's stats to stdout.
// Exported so distributed coordinators can reuse the same output.
func PrintStatsReport(config Config, stats Stats) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("LOAD TEST RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("URL: %s\n", config.URL)
	fmt.Printf("Method: %s\n", config.Method)
	if config.Concurrency > 0 {
		fmt.Printf("Concurrency: %d\n", config.Concurrency)
	}
	fmt.Printf("Total Requests: %d\n", stats.TotalRequests)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Successful: %d\n", stats.Successful)
	fmt.Printf("Failed: %d\n", stats.Failed)
	fmt.Printf("Success Rate: %.2f%%\n",
		float64(stats.Successful)/float64(stats.TotalRequests)*100)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Min Response: %v\n", stats.MinResponseTime)
	fmt.Printf("Max Response: %v\n", stats.MaxResponseTime)
	fmt.Printf("Avg Response: %v\n", stats.AvgResponseTime)
	fmt.Printf("p50 Response: %v\n", stats.P50)
	fmt.Printf("p95 Response: %v\n", stats.P95)
	fmt.Printf("p99 Response: %v\n", stats.P99)
	fmt.Printf("Requests/sec: %.2f\n", stats.RequestsPerSec)
	fmt.Printf("Total Duration: %v\n", stats.TotalDuration)
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println("Status Code Distribution:")
	for code, count := range stats.StatusCodes {
		fmt.Printf("   %d: %d requests\n", code, count)
	}
	if len(stats.Errors) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("Errors:")
		for _, err := range stats.Errors {
			fmt.Printf("   %s\n", err)
		}
	}
	fmt.Println(strings.Repeat("=", 60))
}

// ReportJSON serializes a run's stats as indented JSON.
// Exported so distributed coordinators can reuse the same output.
func ReportJSON(config Config, stats Stats) ([]byte, error) {
	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	report := Report{
		URL:             config.URL,
		Method:          config.Method,
		Concurrency:     config.Concurrency,
		Rate:            config.Rate,
		TotalRequests:   stats.TotalRequests,
		Successful:      stats.Successful,
		Failed:          stats.Failed,
		SuccessRate:     successRate,
		MinResponseTime: stats.MinResponseTime.Milliseconds(),
		MaxResponseTime: stats.MaxResponseTime.Milliseconds(),
		AvgResponseTime: stats.AvgResponseTime.Milliseconds(),
		P50:             stats.P50.Milliseconds(),
		P95:             stats.P95.Milliseconds(),
		P99:             stats.P99.Milliseconds(),
		RequestsPerSec:  stats.RequestsPerSec,
		TotalDuration:   stats.TotalDuration.Milliseconds(),
		StatusCodes:     stats.StatusCodes,
		Errors:          stats.Errors,
	}
	return json.MarshalIndent(report, "", "  ")
}
