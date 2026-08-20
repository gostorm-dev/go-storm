package storm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fatih/color"
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
		if len(stats.Errors) >= maxStoredErrors {
			fmt.Printf("   ... and %d more errors (showing first %d)\n",
				stats.Failed-len(stats.Errors), maxStoredErrors)
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

// padRight pads a string to the given width with spaces.
func padRight(s string, width int) string {
	if len(s) >= width {
		return s[:width]
	}
	return s + strings.Repeat(" ", width-len(s))
}

// padLeft left-aligns a string to the given width with spaces.
func padLeft(s string, width int) string {
	if len(s) >= width {
		return s[:width]
	}
	return strings.Repeat(" ", width-len(s)) + s
}

// PrintStatsTable renders results in a formatted table.
func PrintStatsTable(config Config, stats Stats) {
	green := color.New(color.FgGreen).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()
	bold := color.New(color.FgWhite, color.Bold).SprintFunc()

	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}

	L := 17 // left column inner width
	R := 18 // right column inner width

	row := func(label, value string) string {
		return fmt.Sprintf("  │ %s │ %s │", padRight(label, L), padLeft(value, R))
	}

	fmt.Println()
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))
	fmt.Printf("  │ %s │ %s │\n", bold(padRight("Metric", L)), bold(padLeft("Value", R)))
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))

	fmt.Println(row("URL", truncate(config.URL, R)))
	fmt.Println(row("Method", config.Method))
	fmt.Println(row("Workers", fmt.Sprintf("%d", config.Concurrency)))
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))

	fmt.Println(row("Total", fmt.Sprintf("%d", stats.TotalRequests)))
	fmt.Println(row("Successful", fmt.Sprintf("%d", stats.Successful)))
	fmt.Println(row("Failed", fmt.Sprintf("%d", stats.Failed)))

	if stats.Failed > 0 {
		fmt.Printf("  │ %s │ %s │\n", padRight("Success Rate", L), yellow(padLeft(fmt.Sprintf("%.2f%%", successRate), R)))
	} else {
		fmt.Printf("  │ %s │ %s │\n", padRight("Success Rate", L), green(padLeft(fmt.Sprintf("%.2f%%", successRate), R)))
	}
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))

	fmt.Println(row("Avg Latency", fmt.Sprintf("%.2f ms", float64(stats.AvgResponseTime.Milliseconds()))))
	fmt.Println(row("p50 Latency", fmt.Sprintf("%d ms", stats.P50.Milliseconds())))
	fmt.Println(row("p95 Latency", fmt.Sprintf("%d ms", stats.P95.Milliseconds())))
	fmt.Println(row("p99 Latency", fmt.Sprintf("%d ms", stats.P99.Milliseconds())))
	fmt.Println(row("Min", fmt.Sprintf("%d ms", stats.MinResponseTime.Milliseconds())))
	fmt.Println(row("Max", fmt.Sprintf("%d ms", stats.MaxResponseTime.Milliseconds())))
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))

	fmt.Println(row("RPS", fmt.Sprintf("%.2f", stats.RequestsPerSec)))
	fmt.Println(row("Duration", fmt.Sprintf("%.2f s", stats.TotalDuration.Seconds())))
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┴"+strings.Repeat("─", R+2))

	if len(stats.StatusCodes) > 0 {
		fmt.Println()
		fmt.Println(bold("  Status Codes:"))
		for code, count := range stats.StatusCodes {
			codeColor := green
			if code >= 400 {
				codeColor = yellow
			}
			if code >= 500 {
				codeColor = red
			}
			fmt.Printf("    %s %d\n", codeColor(fmt.Sprintf("%d:", code)), count)
		}
	}
	fmt.Println()
}

// PrintStatsCSV renders results in CSV format.
func PrintStatsCSV(config Config, stats Stats) {
	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	fmt.Println("metric,value")
	fmt.Printf("url,%s\n", config.URL)
	fmt.Printf("method,%s\n", config.Method)
	fmt.Printf("total,%d\n", stats.TotalRequests)
	fmt.Printf("successful,%d\n", stats.Successful)
	fmt.Printf("failed,%d\n", stats.Failed)
	fmt.Printf("success_rate,%.2f\n", successRate)
	fmt.Printf("avg_latency_ms,%.2f\n", float64(stats.AvgResponseTime.Milliseconds()))
	fmt.Printf("p50_ms,%d\n", stats.P50.Milliseconds())
	fmt.Printf("p95_ms,%d\n", stats.P95.Milliseconds())
	fmt.Printf("p99_ms,%d\n", stats.P99.Milliseconds())
	fmt.Printf("min_ms,%d\n", stats.MinResponseTime.Milliseconds())
	fmt.Printf("max_ms,%d\n", stats.MaxResponseTime.Milliseconds())
	fmt.Printf("rps,%.2f\n", stats.RequestsPerSec)
	fmt.Printf("duration_s,%.2f\n", stats.TotalDuration.Seconds())
	for code, count := range stats.StatusCodes {
		fmt.Printf("status_%d,%d\n", code, count)
	}
}

// PrintStatsQuiet renders only numbers, comma-separated (for CI/CD).
func PrintStatsQuiet(config Config, stats Stats) {
	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	fmt.Printf("%d,%d,%d,%.2f,%d,%d,%d,%d,%.2f\n",
		stats.TotalRequests,
		stats.Successful,
		stats.Failed,
		successRate,
		stats.AvgResponseTime.Milliseconds(),
		stats.P50.Milliseconds(),
		stats.P95.Milliseconds(),
		stats.P99.Milliseconds(),
		stats.RequestsPerSec,
	)
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}
