package storm

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
)

// ms converts a duration to fractional milliseconds without truncation.
// Duration.Milliseconds() chops sub-millisecond precision, which reports
// fast targets as 0ms — a lie for a measurement tool.
func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// Report combines run metadata with results for machine-readable output.
type Report struct {
	URL             string  `json:"url"`
	Method          string  `json:"method"`
	Concurrency     int     `json:"concurrency"`
	Rate            int     `json:"rate"`
	TotalRequests   int     `json:"total_requests"`
	Successful      int     `json:"successful"`
	Failed          int     `json:"failed"`
	SuccessRate     float64 `json:"success_rate"`
	MinResponseTime float64 `json:"min_response_time_ms"`
	MaxResponseTime float64 `json:"max_response_time_ms"`
	AvgResponseTime float64 `json:"avg_response_time_ms"`
	P50             float64 `json:"p50_ms"`
	P90             float64 `json:"p90_ms"`
	P95             float64 `json:"p95_ms"`
	P99             float64 `json:"p99_ms"`
	P999            float64 `json:"p999_ms"`

	// Time-to-first-byte distribution — omitted entirely when nothing
	// succeeded. Latency above means full response; TTFB isolates the
	// server-processing + header phase.
	TtfbAvg        float64 `json:"ttfb_avg_ms,omitempty"`
	TtfbP50        float64 `json:"ttfb_p50_ms,omitempty"`
	TtfbP90        float64 `json:"ttfb_p90_ms,omitempty"`
	TtfbP95        float64 `json:"ttfb_p95_ms,omitempty"`
	TtfbP99        float64 `json:"ttfb_p99_ms,omitempty"`
	RequestsPerSec float64 `json:"requests_per_sec"`
	TotalDuration  int64   `json:"total_duration_ms"`
	// RequestedDurationMs records the requested window for duration-mode
	// runs (0 in count mode). Storing the request intent keeps runs
	// comparable and reproducible.
	RequestedDurationMs int64 `json:"requested_duration_ms"`
	// Arrival schedule adherence for rate-limited runs (nil otherwise).
	Arrival     *ArrivalAccuracy `json:"arrival_accuracy,omitempty"`
	StatusCodes map[int]int      `json:"status_codes"`
	Errors      []string         `json:"errors,omitempty"`
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
	fmt.Printf("p90 Response: %v\n", stats.P90)
	fmt.Printf("p95 Response: %v\n", stats.P95)
	fmt.Printf("p99 Response: %v\n", stats.P99)
	fmt.Printf("p99.9 Response: %v\n", stats.P999)
	if stats.TtfbP99 > 0 {
		fmt.Printf("TTFB p50/p90/p95/p99: %v / %v / %v / %v\n",
			stats.TtfbP50, stats.TtfbP90, stats.TtfbP95, stats.TtfbP99)
	}
	fmt.Printf("Requests/sec: %.2f\n", stats.RequestsPerSec)
	if config.Duration > 0 {
		fmt.Printf("Requested Duration: %v\n", config.Duration)
	}
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
		URL:                 config.URL,
		Method:              config.Method,
		Concurrency:         config.Concurrency,
		Rate:                config.Rate,
		TotalRequests:       stats.TotalRequests,
		Successful:          stats.Successful,
		Failed:              stats.Failed,
		SuccessRate:         successRate,
		MinResponseTime:     ms(stats.MinResponseTime),
		MaxResponseTime:     ms(stats.MaxResponseTime),
		AvgResponseTime:     ms(stats.AvgResponseTime),
		P50:                 ms(stats.P50),
		P90:                 ms(stats.P90),
		P95:                 ms(stats.P95),
		P99:                 ms(stats.P99),
		P999:                ms(stats.P999),
		TtfbAvg:             ms(stats.TtfbAvg),
		TtfbP50:             ms(stats.TtfbP50),
		TtfbP90:             ms(stats.TtfbP90),
		TtfbP95:             ms(stats.TtfbP95),
		TtfbP99:             ms(stats.TtfbP99),
		RequestsPerSec:      stats.RequestsPerSec,
		TotalDuration:       stats.TotalDuration.Milliseconds(),
		RequestedDurationMs: config.Duration.Milliseconds(),
		Arrival:             stats.Arrival,
		StatusCodes:         stats.StatusCodes,
		Errors:              stats.Errors,
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

	fmt.Println(row("Avg Latency", fmt.Sprintf("%.2f ms", ms(stats.AvgResponseTime))))
	fmt.Println(row("p50 Latency", fmt.Sprintf("%.2f ms", ms(stats.P50))))
	fmt.Println(row("p90 Latency", fmt.Sprintf("%.2f ms", ms(stats.P90))))
	fmt.Println(row("p95 Latency", fmt.Sprintf("%.2f ms", ms(stats.P95))))
	fmt.Println(row("p99 Latency", fmt.Sprintf("%.2f ms", ms(stats.P99))))
	fmt.Println(row("p99.9 Latency", fmt.Sprintf("%.2f ms", ms(stats.P999))))
	if stats.TtfbP99 > 0 {
		fmt.Println(row("TTFB p50", fmt.Sprintf("%.2f ms", ms(stats.TtfbP50))))
		fmt.Println(row("TTFB p90", fmt.Sprintf("%.2f ms", ms(stats.TtfbP90))))
		fmt.Println(row("TTFB p95", fmt.Sprintf("%.2f ms", ms(stats.TtfbP95))))
		fmt.Println(row("TTFB p99", fmt.Sprintf("%.2f ms", ms(stats.TtfbP99))))
	}
	fmt.Println(row("Min", fmt.Sprintf("%.2f ms", ms(stats.MinResponseTime))))
	fmt.Println(row("Max", fmt.Sprintf("%.2f ms", ms(stats.MaxResponseTime))))
	fmt.Printf("  %s\n", strings.Repeat("─", L+2)+"┼"+strings.Repeat("─", R+2))

	fmt.Println(row("RPS", fmt.Sprintf("%.2f", stats.RequestsPerSec)))
	if config.Duration > 0 {
		fmt.Println(row("Requested", config.Duration.String()))
	}
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
	fmt.Printf("avg_latency_ms,%.2f\n", ms(stats.AvgResponseTime))
	fmt.Printf("p50_ms,%.2f\n", ms(stats.P50))
	fmt.Printf("p90_ms,%.2f\n", ms(stats.P90))
	fmt.Printf("p95_ms,%.2f\n", ms(stats.P95))
	fmt.Printf("p99_ms,%.2f\n", ms(stats.P99))
	fmt.Printf("p999_ms,%.2f\n", ms(stats.P999))
	if stats.TtfbP99 > 0 {
		fmt.Printf("ttfb_avg_ms,%.2f\n", ms(stats.TtfbAvg))
		fmt.Printf("ttfb_p50_ms,%.2f\n", ms(stats.TtfbP50))
		fmt.Printf("ttfb_p90_ms,%.2f\n", ms(stats.TtfbP90))
		fmt.Printf("ttfb_p95_ms,%.2f\n", ms(stats.TtfbP95))
		fmt.Printf("ttfb_p99_ms,%.2f\n", ms(stats.TtfbP99))
	}
	fmt.Printf("min_ms,%.2f\n", ms(stats.MinResponseTime))
	fmt.Printf("max_ms,%.2f\n", ms(stats.MaxResponseTime))
	fmt.Printf("rps,%.2f\n", stats.RequestsPerSec)
	if config.Duration > 0 {
		fmt.Printf("requested_duration_s,%.2f\n", config.Duration.Seconds())
	}
	fmt.Printf("duration_s,%.2f\n", stats.TotalDuration.Seconds())
	for code, count := range stats.StatusCodes {
		fmt.Printf("status_%d,%d\n", code, count)
	}
}

// PrintStatsQuiet renders only numbers, comma-separated (for CI/CD).
// Column layout: total,successful,failed,success_rate,avg,p50,p90,p95,p99,
// p999,rps[,requested_seconds]. Columns 1–9 are unchanged from earlier
// versions; p90/p999 were inserted after p99 as additive columns.
func PrintStatsQuiet(config Config, stats Stats) {
	successRate := 0.0
	if stats.TotalRequests > 0 {
		successRate = float64(stats.Successful) / float64(stats.TotalRequests) * 100
	}
	line := fmt.Sprintf("%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
		stats.TotalRequests,
		stats.Successful,
		stats.Failed,
		successRate,
		ms(stats.AvgResponseTime),
		ms(stats.P50),
		ms(stats.P90),
		ms(stats.P95),
		ms(stats.P99),
		ms(stats.P999),
		stats.RequestsPerSec,
	)
	if config.Duration > 0 {
		line += fmt.Sprintf(",%.2f", config.Duration.Seconds())
	}
	fmt.Println(line)
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}
