package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/gostorm-dev/go-storm/pkg/storm"
)

var reportCmd = &cobra.Command{
	Use:   "report <file>",
	Short: "Display a saved JSON report as text",
	Args:  cobra.ExactArgs(1),
	Example: `  # View a saved report
  storm report result.json

  # View report from distributed test
  storm report dist-report.json`,
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}

		var report storm.Report
		if err := json.Unmarshal(data, &report); err != nil {
			return fmt.Errorf("invalid report: %w", err)
		}

		fmt.Printf("URL: %s\n", report.URL)
		fmt.Printf("Method: %s\n", report.Method)
		fmt.Printf("Concurrency: %d\n", report.Concurrency)
		fmt.Printf("Rate: %d req/sec\n", report.Rate)
		fmt.Printf("Total Requests: %d\n", report.TotalRequests)
		fmt.Printf("Successful: %d\n", report.Successful)
		fmt.Printf("Failed: %d\n", report.Failed)
		fmt.Printf("Success Rate: %.2f%%\n", report.SuccessRate)
		fmt.Printf("Min/Avg/Max: %.2f / %.2f / %.2f ms\n", report.MinResponseTime, report.AvgResponseTime, report.MaxResponseTime)
		fmt.Printf("p50/p90/p95: %.2f / %.2f / %.2f ms\n", report.P50, report.P90, report.P95)
		fmt.Printf("p99/p99.9:   %.2f / %.2f ms\n", report.P99, report.P999)
		if report.Arrival != nil {
			fmt.Printf("Arrival:     %.2f%% on-time (%d/%d slots, lag p99 %.2f ms)\n",
				report.Arrival.AccuracyPct, report.Arrival.Late, report.Arrival.Sent, report.Arrival.LagP99MS)
		}
		fmt.Printf("Requests/sec: %.2f\n", report.RequestsPerSec)
		return nil
	},
}
