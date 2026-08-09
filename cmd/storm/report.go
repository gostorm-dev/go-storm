package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/hariomop12/go-storm/pkg/storm"
)

var reportCmd = &cobra.Command{
	Use:   "report <file>",
	Short: "Display a saved JSON report as text",
	Args:  cobra.ExactArgs(1),
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
		fmt.Printf("Min/Avg/Max: %v / %v / %v\n", report.MinResponseTime, report.AvgResponseTime, report.MaxResponseTime)
		fmt.Printf("p50/p95/p99: %v / %v / %v\n", report.P50, report.P95, report.P99)
		fmt.Printf("Requests/sec: %.2f\n", report.RequestsPerSec)
		return nil
	},
}
