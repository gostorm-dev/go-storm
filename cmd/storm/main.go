package main

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "storm",
	Short: "A lightweight HTTP load testing tool",
	Long: `storm is a lightweight HTTP load testing tool.

It sends N requests to a target URL with configurable concurrency,
rate limiting, and reports latency percentiles in text or JSON.

Use 'storm run' to start a load test.`,
}

func main() {
	rootCmd.AddCommand(runCmd, reportCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
