package main

import (
	"os"

	"github.com/spf13/cobra"
)

var redisAddr string

var rootCmd = &cobra.Command{
	Use:   "storm",
	Short: "A lightweight HTTP load testing tool",
	Long: `storm is a lightweight HTTP load testing tool.

It sends N requests to a target URL with configurable concurrency,
rate limiting, and reports latency percentiles in text or JSON.

Use 'storm run' for a local load test, or 'storm run-dist' with one
or more 'storm agent' processes for distributed load.`,
}

func main() {
	rootCmd.PersistentFlags().StringVar(&redisAddr, "redis", "localhost:6379", "Redis address for distributed mode")

	rootCmd.AddCommand(runCmd, runDistCmd, agentCmd, reportCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
