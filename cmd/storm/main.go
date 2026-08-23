package main

import (
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/gostorm-dev/go-storm/internal/buildinfo"
)

var redisAddr string

var rootCmd = &cobra.Command{
	Use:     "storm",
	Version: buildinfo.Version,
	Short:   "A lightweight HTTP load testing tool",
	Long: `go-storm — The Load Tester That Tells Truth

A high-performance HTTP load testing engine written in Go.
Unlike other tools, go-storm detects when YOUR generator is the bottleneck.

Use 'storm run' for a local load test, or 'storm run-dist' with one
or more 'storm agent' processes for distributed load.

Documentation: https://gostorm-dev.github.io/docs/`,
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
		printBanner()
		cmd.Help()
	},
}

func main() {
	rootCmd.AddCommand(runCmd, runDistCmd, agentCmd, reportCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		color.New(color.FgRed).Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
