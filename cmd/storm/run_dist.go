package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/hariomop12/go-storm/internal/dist"
	"github.com/hariomop12/go-storm/pkg/storm"
)

var waitAgents int

var runDistCmd = &cobra.Command{
	Use:   "run-dist",
	Short: "Run a distributed load test using Redis agents",
	Long: `run-dist pushes N jobs into the Redis queue and waits for the agents
to process them all. Any number of agents can be running; each pulls jobs
from the shared queue and pushes results back.

Start agents with 'storm agent' on the machines you want to load from,
then run this command once.`,
	Example: `  # Basic distributed test
  storm run-dist -u https://example.com -n 10000

  # Wait for 3 agents before starting
  storm run-dist -u https://example.com -n 10000 --agents 3

  # Save report and show agent breakdown
  storm run-dist -u https://example.com -n 10000 --format json --output report.json`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if total <= 0 {
			return fmt.Errorf("total requests must be positive, got %d", total)
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		rdb := dist.NewRedis(redisAddr)
		if err := rdb.Ping(ctx); err != nil {
			return fmt.Errorf("cannot connect to redis at %s: %w", redisAddr, err)
		}
		if err := rdb.Flush(ctx); err != nil {
			return err
		}

		cfg := storm.Config{
			URL:     url,
			Method:  method,
			Timeout: time.Duration(timeout) * time.Second,
			Payload: []byte(body),
		}

		cfg.Headers = http.Header{}
		for _, h := range headers {
			k, v, err := storm.ParseHeaderSpec(h)
			if err != nil {
				return err
			}
			cfg.Headers.Add(k, v)
		}

		// Brand banner for human-readable output only — json stays clean.
		if format == "" || format == "text" {
			printBanner()
		}

		color.Cyan("Distributed Load Test")
		fmt.Printf("Target: %s\n", cfg.URL)
		fmt.Printf("Total: %d requests\n", total)
		fmt.Printf("Queue: %s\n", redisAddr)
		if waitAgents > 0 {
			color.Yellow("Waiting for %d agents...", waitAgents)
		}

		jobs := make([]storm.Job, total)
		for i := range jobs {
			jobs[i] = storm.Job{
				ID:      i + 1,
				URL:     cfg.URL,
				Method:  cfg.Method,
				Body:    cfg.Payload,
				Headers: cfg.Headers,
			}
		}

		// Live result counter (text output only).
		runID := dist.NewRunID()
		var done chan struct{}
		if format == "text" {
			done = make(chan struct{})
			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						n, err := rdb.ResultsCount(ctx, runID)
						if err == nil {
							fmt.Printf("\rResults: %d/%d", n, total)
						}
					}
				}
			}()
		}

		stats, breakdown, err := rdb.RunCoordinator(ctx, runID, jobs, waitAgents)
		if done != nil {
			close(done)
			fmt.Println()
		}
		if err != nil {
			return err
		}

		var jsonOut []byte
		if format == "json" || output != "" {
			data, err := storm.ReportJSON(cfg, stats)
			if err != nil {
				return err
			}
			jsonOut = data
		}

		if output != "" {
			if err := os.WriteFile(output, jsonOut, 0644); err != nil {
				return fmt.Errorf("failed to write report to %s: %w", output, err)
			}
			color.Green("Report saved to %s", output)
		}

		if format == "json" {
			fmt.Println(string(jsonOut))
			if violation := evaluateThresholds(stats); violation != "" {
				fmt.Fprintf(os.Stderr, "\nFAIL: %s\n", violation)
				os.Exit(2)
			}
			return nil
		}

		printAgentBreakdown(breakdown)
		storm.PrintStatsReport(cfg, stats)

		if violation := evaluateThresholds(stats); violation != "" {
			fmt.Fprintf(os.Stderr, "\nFAIL: %s\n", violation)
			os.Exit(2)
		}
		return nil
	},
}

// printAgentBreakdown renders each agent's share of the run.
func printAgentBreakdown(breakdown []dist.AgentStats) {
	if len(breakdown) == 0 {
		return
	}

	fmt.Println("\n" + strings.Repeat("-", 60))
	fmt.Println("AGENT BREAKDOWN")
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("%-16s %8s %10s %10s %10s\n", "Agent", "Requests", "Avg", "p95", "Success")
	for _, a := range breakdown {
		rate := 0.0
		if a.Stats.TotalRequests > 0 {
			rate = float64(a.Stats.Successful) / float64(a.Stats.TotalRequests) * 100
		}
		fmt.Printf("%-16s %8d %10v %10v %9.1f%%\n",
			a.Agent.ID,
			a.Stats.TotalRequests,
			a.Stats.AvgResponseTime,
			a.Stats.P95,
			rate,
		)
	}
	fmt.Println(strings.Repeat("-", 60))
}

func init() {
	runDistCmd.Flags().StringVarP(&url, "url", "u", "", "Target URL (required)")
	runDistCmd.Flags().IntVarP(&total, "requests", "n", 100, "Total requests to send")
	runDistCmd.Flags().StringVarP(&method, "method", "m", "GET", "HTTP method: GET, POST, PUT, DELETE")
	runDistCmd.Flags().IntVarP(&timeout, "timeout", "t", 10, "Request timeout in seconds")
	runDistCmd.Flags().StringVarP(&body, "body", "b", "", "Request body (for POST/PUT)")
	runDistCmd.Flags().StringArrayVarP(&headers, "header", "H", nil, "Custom header (repeatable): -H \"Key: Value\"")
	runDistCmd.Flags().IntVar(&failAboveErrors, "fail-above-errors", -1, "Exit with code 2 if failed requests exceed N (-1 = disabled)")
	runDistCmd.Flags().Float64Var(&failAboveP95, "fail-above-p95", -1, "Exit with code 2 if p95 latency exceeds MS milliseconds (-1 = disabled)")
	runDistCmd.Flags().IntVar(&waitAgents, "agents", 0, "Wait for this many agents before starting (0 = don't wait)")
	runDistCmd.Flags().StringVar(&format, "format", "text", "Output format: text or json")
	runDistCmd.Flags().StringVar(&output, "output", "", "Write JSON report to a file")
}
